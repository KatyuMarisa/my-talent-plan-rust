use std::sync::{Condvar, Arc};
use std::sync::atomic::{AtomicUsize, AtomicU8};
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};

use lockfree::map::{Map as LockFreeMap, ReadGuard};
use lockfree::set::{Set as LockFreeSet};

use super::access_control::{AccessController, CtrlGuard};
use super::manifest::Rid;

pub struct Memtable {
    map: LockFreeMap<String, MemtableValue>,
    ctrl: AccessController,
    state: AtomicU8,
    uncompacted_keys: LockFreeSet<String>,
    uncompacted: AtomicUsize,
    flush_limit: usize,
    bg_cond: Arc<Condvar>,
}

#[derive(PartialEq)]
pub enum MemtableAccessState {
    Ok,
    Retry,
    FlushReject,
    CompactionReject,
    #[allow(dead_code)]
    CloseReject,
    ErrRemoveNonExist,
}

#[derive(Clone, Debug)]
pub enum MemtableValue {
    Rid(Rid),
    Value(Option<String>),
}

impl Memtable {
    const _MEMTABLE_OK: u8 = 0;
    const _MEMTABLE_FLUSHING: u8 = 1;
    const _MEMTABLE_COMPACTION: u8 = 2;

    pub fn new(flush_limit: usize, bd_cond: Arc<Condvar>) -> Self {
        Self {
            map: LockFreeMap::new(),
            ctrl: AccessController::default(),
            state: AtomicU8::new(Self::_MEMTABLE_OK),
            uncompacted_keys: LockFreeSet::new(),
            uncompacted: AtomicUsize::new(0),
            flush_limit,
            bg_cond: Arc::clone(&bd_cond),
        }
    }

    /// Get the value from Memtable. and always return MemtableState::Ok.
    pub fn try_get(&self, key: &String) -> (MemtableAccessState, Option<MemtableValue>) {
        let state = self.current_state();
        if MemtableAccessState::Ok == state {
            if let Some(_read_guard) = self.ctrl.guard_read() {
                match self.map.get(key) {
                    Some(kv) => {
                        return (MemtableAccessState::Ok, Some(kv.val().clone()))
                    }

                    None => {
                        return (MemtableAccessState::Ok, None)
                    }
                }
            }
            return (MemtableAccessState::Retry , None)
        }
        return (state, None)
    }

    pub fn try_set(&self, key: &String, value: String) -> MemtableAccessState {
        self.tryset_or_remove(key, Some(value))
    }

    pub fn try_remove(&self, key: &String) -> MemtableAccessState {
        self.tryset_or_remove(key, None)
    }

    /// Set a new key-value mapping. This method may block when index is updating.
    fn tryset_or_remove(&self, key: &String, value: Option<String>) -> MemtableAccessState {
        let state = self.current_state();
        if MemtableAccessState::Ok == state {
            if let Some(_write_guard) = self.ctrl.guard_write() {
                // estimate size amount
                let mut len = key.len();
                if let Some(ref val) = value {
                    len += val.len();
                }
                // perform insertion/deleteion
                let is_remove = value.is_none();
                let key_exist =
                    self.map.insert(key.to_owned(), MemtableValue::Value(value)).is_some();
                // wake up background thread if size exceed flush threshold
                let size_after = self.uncompacted.fetch_add(len, SeqCst);
                if size_after > self.flush_limit {
                    self.wake_up();
                }
                // remove a non exist key should be treat specially
                if is_remove && !key_exist {
                    return MemtableAccessState::ErrRemoveNonExist;
                } else {
                    return MemtableAccessState::Ok;
                }
            }
            return MemtableAccessState::Retry;
        } else {
            return state
        }
    }

    /// This method can only be called when database is recovering or when database is doing flush/compaction.
    /// Because we have forbiden all read/write requests, so it's safe.
    pub fn raw_set(&self, key: &String, val: MemtableValue) {
        assert!(self.ctrl.check_write_paused());
        if let MemtableValue::Value(Some(_)) = &val {
            unreachable!();
        }

        self.map.insert(key.to_owned(), val);
    }

    /// This metchod can only be called when database is recovering, which is safe.
    pub fn raw_remove(&self, key: &String) {
        assert!(self.ctrl.check_write_paused());
        self.map.remove(key);
    }

    /// Whether flush or not.
    pub fn should_flush(&self) -> bool {
        self.uncompacted.load(Relaxed) >= self.flush_limit
    }

    /// Prepare for flush, reject all incoming write requests. The CtrlGuard returned from this function should
    /// be consumed when flush task is finish, just call `Memtable::finish_flush` and pass it as argument.
    pub fn prepare_flush(&self) -> CtrlGuard {
        self.state.compare_exchange(Self::_MEMTABLE_OK, Self::_MEMTABLE_FLUSHING, Acquire, Relaxed).unwrap();
        self.ctrl.pause_write()
    }

    /// Prepare for compaction. This function is just as same as `prepare_flush` expect that the incoming write requests may
    /// sleep for longer time. 
    pub fn prepare_compaction(&self) {
        self.state.compare_exchange(Self::_MEMTABLE_OK, Self::_MEMTABLE_COMPACTION, Acquire, Relaxed).unwrap();
    }

    /// Flush/compaction task is finished, Memtable could handle write requests or safe close. Use `CtrlGuard` returned from
    /// Memtable::prepare_flush_or_compaction as argument.
    pub fn finish_flush(&self, g: CtrlGuard) {
        assert!(self.ctrl.check_write_paused());
        drop(g);
    }

    /// Prepare for force flush even Memtable doesn't reach flush threshold.
    /// This method is valid iff no read/write is performing and no incomming read/write requests.
    pub fn prepare_force_flush(&self) {
        assert!(self.state.load(Relaxed) == Self::_MEMTABLE_OK && self.ctrl.check_no_pending_requests());
        self.uncompacted.store(self.flush_limit + 1, Relaxed);
    }

    /// Remove all keys in self.uncompacted_keys and return an iterator of all flushable kv-pairs.
    /// TODO: I'm afraid of writing unsafe code. Is there any elegant way to clean/take all values
    /// in self.uncompacted_keys?
    pub fn take_all_flushable(&self) -> impl Iterator<Item = ReadGuard<String, MemtableValue>> {
        assert!(self.ctrl.check_write_paused());
        let mut res = Vec::new();
        for key in self.uncompacted_keys.iter() {
            self.uncompacted_keys.remove(key.as_ref());
            res.push(self.map.get(key.as_ref()).unwrap());
        }
        self.uncompacted.store(0, Relaxed);
        res.into_iter()
    }

    /// Return an iterator across all key index.
    pub fn all(&self) -> impl Iterator<Item = ReadGuard<String, MemtableValue>> {
        assert!(self.ctrl.check_write_paused());
        self.map.iter()
    }

    fn current_state(&self) -> MemtableAccessState {
        match self.state.load(Relaxed) {
            Self::_MEMTABLE_OK => {
                MemtableAccessState::Ok
            }

            Self::_MEMTABLE_FLUSHING => {
                MemtableAccessState::FlushReject
            }

            Self::_MEMTABLE_COMPACTION => {
                MemtableAccessState::CompactionReject
            }

            _ => {
                unreachable!("???");
            }
        }
    }

    /// Wake up background thread to check Memtable. It always indicate a flush task should be scheduled.
    fn wake_up(&self) {
        self.bg_cond.notify_one();
    }
}
