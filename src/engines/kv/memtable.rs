use std::sync::{Condvar, Arc};
use std::sync::atomic::{AtomicUsize, AtomicU8, AtomicPtr};
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};

use lockfree::map::{Map as LockFreeMap, ReadGuard};

use super::access_control::{AccessController, CtrlGuard};
use super::bufring::{BufNode, BufNodePtr};
use super::compaction::Compactor;
use super::manifest::Rid;

type Vid = usize;

pub struct Memtable {
    state: AtomicU8,
    /// in-memory key index. The value is either a rid which indicate the pos of value in SSTable, or
    /// a value string.
    index: LockFreeMap<String, MemtableValue>,
    /// control the access to memtable.
    ctrl: AccessController,
    /// memtable version, increment with the schedule of flush/compaction
    vid: AtomicUsize,
    /// current-using uncompacted keys pointer.
    uncompacted_keys_ptr: AtomicPtr<BufNode>,

    bg_cond: Arc<Condvar>,
    /// for the optimization of compaction.
    flush_limit: usize,
    estimate_unflushed: AtomicUsize,
    estimate_total: AtomicUsize,
    estimate_total_compaction: AtomicUsize,
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
    Rid((Vid, Rid)),
    Value((Vid, Option<String>)),
}

impl Memtable {
    #[allow(dead_code)]
    pub const INVALID_VID: usize = 0;
    pub const INIT_VID: usize = 1;

    const _MEMTABLE_OK: u8 = 0;
    const _MEMTABLE_FLUSHING: u8 = 1;
    const _MEMTABLE_COMPACTION: u8 = 2;
    const _MEMTABLE_RETRY: u8 = 3;

    pub fn new(flush_limit: usize, bg_cond: Arc<Condvar>, init_buf_ptr: BufNodePtr) -> Self {
        Self {
            vid: AtomicUsize::new(Self::INIT_VID + 1),
            index: LockFreeMap::new(),
            ctrl: AccessController::default(),
            state: AtomicU8::new(Self::_MEMTABLE_OK),
            uncompacted_keys_ptr: AtomicPtr::new(init_buf_ptr.raw()),
            bg_cond,
            flush_limit,
            estimate_unflushed: AtomicUsize::new(0),
            estimate_total: AtomicUsize::new(0),
            estimate_total_compaction: AtomicUsize::new(0),
        }
    }

    pub fn try_get(&self, key: &String) -> (MemtableAccessState, Option<MemtableValue>) {
        let state = self.current_state();
        if MemtableAccessState::Ok == state {
            if let Some(_read_guard) = self.ctrl.guard_read() {
                match self.index.get(key) {
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
        self.try_set_or_remove(key, Some(value))
    }

    pub fn try_remove(&self, key: &String) -> MemtableAccessState {
        self.try_set_or_remove(key, None)
    }

    /// Set a new key-value mapping. This method may block when index is updating.
    fn try_set_or_remove(&self, key: &String, value: Option<String>) -> MemtableAccessState {
        let state = self.current_state();
        if MemtableAccessState::Ok == state {
            if let Some(_write_guard) = self.ctrl.guard_write() {
                // estimate size amount
                let key_len = key.len();
                let value_len = if let Some(ref val) = value {
                    val.len()
                } else {
                    0
                };
                // perform insertion/deleteion
                // safe because CtrlGuard is hold so vid won't advance.
                let vid = self.vid.load(Relaxed);
                let is_remove = value.is_none();
                let value_before
                    = self.index.insert(key.to_owned(), MemtableValue::Value((vid, value)));
                let exist = value_before.is_some();
                // the previous value's length
                let value_len_before = match value_before {
                    Some(key_value) => match key_value.val() {
                        MemtableValue::Rid((_, (_, (_, len)))) => *len,
                        MemtableValue::Value((_, val)) => match val {
                            Some(val) => val.len(),
                            None => 0,
                        },
                    },
                    None => 0,
                };

                // for esstimating the size after compaction
                let delta: isize = value_len as isize - value_len_before as isize;
                if delta >= 0 {
                    self.estimate_total_compaction.fetch_add(delta as usize, SeqCst);
                } else {
                    self.estimate_total_compaction.fetch_sub((-delta) as usize, SeqCst);
                };
                self.estimate_total.fetch_add(key_len + value_len, SeqCst);
                let prev_size = self.estimate_unflushed.fetch_add(key_len + value_len, SeqCst);
                let unflushed_size = prev_size + key_len + value_len;
                // wake up background thread if size exceed flush threshold
                if unflushed_size > self.flush_limit {
                    self.wake_up_flush_or_compaction();
                }

                if is_remove && !exist {
                    return MemtableAccessState::ErrRemoveNonExist
                } else {
                    return MemtableAccessState::Ok
                }

            }
            return MemtableAccessState::Retry;
        } else {
            return state
        }
    }

    pub fn raw_get(&self, key: &String) -> ReadGuard<String, MemtableValue> {
        self.index.get(key).unwrap()
    }

    /// This method can only be called when database is recovering or when database is doing flush/compaction.
    /// Because we have forbiden all read/write requests, so it's safe.
    pub fn raw_set(&self, key: &String, val: MemtableValue) {
        debug_assert!(self.ctrl.check_write_paused());
        if let MemtableValue::Value(_) = &val {
            unreachable!("raw set cannot pass ValueString as argument!");
        }
        self.index.insert(key.to_owned(), val);
    }

    /// This metchod can only be called when database is recovering, which is safe.
    pub fn raw_remove(&self, key: &String) {
        debug_assert!(self.ctrl.check_write_paused());
        self.index.remove(key);
    }

    /// Whether flush or not.
    pub fn should_flush_or_compact(&self) -> ShouldFlushOrCompact {
        if self.estimate_unflushed.load(Relaxed) <= self.flush_limit {
            return ShouldFlushOrCompact::No
        }
        let estimate = self.estimate_total.load(Relaxed);
        let estimate_after_compaction = self.estimate_total.load(Relaxed);
        if estimate > Compactor::INIT_COMPACT_LIMIT && estimate * 4 / 3 > estimate_after_compaction {
            return ShouldFlushOrCompact::Compact
        } else {
            return ShouldFlushOrCompact::Flush
        }
    }

    /// The buffer in Memtable is full, so we use a newly allocated buffer replace it.
    /// Note that when we call this method, the memtable's state is still MemtableState::Ok, which means it won't
    /// block the write requests. The data race of get reference of underlying LockFreeSet is avoid by atomic operation.
    pub fn swap_buf(&self, new_buf_ptr: BufNodePtr, is_compaction: bool) -> BufNodePtr {
        let old = self.uncompacted_keys_ptr.swap(new_buf_ptr.raw(), Acquire);
        // The real size of LockFreeSet maybe smaller because some write requests maybe happend between
        // the update of buffer ptr and the reset of self.estimate_unflushed. However, it's used for
        // raw estimation, so it needn't to be consistent with buffer's real size. And so does
        // estimate_tot and estimate_compaction.
        self.estimate_unflushed.store(0, SeqCst);
        if is_compaction {
            let estimate_compaction = self.estimate_total_compaction.load(Relaxed);
            self.estimate_total.store(estimate_compaction, SeqCst);
        }
        BufNodePtr::from(old)
    }

    pub fn pause_for_swap_buf(&self) -> MemtablePause {
        self.state.compare_exchange(
            Self::_MEMTABLE_OK,
            Self::_MEMTABLE_RETRY,
            Acquire,
            Relaxed)
        .unwrap();

        let ctrl_guard = self.ctrl.pause_write();
        MemtablePause {
            _safe_drop: false,
            _memtb: self,
            _ctrl: ctrl_guard,
            _after_state: Self::_MEMTABLE_OK,
             _current_state: Self::_MEMTABLE_RETRY,
        }
    }

    /// We have finished flush task and we should reset rid of flushed keys. During this period, we should reject all
    /// incoming write requests, otherwise we may face with inconsistency problem. Considering the sequence below:
    /// 
    /// thread_1 set key1:value1 and done -> thread_bg perform flush task -> thread_bg try to reset key1:value1 to key1:rid1
    /// thread_2 set key1:value2 -> thread_2 set done -> thread_bg set done -> thread_3 get key1 and found rid1, an outdated
    /// value.
    /// 
    /// During the lifetime of the returned PauseGuard, all write requests to Memtable is rejected. It won't block for 
    /// a long while because all operations is in-memory operation, without any IO or kernel trap.
    /// 
    /// The CtrlGuard returned from this function should be consumed when flush task is finish, just call `Memtable::finish_flush`
    /// and pass it as argument.
    pub fn pause_for_reset_flush_rids(&self) -> MemtablePause {
        self.state.compare_exchange(Self::_MEMTABLE_OK, Self::_MEMTABLE_FLUSHING, Acquire, Relaxed).unwrap();
        let ctrl_guard = self.ctrl.pause_write();
        MemtablePause {
            _safe_drop: false,
            _memtb: self,
            _ctrl: ctrl_guard,
            _after_state: Self::_MEMTABLE_OK,
            _current_state: Self::_MEMTABLE_FLUSHING,
        }
    }

    /// Reset rids for all compacted keys. This function is just as same as `prepare_flush` expect all cached uncompacted_keys
    /// will be removed because compaction task doesn't care of it, and self.state will change to _MEMTABLE_COMPACTION. 
    pub fn pause_for_reset_compaction_rids(&self) -> MemtablePause {
        self.state.compare_exchange(Self::_MEMTABLE_OK, Self::_MEMTABLE_COMPACTION, Acquire, Relaxed).unwrap();
        let ctrl_guard = self.ctrl.pause_write();
        let pause = MemtablePause {
            _safe_drop: false,
            _memtb: self,
            _ctrl: ctrl_guard,
            _after_state: Self::_MEMTABLE_OK,
            _current_state: Self::_MEMTABLE_COMPACTION,
        };
        pause
    }

    /// Flush/compaction/bufswap task is finished.
    pub fn finish(&self, mut g: MemtablePause) {
        debug_assert!(self.ctrl.check_write_paused());
        g._safe_drop = true;
        drop(g);
    }

    /// Prepare for force flush even Memtable doesn't reach flush threshold.
    /// This method is valid iff no read/write is performing and no incomming read/write requests.
    pub fn prepare_force_flush(&self) {
        debug_assert!(
            self.state.load(Relaxed) == Self::_MEMTABLE_OK &&
            self.ctrl.check_no_pending_requests()
        );
    }

    pub fn advance_version(&self) -> Vid {
        self.vid.fetch_add(1, Relaxed)
    }

    fn current_state(&self) -> MemtableAccessState {
        match self.state.load(Relaxed) {
            Self::_MEMTABLE_OK => {
                MemtableAccessState::Ok
            }

            Self::_MEMTABLE_RETRY => {
                MemtableAccessState::Retry
            }

            Self::_MEMTABLE_FLUSHING => {
                MemtableAccessState::FlushReject
            }

            Self::_MEMTABLE_COMPACTION => {
                MemtableAccessState::CompactionReject
            }

            _ => {
                unreachable!("")
            }
        }
    }

    /// wake up background thread to perform flush/compaction
    fn wake_up_flush_or_compaction(&self) {
        self.bg_cond.notify_all();
    }

    pub fn iter(&self) -> impl Iterator<Item = ReadGuard<String, MemtableValue>> {
        self.index.iter()
    }
}

pub struct MemtablePause<'a> {
    _safe_drop: bool, 
    _memtb: &'a Memtable,
    _ctrl: CtrlGuard<'a>,
    _after_state: u8,
    _current_state: u8,
}

impl<'a> Drop for MemtablePause<'a> {
    fn drop(&mut self) {
        if !self._safe_drop {
            eprintln!("PauseGuard doesn't consume in Memtable::flush_finish");
        }
        // This design is conflicted with 'Exception Safety Pattern'. But now
        // I just only want to focus on the correctiness of design.
        self._memtb.state.compare_exchange(
            self._current_state,
            self._after_state,
            Acquire,
            Relaxed)
        .unwrap();
    }
}

#[derive(PartialEq, Eq)]
pub enum ShouldFlushOrCompact {
    No,
    Flush,
    Compact,
}

#[cfg(test)]
mod memtable_unit_test {
    use std::ops::Range;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Condvar, Mutex};

    use crate::Result;
    use crate::engines::kv::bufring::BufNodePtr;
    use super::{Memtable, MemtableValue, MemtableAccessState};
    use super::super::bufring::create_buf_ring;

    struct RetryStatistic {
        swap_reject: usize,
        flush_reject: usize,
        compact_reject: usize,
    }

    fn retry_until_success<F, V, C, R> (
        func: F,
        cb: C,
        stat: &mut RetryStatistic
    ) -> R
    where
        F: Fn() -> (MemtableAccessState, V),
        C: FnOnce(V) -> R
    {
        let v: V;
        loop {
            let (state, vv) = func();
            match state {
                MemtableAccessState::Ok => {
                    v = vv;
                    break;
                },
                MemtableAccessState::Retry => {
                    stat.swap_reject += 1;
                },
                MemtableAccessState::FlushReject => {
                    stat.flush_reject += 1;
                },
                MemtableAccessState::CompactionReject => {
                    stat.compact_reject += 1;
                },
                MemtableAccessState::CloseReject | 
                MemtableAccessState::ErrRemoveNonExist => {
                    unreachable!("")
                },
            }
        }
        cb(v)
    }

    fn thread_do_range<F, V, C, R> (
        tid: usize,
        func: F,
        cb: C,
        rng: Range<usize>,
    )
    where
        F: Fn(usize, usize) -> (MemtableAccessState, V),
        C: Fn(usize, usize, V) -> R
    {
        let num_records = rng.len();
        let mut stat = RetryStatistic {
            swap_reject: 0,
            flush_reject: 0,
            compact_reject: 0,
        };

        for j in rng {
            let func2 = || func(tid, j);
            let cb2 = |v| cb(tid, j, v);
            retry_until_success(func2, cb2, &mut stat);
        }

        println!("
            thread {} set {} records,\n
            total retry: {}\n,
            again_retry: {}\n,
            flush_retry: {}\n,
            compaction_retry: {}\n", 
            tid, num_records,
            stat.swap_reject + stat.flush_reject + stat.compact_reject,
            stat.swap_reject,
            stat.flush_reject,
            stat.compact_reject,
        );
    }

    fn one_thread_set(
        memtable: Arc<Memtable>,
        tid: usize,
        rng: Range<usize>,
    ) {
        let func = |i: usize, j: usize| {
            let key = format!("key-{}-{}", i, j);
            let value = format!("value-{}-{}", i, j);
            ((&memtable).try_set(&key, value), ())
        };

        let cb = |_: usize, _: usize, _: ()| {
            ()
        };

        thread_do_range(tid, func, cb, rng)
    }

    fn one_thread_set_then (
        memtable: Arc<Memtable>,
        tid: usize,
        rng: Range<usize>,
        then: Box<dyn FnOnce(Arc<Memtable>, usize, Range<usize>)>
    ) {
        let rng2 = rng.clone();
        one_thread_set(memtable.clone(), tid, rng);
        then(memtable, tid, rng2);
    }

    fn one_thread_get_expected(
        memtable: Arc<Memtable>,
        tid: usize,
        rng: Range<usize>,
    ) {
        let func = |i: usize, j: usize| {
            let key = format!("key-{}-{}", i, j);
            (&memtable).try_get(&key)
        };

        let cb = |i: usize, j: usize, value: Option<MemtableValue>| {
            let expected = format!("value-{}-{}", i, j);
            if let Some(MemtableValue::Value((_, Some(value2)))) = value {
                assert_eq!(expected, value2);
            } else {
                panic!("get_expect got unexpected value");
            }
            ()
        };

        thread_do_range(tid, func, cb, rng);
    }

    fn one_thread_get_none(
        memtable: Arc<Memtable>,
        tid: usize,
        rng: Range<usize>
    ) {
        let func = |i: usize, j: usize| {
            let key = format!("key-{}-{}", i, j);
            (&memtable).try_get(&key)
        };

        let cb = |_: usize, _: usize, value: Option<MemtableValue>| {
            match value {
                Some(MemtableValue::Value((_, None))) => { },
                None => { }
                Some(v) => {
                    panic!("get_none got unexpected value: {:?}", v)
                }
            }
        };

        thread_do_range(tid, func, cb, rng);
    }

    fn one_thread_remove(
        memtable: Arc<Memtable>,
        tid: usize,
        rng: Range<usize>
    ) {
        let func = |i: usize, j: usize| {
            let key = format!("key-{}-{}", i, j);
            ((&memtable).try_remove(&key), ())
        };

        let cb = |_: usize, _: usize, _: ()| {
            ()
        };
        thread_do_range(tid, func, cb, rng)
    }

    fn one_thread_remove_then(
        memtable: Arc<Memtable>,
        tid: usize,
        rng: Range<usize>,
        then: Box<dyn FnOnce(Arc<Memtable>, usize, Range<usize>)>,
    ) {
        one_thread_remove(memtable.clone(), tid, rng.clone());
        then(memtable, tid, rng);
    }

    #[test]
    fn test_without_buf_swap() -> Result<()> {
        let num_threads = 100;
        let num_records_per_thread = 2000;
        let ring = create_buf_ring();
        let init_buf_ptr = ring.lock().unwrap().alloc_buf().unwrap();

        let memtable = Arc::new(Memtable::new(
            1 << 14,
            Arc::new(Condvar::new()),
            init_buf_ptr,
        ));

        // concurrent set test
        let mut handles = Vec::new();
        for tid in 0..num_threads {
            let memtable2 = memtable.clone();
            let handle = std::thread::spawn(move || {
                one_thread_set_then(
                    memtable2,
                    tid,
                    0..num_records_per_thread,
                    Box::new(one_thread_get_expected)
                );
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // remove half values and try got none.
        let mut handles = Vec::new();
        for tid in 0..num_threads {
            let memtable2 = memtable.clone();
            let handle = std::thread::spawn(move || {
                one_thread_remove_then(
                    memtable2,
                    tid,
                    0..num_records_per_thread/2,
                    Box::new(one_thread_get_none)
                )
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // got remaining values.
        let mut handles = Vec::new();
        for tid in 0..num_threads {
            let memtable2 = memtable.clone();
            let handle = std::thread::spawn(move || {
                one_thread_get_expected(
                    memtable2,
                    tid,
                    num_records_per_thread/2..num_records_per_thread
                )
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }

        Ok(())
    }

    #[test]
    fn test_with_bufswap() -> Result<()> {
        use super::ShouldFlushOrCompact;

        let ring = Arc::new(create_buf_ring());
        let bg_cond = Arc::new(Condvar::new()); 
        let memtable = Arc::new(Memtable::new(
            1<<14,
            bg_cond.clone(),
            ring.lock().unwrap().alloc_buf().unwrap()
        ));
        
        let cleaner_cond = Arc::new(Condvar::new());
        let monitor_cleaner_cond = cleaner_cond.clone();
        let monitor_ring = ring.clone();
        let monitor_cond = bg_cond.clone();
        let monitor_memtable = memtable.clone();

        let (sender, receiver) = channel();
        let monitor = move |tx: std::sync::mpsc::Sender<BufNodePtr>| {
            loop {
                match monitor_memtable.should_flush_or_compact() {
                    ShouldFlushOrCompact::No => {
                        continue;
                    },

                    ShouldFlushOrCompact::Flush | ShouldFlushOrCompact::Compact => {
                        let bufptr = loop {
                            match monitor_ring.lock().unwrap().alloc_buf() {
                                Some(bufptr) => {
                                    break bufptr
                                },
                                None => {
                                    continue;
                                },
                            }
                        };

                        let pause = monitor_memtable.pause_for_swap_buf();
                        let old = monitor_memtable.swap_buf(bufptr, false);
                        monitor_memtable.finish(pause);
                        tx.send(old).unwrap();
                    }
               }
            }
        };
        
        let cleaner_ring = ring.clone();
        let cleaner_me_cond = cleaner_cond.clone();
        let cleaner = move |rx: std::sync::mpsc::Receiver<BufNodePtr>| {
            loop {
                match rx.recv() {
                    Err(_) => {
                        break;
                    }

                    Ok(bufptr) => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                        cleaner_ring.lock().unwrap().release_buf(bufptr);
                    }
                }
            }
        };

        let monitor_handle = std::thread::spawn(move || monitor(sender));
        let cleaner_handle = std::thread::spawn(move || cleaner(receiver));
        
        let num_threads = 100;
        let num_records_per_thread = 2000;

        let mut handles = Vec::new();
        for tid in 0..num_threads {
            let memtable2 = memtable.clone();
            let handle = std::thread::spawn(move || {
                one_thread_set_then(
                    memtable2,
                    tid,
                    0..num_records_per_thread,
                    Box::new(one_thread_get_expected)
                );
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        
        Ok(())
    }

    // #[test]
    // fn multi_thread_test() -> Result<()> {
    //     let nthreads = 100;
    //     let record_per_thread = 2000;

    //     let ring = Arc::new(create_buf_ring());
    //     let cond = Arc::new(Condvar::new());
    //     let running = Arc::new(Mutex::new(true));
    //     let memtable = Arc::new(
    //         Memtable::new(
    //             1<<14,
    //             cond.clone(),
    //             ring.lock().unwrap().alloc_buf().unwrap())
    //         );

    //     let bg_ring = ring.clone();
    //     let bg_cond = cond.clone();
    //     let bg_running = running.clone();
    //     let bg_memtable = memtable.clone();
    //     let bg_flush_compaction_handle = std::thread::spawn(move || {
    //         loop {
    //             let running_guard = bg_running.lock().unwrap();
    //             if !*running_guard {
    //                 break;
    //             }
    //             _ = bg_cond.wait(running_guard);
            
                
    //             if !bg_memtable.should_flush() {
    //                 continue;
    //             }
    //             // mock flush or compaction
    //             let x: u8 = rand::random();
    //             let _guard;

    //             let ptr = loop {
    //                 match bg_ring.lock().unwrap().alloc_buf() {
    //                     Some(new_buf_ptr) => {
    //                         println!("alloc buf");
    //                         break new_buf_ptr
    //                     },
    //                     None => {
    //                         std::thread::sleep(std::time::Duration::from_millis(10));
    //                         continue;
    //                     },
    //                 }
    //             };

    //             if 0 == (x & 1) {
    //                 // let node = bg_ring.lock().unwrap().alloc_buf();
    //                 // _guard = bg_memtable.prepare_reset_compaction_rids();
    //                 // std::thread::sleep(std::time::Duration::from_millis(50));
    //             } else {
    //                 // _guard = bg_memtable.prepare_reset_flushed_rids();
    //                 std::thread::sleep(std::time::Duration::from_millis(10));
    //             }
    //             // pretend that we have take all uncompacted keys...
    //             bg_memtable.estimitate.store(0, SeqCst);
    //             // bg_memtable.finish_flush(_guard);
    //         }
    //     });

    //     let mut handles = Vec::new();
    //     for i in 0..nthreads {
    //         let memtable2 = memtable.clone();
    //         let h = std::thread::spawn(move || {
    //             for j in 0..record_per_thread {
    //                 let key = format!("key-{}-{}", i, j);
    //                 let value = format!("value-{}-{}", i, j);

    //                 loop {
    //                     match memtable2.try_set(&key, (&value).to_owned()) {
    //                         super::MemtableAccessState::Ok => {
    //                             break;
    //                         },

    //                         super::MemtableAccessState::Retry => {
    //                             continue;
    //                         },

    //                         super::MemtableAccessState::FlushReject => {
    //                             println!("flush reject");
    //                             std::thread::sleep(std::time::Duration::from_millis(10));
    //                         },

    //                         super::MemtableAccessState::CompactionReject => {
    //                             println!("compaction reject");
    //                             std::thread::sleep(std::time::Duration::from_millis(50));
    //                         },

    //                         _ => {
    //                             unreachable!("");
    //                         }
    //                     }
    //                 }
    //             }
    //             println!("thread {} exit", i);
    //         });
    //         handles.push(h);
    //     }

    //     for h in handles {
    //         h.join().unwrap();
    //     }

    //     *running.lock().unwrap() = false;
    //     cond.notify_all();
    //     bg_flush_compaction_handle.join().unwrap();

    //     for i in 0..nthreads {
    //         for j in 0..record_per_thread {
    //             let key = format!("key-{}-{}", i, j);
    //             let value = format!("value-{}-{}", i, j);

    //             loop {
    //                 match memtable.try_get(&key) {
    //                     (super::MemtableAccessState::Ok, res) => {
    //                         if let Some(MemtableValue::Value(valstr)) = res {
    //                             assert_eq!(valstr.1.unwrap(), value);
    //                         } else {
    //                             panic!("get failed");
    //                         }
    //                         break;
    //                     },

    //                     (super::MemtableAccessState::Retry, _) => {
    //                         continue;
    //                     },

    //                     _ => {
    //                         unreachable!("");
    //                     }
    //                 }
    //             }
    //         }
    //     }

    //     let mut handles = Vec::new();
    //     for i in 0..nthreads {
    //         let memtable2 = memtable.clone();
    //         let handle = std::thread::spawn(move || {
    //             for j in 0..record_per_thread/2 {
    //                 let key = format!("key-{}-{}", i, j);

    //                 loop {
    //                     match memtable2.try_remove(&key) {
    //                         super::MemtableAccessState::Ok => {
    //                             break;
    //                         }
    //                         super::MemtableAccessState::Retry => {
    //                             continue;
    //                         }
    //                         super::MemtableAccessState::FlushReject => {
    //                             std::thread::sleep(std::time::Duration::from_millis(10));
    //                             continue;
    //                         }
    //                         super::MemtableAccessState::CompactionReject => {
    //                             std::thread::sleep(std::time::Duration::from_millis(50));
    //                             continue;
    //                         }
    //                         _ => {
    //                             unreachable!("");
    //                         }
    //                     }
    //                 }
    //             }

    //             for j in 0..record_per_thread {
    //                 let key = format!("key-{}-{}", i, j);
    //                 let value = format!("value-{}-{}", i, j);
    //                 loop {
    //                     match memtable2.try_get(&key) {
    //                         (super::MemtableAccessState::Ok, res) => {
    //                             if j < record_per_thread/2 {
    //                                 // values should be removed.
    //                                 if let Some(MemtableValue::Value((_, None))) = res {
    //                                     // success
    //                                 } else {
    //                                     panic!("remove failed");
    //                                 }
    //                             } else {
    //                                 // values should be keeped.
    //                                 if let Some(MemtableValue::Value(valstr)) = res {
    //                                     assert_eq!(valstr.1.unwrap(), value);
    //                                 } else {
    //                                     panic!("get failed");
    //                                 }
    //                             }
    //                             break;
    //                         },

    //                         (super::MemtableAccessState::Retry, _) => {
    //                             continue;
    //                         },

    //                         _ => {
    //                             unreachable!("");
    //                         }
    //                     }
    //                 }
    //             }

    //             println!("thread {} exit", i);
    //         });
    //         handles.push(handle);
    //     }

    //     Ok(())
    // }
}