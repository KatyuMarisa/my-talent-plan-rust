use std::sync::{Condvar, Arc};
use std::{sync::atomic::AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};

use lockfree::map::{Map as LockFreeMap, Iter, ReadGuard};
use lockfree::set::Set as LockFreeSet;

use super::manifest::RID;

pub struct Memtable {
    map: LockFreeMap<String, MemtableValue>,
    uncompacted_keys: LockFreeSet<String>,
    pin_count: AtomicUsize,
    uncompacted: AtomicUsize,
    flush_limit: usize,
    bg_cond: Arc<Condvar>,
}

#[derive(PartialEq)]
pub enum MemtableState {
    Ok,
    FlushRace,
    AccessRace,
    CompactionRace,
    KeyNotExist(String),
}

#[derive(Clone, Debug)]
pub enum MemtableValue {
    RID(RID),
    Value(Option<String>),
}

impl Memtable {
    const PINCOUNT_FLUSH: usize = 1 << 12;
    const PINCOUNT_COMPACTION: usize = 1 << 13;

    pub fn new(flush_limit: usize, bd_cond: Arc<Condvar>) -> Self {
        Self {
            map: LockFreeMap::new(),
            pin_count: AtomicUsize::new(0),
            uncompacted_keys: LockFreeSet::new(),
            uncompacted: AtomicUsize::new(0),
            flush_limit,
            bg_cond: Arc::clone(&bd_cond),
        }
    }

    pub fn get(&self, key: String) -> (MemtableState, Option<MemtableValue>) {
        let state = self.pin_read_write();
        if state != MemtableState::Ok {
            return (state, None)
        }

        match self.map.get(&key) {
            Some(v) => {
                self.unpin_read_write();
                return (MemtableState::Ok, Some(v.val().clone()))
            }

            None => {
                self.unpin_read_write();
                return (MemtableState::Ok, None)
            }
        }
    }

    pub fn set(&self, key: String, value: String) -> MemtableState {
        let state = self.pin_read_write();
        if state != MemtableState::Ok {
            return state
        }

        let len = value.len();
        self.map.insert(key.to_owned(), MemtableValue::Value(Some(value)));
        // It seems that [insert into self.map] and [insert into self.uncompacted_keys] should be done linearizationly.
        // However, both `set` and `remove` operation is insert a new entry into self.uncompacted_keys, so it must be
        // thread-safe...maybe or not?
        self.uncompacted_keys.insert(key).unwrap_or_default();
        let size_after = self.uncompacted.fetch_add(len, SeqCst);
        self.unpin_read_write();

        if size_after >= self.flush_limit {
            self.wake_up_flush();
        }
        return MemtableState::Ok
    }

    pub fn remove(&self, key: String) -> MemtableState {
        let state = self.pin_read_write();
        if state != MemtableState::Ok {
            return state
        }
        // We could *not* remove directly from self.map because such key may also exist in SSTable.
        // A key is guarented to be removed if a TOMB record is flushed into SSTable. 
        // 
        // Another choosable solution in pseudo code:
        // 
        // defer self.unpin_read_write();
        // if !self.map.has_key(&key) {
        //     return KeyNotExist
        // }
        // 
        // if self.map.insert(key, Value::TOMB).is_some() {
        //     return Ok
        // } else {
        //     return KeyNotExist
        // }
        // 
        // Such solution will be more effective if most operation try to remove non-exist key, which is
        // really rare. On the other hand, such operation will return an error, which is very heavy 
        // (because of the resolve of symbol table and runtime support). So we don't choose the solution above.
        // 
        let len = key.len();
        let key_found: bool = self.map.insert(key.to_owned(), MemtableValue::Value(None)).is_some();
        self.uncompacted_keys.insert(key.to_owned()).unwrap_or_default();
        let size_after = self.uncompacted.fetch_add(len, SeqCst);
        if size_after > self.flush_limit {
            self.wake_up_flush();
        }

        if key_found {
            self.unpin_read_write();
            return MemtableState::Ok
        } else {
            self.unpin_read_write();
            return MemtableState::KeyNotExist(key)
        }
    }

    // This method can only be invoked when database is recovering or when database is doing flush/compaction.
    // Because we have forbiden all read/write requests, so it's safe.
    pub fn raw_set(&self, key: &String, val: MemtableValue) {
        let pin = self.pin_count.load(Relaxed);
        assert!(pin == 0 ||
            pin == Self::PINCOUNT_FLUSH ||
            pin == (Self::PINCOUNT_COMPACTION + Self::PINCOUNT_FLUSH));

        self.map.insert(key.to_owned(), val.clone());        
        match val {
            MemtableValue::Value(None) => {
                self.uncompacted.fetch_add(key.len(), Relaxed);
            }

            MemtableValue::RID(rid) => {
                // nothing todo
            }

            _ => {
                unreachable!();
            }
        }
    }

    // This metchod can only be invoked when database is recovering, which is safe.
    pub fn raw_remove(&self, key: &String) {
        assert!(self.pin_count.load(Relaxed) == 0);
        self.map.remove(key);
    }

    pub fn remove_flushable(&self) -> impl Iterator<Item = ReadGuard<String, MemtableValue>> {
        assert!(self.pin_count.load(Relaxed) == Self::PINCOUNT_FLUSH);
        // TODO: Learn more about move.
        self.uncompacted.store(0, SeqCst);
        self.uncompacted_keys.iter().map(move |guard| {
            self.uncompacted_keys.remove(guard.as_ref());
            self.map.get(guard.as_ref()).unwrap()
        })

        // let mut should_flush = Vec::new();

        // for key_guard in self.uncompacted_keys.iter() {
        //     self.uncompacted_keys.remove(key_guard.as_ref()).unwrap();
        //     should_flush.push(self.map.get(key_guard.as_ref()).unwrap());
        // }

        // return should_flush.into_iter();

        // for guard in self.uncompacted_keys.iter() {
        //     self.uncompacted_keys.remove(guard.as_ref());
        //     should_flush.push(self.map.get(guard.as_ref()).unwrap());
        // }


        // return should_flush.into_iter()
        // unimplemented!()

        // self.uncompacted.store(0, SeqCst);
        // return should_flush;

        // let keys = self.uncompacted_keys.iter().collect::<Vec<_>>();
        // let mut should_flush = Vec::new();

        // for key in keys {
        //     should_flush.push(self.map.get(key.as_ref()).unwrap());
        //     self.uncompacted_keys.remove(key.as_ref());
        // }
        // self.uncompacted.store(0, SeqCst);
        // return should_flush;
        
        // assert!(self.pin_count.load(Relaxed) == Self::PINCOUNT_FLUSH);
        // let keys = self.map.iter()
        //     .filter(|kv| {
        //         if let MemtableValue::RID(_) = kv.val() {
        //             return false;
        //         }
        //         return true;
        //     });
 
        // // prepare a big buffer
        // let mut should_flush = Vec::<(String, MemtableValue)>::with_capacity(10000);
        // for kv in keys {
        //     self.map.remove(kv.key()).unwrap();
        //     should_flush.push(
        //         // Removed::<String, MemtableValue>::try_into(rm).unwrap()
        //         (kv.key().to_owned(), kv.val().to_owned())
        //     );
        // }
        // // reset uncompacted bytes.
        // self.uncompacted.store(0, SeqCst);
        // return should_flush;
    }

    pub fn remove_all(&self) -> impl Iterator<Item = ReadGuard<String, MemtableValue>> {
        assert!(self.pin_count.load(Relaxed) == Self::PINCOUNT_COMPACTION + Self::PINCOUNT_FLUSH);
        // assert!(self.uncompacted_keys.iter().count() == 0);  time consuming
        self.map.iter()

        // let keys = self.map.iter();
        // let mut all_values = Vec::with_capacity(10000);
        // let mut removed_uncompacted_size: usize = 0;
        
        // for kv in keys {
        //     self.map.remove(kv.key()).unwrap();
        //     if let MemtableValue::Value(Some(valuestr)) = kv.val() {
        //         removed_uncompacted_size += valuestr.len();
        //     } else {
        //         removed_uncompacted_size += kv.key().len();
        //     }
        
        //     all_values.push(
        //         // Removed::<String, MemtableValue>::try_into(rm).unwrap()
        //         (kv.key().to_owned(), kv.val().to_owned())
        //     );
        // }
        // return all_values;
    }

    pub fn should_flush(&self) -> bool {
        return self.uncompacted.load(Acquire) >= self.flush_limit;
    }

    // TODO: maybe hungry.
    pub fn pin_flush(&self) -> bool {
        loop {
            let pin = self.pin_count.load(Acquire);
            if pin >= Self::PINCOUNT_FLUSH {
                return false;
            }
            
            if let Ok(_) = self.pin_count.compare_exchange(pin, pin + Self::PINCOUNT_FLUSH,
                Acquire, Relaxed) {
                loop {
                    assert!(self.pin_count.load(Relaxed) >= Self::PINCOUNT_FLUSH);
                    if Self::PINCOUNT_FLUSH == self.pin_count.load(Acquire) {
                        return true;
                    } else {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }
        }
    }

    pub fn unpin_flush(&self) {
        self.pin_count.compare_exchange(Self::PINCOUNT_FLUSH,
            0, Acquire, Relaxed).unwrap();
    }

    pub fn pin_compaction(&self) {
        self.pin_count.compare_exchange(Self::PINCOUNT_FLUSH,
            Self::PINCOUNT_COMPACTION + Self::PINCOUNT_FLUSH, Acquire, Relaxed).unwrap();
    }

    pub fn unpin_compaction(&self) {
        self.pin_count.compare_exchange(Self::PINCOUNT_COMPACTION + Self::PINCOUNT_FLUSH,
            Self::PINCOUNT_FLUSH, Acquire, Relaxed).unwrap();
    }

    // only valid if no read/write is performing
    pub fn stop_and_prepare_flush_memtable(&self) {
        self.pin_count.compare_exchange(0, Self::PINCOUNT_FLUSH,  Acquire, Relaxed).unwrap();
        self.uncompacted.store(self.flush_limit + 1, SeqCst);
    }

    /// If MemtableState::Ok is returned, all read/write access before unpn_read_write
    /// is guaranted to be safe.
    /// Should `unpin_read_write` after a read/write access is complete.
    fn pin_read_write(&self) -> MemtableState {
        let mut retry = 5;
        loop {
            if 0 == retry {
                return MemtableState::AccessRace;
            }
            let pin = self.pin_count.load(Acquire);
            if pin >= Self::PINCOUNT_FLUSH && pin < Self::PINCOUNT_COMPACTION {
                return MemtableState::FlushRace;
            } else if pin >= Self::PINCOUNT_COMPACTION {
                return MemtableState::CompactionRace;
            }

            if let Ok(_) = self.pin_count.compare_exchange(pin, pin + 1, Acquire, Relaxed) {
                return MemtableState::Ok;
            } else {
                // nothing to do.
            }
            retry -= 1;
        }
    }

    /// unpin read/write access.
    fn unpin_read_write(&self) {
        self.pin_count.fetch_sub(1, Acquire);
    }

    /// Return false if compaction already completed, or another thread is doing compaction.
    /// If true is returned, this thread must take charge of compaction task.
    fn wake_up_flush(&self) {
        self.bg_cond.notify_one();
    }
}

#[cfg(test)]
mod memtable_unit_test {
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use rand::random;

    use crate::Result;
    use super::Memtable;
    use super::MemtableState;
    use super::MemtableValue;

    #[test]
    fn test_multi_thread() -> Result<()> {
        let cond = Arc::new(Condvar::new());
        let memtb = Arc::new(Memtable::new(1 << 14, cond.clone()));
        
        let cond2 = cond.clone();
        let memtb2 = memtb.clone();
        let running = Arc::<Mutex<bool>>::new(Mutex::new(true));
        let running2 = running.clone();
        let bg_flush_compaction = std::thread::spawn(move || {
            loop {
                {
                    let guard = running2.lock().unwrap();
                    if !(*guard) {
                        break;
                    }
                    _ = cond2.wait(guard);
                }

                if !memtb2.should_flush() {
                    continue;
                }

                if !memtb2.pin_flush() {
                    panic!("should not happend");
                }

                // mock flush or compaction
                let x: u8 = random();
                if 0 == x % 2 {
                    // flush
                    memtb2.pin_flush();
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    memtb2.unpin_flush();
                } else {
                    // compaction
                    memtb2.pin_flush();
                    memtb2.pin_compaction();
                    std::thread::sleep(std::time::Duration::from_millis(30));
                    memtb2.unpin_compaction();
                    memtb2.unpin_flush();
                }
            }
            println!("safe exit");
        });

        let thread_nums = 100;
        let mut threads = Vec::new();
        for i in 0..thread_nums {
            let memtb_handle = memtb.clone();
            let thread_handle = std::thread::spawn(move || {
                for j in 0..2000 {
                    let key = format!("key-{}-{}", i, j);
                    let value = format!("value-{}-{}", i, j);

                    loop {
                        match memtb_handle.set(key.to_owned(), value.to_owned()) {
                            MemtableState::Ok => { break; },
                            MemtableState::AccessRace => { println!("Access Race!"); },
                            MemtableState::FlushRace => { std::thread::sleep(std::time::Duration::from_millis(10)); },
                            MemtableState::CompactionRace => { std::thread::sleep(std::time::Duration::from_millis(50)); },
                            MemtableState::KeyNotExist(_) => { panic!("should not happend"); },
                        }
                    }
                }
            });
            threads.push(thread_handle);
        }
        
        for h in threads {
            h.join().unwrap();
        }

        // kill the background thread.
        {
            let mut guard = running.lock().unwrap();
            *guard = false;
            cond.notify_one();
        }

        bg_flush_compaction.join().unwrap();

        for i in 0..thread_nums {
            for j in 0..2000 {
                let key = format!("key-{}-{}", i, j);
                let value = format!("value-{}-{}", i, j);
                let (_, value2) = memtb.get(key);
                if let Some(MemtableValue::Value(Some(value3))) = value2 {
                    assert!(value3 == value);
                } else {
                    assert!(false);
                }
            }
        }

        Ok(())
    }
}