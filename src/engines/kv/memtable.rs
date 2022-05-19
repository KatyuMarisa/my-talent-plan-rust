use std::sync::{Condvar, Arc};
use std::{sync::atomic::AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};

use lockfree::map::{Map as ThreadSafeMap};
use lockfree::map::Removed;

use crate::{Result, KvError};

use super::manifest::RID;

pub struct Memtable {
    map: ThreadSafeMap<String, Value>,
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
}

#[derive(Clone, Debug)]
pub enum Value {
    RID(RID),
    ValueStr(String),
    TOMB,
}

impl Memtable {
    const PINCOUNT_FLUSH: usize = 1 << 12;
    const PINCOUNT_COMPACTION: usize = 1 << 13;

    pub fn new(flush_limit: usize, bd_cond: Arc<Condvar>) -> Self {
        Self {
            map: ThreadSafeMap::new(),
            pin_count: AtomicUsize::new(0),
            uncompacted: AtomicUsize::new(0),
            flush_limit,
            bg_cond: Arc::clone(&bd_cond),
        }
    }

    pub fn get(&self, key: String) -> Result<(MemtableState, Option<Value>)> {
        let state = self.pin_read_write();
        if state != MemtableState::Ok {
            return Ok((state, None))
        }

        match self.map.get(&key) {
            Some(v) => {
                self.unpin_read_write();
                return Ok((MemtableState::Ok, Some(v.val().clone())))
            }

            None => {
                self.unpin_read_write();
                return Ok((MemtableState::Ok, None))                
            }
        }
       
    }

    pub fn set(&self, key: String, value: String) -> Result<MemtableState> {
        let state = self.pin_read_write();
        if state != MemtableState::Ok {
            return Ok(state)
        }

        let len = value.len();
        self.map.insert(key, Value::ValueStr(value));
        let size_after = self.uncompacted.fetch_add(len, SeqCst);
        self.unpin_read_write();

        if size_after >= self.flush_limit {
            self.wake_up_flush();
        }
        return Ok(MemtableState::Ok)
    }

    pub fn remove(&self, key: String) -> Result<MemtableState> {
        let state = self.pin_read_write();
        if state != MemtableState::Ok {
            return Ok(state);
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
        // Such solution will be more effective if most remove operation is invalid, which is
        // really rare. On the other hand, such operation will return an error, which is very 
        // heavy (because of the resolve of symbol table and runtime support). So we don't 
        // choose the solution above.
        let len = key.len();
        let key_found: bool = self.map.insert(key.to_owned(), Value::TOMB).is_some();
        let size_after = self.uncompacted.fetch_add(len, SeqCst);
        if size_after > self.flush_limit {
            self.wake_up_flush();
        }

        if key_found {
            self.unpin_read_write();
            return Ok(MemtableState::Ok)
        } else {
            self.unpin_read_write();
            return Err(KvError::KeyNotFoundError{key}.into())
        }
    }

    pub fn unpin_set(&self, key: &String, val: Value) {
        assert!(self.pin_count.load(Relaxed) == 0);
        self.map.insert(key.to_owned(), val.clone());        
        match val {
            Value::ValueStr(s) => { self.uncompacted.fetch_add(s.len(), Relaxed); }
            Value::TOMB => { self.uncompacted.fetch_add(key.len(), Relaxed); }
            Value::RID(_) => { }
        }
    }

    pub fn unpin_remove(&self, key: &String) {
        assert!(self.pin_count.load(Relaxed) == 0);
        // This function can only be called when database is recovering, so directly remove is safe.
        self.map.remove(key);
    }

    pub fn remove_flushable(&self) -> Vec<(String, Value)> {
        assert!(self.pin_count.load(Relaxed) == Self::PINCOUNT_FLUSH);
        let keys = self.map.iter().filter(|kv|{
            if let Value::RID(_) = kv.val() {
                return false;
            }
            return true;
        });

        let mut should_flush = Vec::<(String, Value)>::new();
        for kv in keys {
            let rm: Removed<String, Value> = self.map.remove(kv.key()).unwrap();
            // TODO: Wtf and why?
            should_flush.push(
                Removed::<String, Value>::try_into(rm).unwrap()
            );
        }
        return should_flush;
    }

    pub fn remove_all(&self) -> Vec<(String, Value)> {
        assert!(self.pin_count.load(Relaxed) == Self::PINCOUNT_COMPACTION);
        let keys = self.map.iter();
        let mut all_values = Vec::new();
        for kv in keys {
            let rm = self.map.remove(kv.key()).unwrap();
            all_values.push(
                Removed::<String, Value>::try_into(rm).unwrap()
            );
        }
        return all_values;
    }

    pub fn should_flush(&self) -> bool {
        return self.uncompacted.load(Acquire) >= self.flush_limit;
    }

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
                        std::thread::sleep(std::time::Duration::from_millis(10));
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

    // pub fn dump(&self) -> Result<()> {
    //     if !self.pin_compaction() {
    //         return Ok(())
    //     }
    //     // println!("do compaction!");
    //     let pin = self.pin_count.load(Relaxed);
    //     if pin != Self::PINCOUNT_SHOULD_COMPACT {
    //         return Err(KvError::MaybeCorrput.into());
    //     }

    //     // TODO: clear whole map without borrow mut
    //     // Maybe use cell?
    //     let mut keys = Vec::new();
    //     for entry in self.map.iter() {
    //         keys.push(entry.key().to_owned());
    //     }
    //     for k in keys {
    //         self.map.remove(&k);
    //     }
    //     // std::thread::sleep(
    //     //     std::time::Duration::from_millis(50)
    //     // );

    //     self.uncompacted.store(0, SeqCst);
    //     self.unpin_compaction();
    //     // println!("compaction success");
    // pub fn dump(&self) -> Result<()> {
    //     if !self.pin_compaction() {
    //         return Ok(())
    //     }
    //     // println!("do compaction!");
    //     let pin = self.pin_count.load(Relaxed);
    //     if pin != Self::PINCOUNT_SHOULD_COMPACT {
    //         return Err(KvError::MaybeCorrput.into());
    //     }

    //     // TODO: clear whole map without borrow mut
    //     // Maybe use cell?
    //     let mut keys = Vec::new();
    //     for entry in self.map.iter() {
    //         keys.push(entry.key().to_owned());
    //     }
    //     for k in keys {
    //         self.map.remove(&k);
    //     }
    //     // std::thread::sleep(
    //     //     std::time::Duration::from_millis(50)
    //     // );

    //     self.uncompacted.store(0, SeqCst);
    //     self.unpin_compaction();
    //     // println!("compaction success");
    //     Ok(())
    // }

    /// Return false if serious data race is happening, or a background compaction task is running.
    /// If true is returned, all read/write access is guaranted to be safe.
    /// Should `unpin_read_write` after a read/write access is complete.
    fn pin_read_write(&self) -> MemtableState {
        let mut retry = 5;
        loop {
            if 0 == retry {
                return MemtableState::AccessRace;
            }
            let pin = self.pin_count.load(Acquire);
            if pin >= Self::PINCOUNT_FLUSH {
                return MemtableState::FlushRace;
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
    use std::sync::atomic::AtomicUsize;

    use crate::Result;
    use super::Memtable;
    use super::MemtableState;

    // #[test]
    // fn test_multi_thread() -> Result<()> {
    //     let memtb = Arc::new(Memtable::new(1 << 13));
    //     let mut threads = Vec::new();
    //     for i in 0..50 {
    //         let tb = Arc::clone(&memtb);
    //         let handle = std::thread::spawn(move || {
    //             for j in 0..5000 {
    //                 let key = format!("key-{}-{}", i, j);
    //                 let value = format!("value-{}-{}", i, j);
    //                 loop {
    //                     match tb.set(key.to_owned(), value.to_owned()) {
    //                         Ok(state) => {
    //                             match state {
    //                                 MemtableState::Ok => {
    //                                     break;
    //                                 }

    //                                 MemtableState::AccessRace => {
    //                                     println!("race on data access");
    //                                 }

    //                                 MemtableState::FlushRace => {
    //                                     // println!("race on compaction!");
    //                                     std::thread::sleep(
    //                                         std::time::Duration::from_millis(50)
    //                                     );
    //                                 }

    //                                 MemtableState::ShouldCompact => {
    //                                     tb.dump().unwrap();
    //                                 }
    //                             }
    //                         }

    //                         Err(err) => {
    //                             if let Some(crate::KvError::DataRace) = err.downcast_ref::<crate::KvError>() {
    //                                 // nothing todo
    //                             } else {
    //                                 println!("{}", err);
    //                                 std::process::exit(1);
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //             println!("thread {} end", i);
    //         });
    //         threads.push(handle);
    //     }

    //     for h in threads {
    //         h.join().unwrap();
    //     }

    //     Ok(())
    // }
}