use std::sync::Mutex;
use lockfree::set::Set as LockFreeSet;

pub struct BufNode {
    id: usize,
    val: LockFreeSet<String>,
}

impl BufNode {
    pub fn as_inner(&self) -> &LockFreeSet<String> {
        &self.val
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct BufNodePtr(*mut BufNode);

unsafe impl Send for BufNodePtr {}

impl AsRef<BufNode> for BufNodePtr {
    fn as_ref(&self) -> &BufNode {
        unsafe { self.0.as_ref().unwrap() }
    }
}

impl From<*mut BufNode> for BufNodePtr {
    fn from(raw: *mut BufNode) -> Self {
        Self(raw)
    }
}

impl BufNodePtr {
    pub fn raw(&self) -> *mut BufNode {
        self.0
    }
}

pub struct BufRing {
    buf: Vec<BufNode>,
    free: Vec<usize>,
    inuse: Vec<usize>,
}

pub fn create_buf_ring() -> Mutex<BufRing> {
    Mutex::new(BufRing::new())
}

impl BufRing {
    const _CAP: usize = 16;

    fn new() -> Self {
        let mut buf = Vec::new();
        for id in 0..Self::_CAP {
            buf.push(BufNode { id, val: LockFreeSet::new() });
        }

        let mut free = Vec::new();
        for id in 0..Self::_CAP {
            free.push(id);
        }

        Self {
            buf,
            free,
            inuse: Vec::new(),
        }
    }

    pub fn release_buf(&mut self, bufptr: BufNodePtr) {
        // Safety: BufNodePtr points to inner struct and is guard by Mutex wrapper to avoid data race,
        // so it's safe to use a mut reference.
        let at_buf = bufptr.as_ref().id;

        *self.buf.get_mut(at_buf).unwrap() = BufNode { id: at_buf, val: LockFreeSet::new() };

        let as_inused_list = self.inuse.iter().position(|e| *e == at_buf)
            .expect("this buf is not inuse!");
        self.inuse.remove(as_inused_list);
        self.free.push(at_buf);
    }

    pub fn alloc_buf(&mut self) -> Option<BufNodePtr> {
        match self.free.pop() {
            Some(idx) => {
                self.inuse.push(idx);
                let raw_ptr = 
                    BufNodePtr::from(self.buf.get_mut(idx).unwrap() as *mut BufNode);
                Some(raw_ptr)
            }

            None => {
                None
            }
        }
    }

    /// For test only.
    #[allow(dead_code)]
    fn get_buf(&mut self, idx: usize) -> &LockFreeSet<String> {
        &self.buf.get(idx).unwrap().val
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        debug_assert!(self.inuse.is_empty());
    }
}

#[cfg(test)]
mod background_unit_test {
    use crate::engines::kv::bufring::BufNodePtr;

    use super::{BufRing, create_buf_ring, BufNode};
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::Ordering::{Relaxed, AcqRel};
    use std::sync::atomic::AtomicPtr;

    /// test basic
    #[test]
    fn basic_test() {
        let mut ring = BufRing::new();

        let ptr_1 = ring.alloc_buf().unwrap();
        let ptr_2 = ring.alloc_buf().unwrap();
        let buf_1_idx =  ptr_1.as_ref().id;
        let buf_2_idx = ptr_2.as_ref().id;

        ptr_1.as_ref().val.insert("key-1".to_owned()).unwrap();
        ptr_2.as_ref().val.insert("key-2".to_owned()).unwrap();

        let mut buf1 = ring.get_buf(buf_1_idx).into_iter().collect::<Vec<_>>();
        assert_eq!(*buf1.pop().unwrap(), "key-1".to_owned());
        drop(buf1);

        let mut buf2 = ring.get_buf(buf_2_idx).into_iter().collect::<Vec<_>>();
        assert_eq!(*buf2.pop().unwrap(), "key-2".to_owned());
        drop(buf2);

        ring.release_buf(ptr_1);
        ring.release_buf(ptr_2);

        // assert_eq!(ring.get_buf(buf_1_idx)
        assert_eq!(ring.get_buf(buf_1_idx).iter().count(), 0);
        assert_eq!(ring.get_buf(buf_2_idx).iter().count(), 0);
    }


    /// reuse buf
    #[test]
    fn basic_test_2() {
        let mut ring = BufRing::new();

        let mut v = Vec::new();
        for _ in 0..BufRing::_CAP {
            let bufptr = ring.alloc_buf().unwrap();
            let s = bufptr.as_ref().as_inner();
            s.insert(format!("key-0")).unwrap();
            v.push(bufptr);
        }

        assert_eq!(ring.alloc_buf(), None);
        let ptr_1 = v.pop().unwrap();
        let ptr_1_raw = ptr_1.raw();
        ring.release_buf(ptr_1);
    
        let ptr_saved = ring.alloc_buf().unwrap();
        assert_eq!(ptr_1_raw, ptr_saved.raw());
        let s_saved = &ptr_saved.as_ref().val;
        assert_eq!(s_saved.iter().count(), 0);
        s_saved.insert(format!("key-0")).unwrap();
        s_saved.insert(format!("key-1")).unwrap();
        
        for ptr in v {
            ring.release_buf(ptr);
        }

        let mut v = Vec::new();
        for _ in 0..BufRing::_CAP - 1 {
            let bufptr = ring.alloc_buf().unwrap();
            let s = bufptr.as_ref().as_inner();
            assert_eq!(0, s.iter().count());
            s.insert(format!("key-0")).unwrap();
            v.push(bufptr);
        }

        assert_eq!(ring.alloc_buf(), None);
        s_saved.insert(format!("key-2")).unwrap();

        for ptr in v {
            {
                let s = ptr.as_ref().as_inner();
                let mut keys = s.into_iter().collect::<Vec<_>>();
                assert_eq!(keys.len(), 1);
                assert_eq!(*keys.pop().unwrap(), format!("key-0"));
            }
            ring.release_buf(ptr);
        }

        let mut keys = s_saved.into_iter().collect::<Vec<_>>();
        keys.sort_by(|a, b| {
            a.as_ref().cmp(b.as_ref())
        });
        assert_eq!(keys.len(), 3);
        for (i, key) in keys.into_iter().enumerate() {
            assert_eq!(format!("key-{}", i), *key);
        }
        ring.release_buf(ptr_saved);
    }

    #[test]
    fn simple_use_case() {
        let mut ring = BufRing::new();
        let node_ptr_1 = ring.alloc_buf().unwrap();
        let node_1_idx = node_ptr_1.as_ref().id;

        let current: AtomicPtr<BufNode> = AtomicPtr::from(node_ptr_1.raw());

        let s = unsafe { 
            current.load(Relaxed)
            .as_ref().unwrap().as_inner()
        };
        s.insert(format!("key-1")).unwrap();
        s.insert(format!("key-2")).unwrap();
        drop(s);

        let node_ptr_2 = ring.alloc_buf().unwrap();
        let old = current.swap(node_ptr_2.raw(), AcqRel);
        ring.release_buf(BufNodePtr::from(old));
        drop(old);

        let s = ring.get_buf(node_1_idx);
        assert_eq!(s.into_iter().count(), 0);

        let s2 = unsafe { 
            current.load(Relaxed)
            .as_ref()
            .unwrap()
            .as_inner()
        };
        s2.insert(format!("key-3")).unwrap();
        assert_eq!(s2.into_iter().count(), 1);

        ring.release_buf(BufNodePtr::from(current.load(Relaxed)));
    }

    #[test]
    fn concurrent_use_case() {
        use std::sync::mpsc::{SyncSender, Receiver};

        let ring = Arc::new(create_buf_ring());
        let current = Arc::new(
            AtomicPtr::new(
                ring.lock().unwrap()
                .alloc_buf()
                .unwrap()
                .raw()
        ));

        let (tx, rx) = std::sync::mpsc::sync_channel(10);

        let producer = |tx: SyncSender<_>, ring: Arc<Mutex<BufRing>>, current: Arc<AtomicPtr<BufNode>>| {
            for _ in 0..100 {
                let s = unsafe {
                    current.load(Relaxed).as_ref().unwrap().as_inner()
                };

                for i in 0..1000 {
                    s.insert(format!("key-{}", i)).unwrap();
                }

                loop {
                    let alloc_res = ring.lock().unwrap().alloc_buf();
                    match alloc_res {
                        Some(new_ptr) => {
                            let old = current.swap(new_ptr.raw(), AcqRel);
                            tx.send(AtomicPtr::new(old)).unwrap();
                            break;
                        }

                        None => {
                            println!("waiting...");
                            std::thread::sleep(std::time::Duration::from_millis(40));
                        }
                    }
                }
            }

            let old = current.load(Relaxed);
            tx.send(AtomicPtr::new(old)).unwrap();
        };

        let cleaner = |rx: Receiver<AtomicPtr<BufNode>>, ring: Arc<Mutex<BufRing>>| {
            for _ in 0..100 {
                let old = rx.recv().unwrap();
                // pretend we are cleaning...
                std::thread::sleep(std::time::Duration::from_millis(50));

                let s = unsafe { old.load(Relaxed).as_ref().unwrap().as_inner() };
                let v = s.into_iter().collect::<Vec<_>>();
                assert_eq!(v.len(), 1000);
                ring.lock().unwrap().release_buf(BufNodePtr::from(old.load(Relaxed)));
                println!("clean!");
            }

            let old = rx.recv().unwrap();
            ring.lock().unwrap().release_buf(BufNodePtr::from(old.load(Relaxed)));
        };

        let ring2 = ring.clone();
        let ring3 = ring.clone();
        let h1 = std::thread::spawn(move || producer(tx, ring2, current));
        let h2 = std::thread::spawn(move || cleaner(rx, ring3));
        
        h1.join().unwrap();
        h2.join().unwrap();
    }
}
