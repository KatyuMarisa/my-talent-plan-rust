#[cfg(test)]
mod mmap_learn_tests {
    use std::{io::{Read, Write}, fs::{OpenOptions, remove_file}, path::PathBuf, hash::Hasher};
    use lockfree::map::Preview;
    use memmap::MmapMut;

    #[test]
    fn simple_mmap_test() -> std::io::Result<()> {
        let path = PathBuf::from("/home/yukari/kvnew/kvs/testfile.dbf");
        remove_file(&path).unwrap_or_default();

        let mut f = OpenOptions::new()
                                 .read(true)
                                 .write(true)
                                 .create(true)
                                 .open(&path)?;
        f.set_len(128)?;        

        let mut mmap = unsafe { MmapMut::map_mut(&f) }?;
        let start = b"Hello".len();
        // why a slice can convert to std::io::Write?
        let mut m = mmap.as_mut();
        m.write_all(b"Hello")?;


        (&mut mmap[..]).write_all(b"Hello")?;
        (&mut mmap[start..=start + b" World".len() + 1]).write_all(b" World")?;
        drop(mmap);
        drop(f);

        f = OpenOptions::new()
                                 .read(true)
                                 .write(true)
                                 .create(true)
                                 .open(&path)?;

        let mut vec = Vec::new();
        f.read_to_end(&mut vec)?;
        let s = String::from_utf8(vec).unwrap();
        println!("content is {}, length is {}", s, s.len());
        Ok(())
    }


    #[test]
    fn lockfree_hashmap_test() {
        let a50 = std::sync::atomic::AtomicUsize::new(50);
        a50.fetch_sub(10, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(a50.load(std::sync::atomic::Ordering::Acquire), 40);
        a50.fetch_sub(10, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(a50.fetch_sub(10, std::sync::atomic::Ordering::Acquire), 30);
    }

    #[test]
    fn lock_free_map_test() {
        use lockfree::map::Map as LockFreeMap;
        #[derive(Eq, Debug)]
        struct KeyGuard {
            ver: usize,
            key: String,
        }

        impl std::hash::Hash for KeyGuard {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.key.hash(state);
            }
        }

        impl Ord for KeyGuard {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.key.cmp(&other.key)
            }
        }

        impl PartialOrd for KeyGuard {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.key.partial_cmp(&other.key)
            }
        }

        impl PartialEq for KeyGuard {
            fn eq(&self, other: &Self) -> bool {
                self.ver == other.ver && self.key == other.key
            }
        }

        let map = LockFreeMap::<KeyGuard, String>::new();
        map.insert( KeyGuard { ver: 1, key: "1".to_owned()}, "1".to_owned());
        map.insert_with( KeyGuard {ver: 2, key: "1".to_owned()},  |k, ov, okv|{
            if let Some((kk, _)) = okv {
                match kk.ver.cmp(&k.ver) {
                    std::cmp::Ordering::Less => {
                        Preview::Keep
                    },
                    std::cmp::Ordering::Equal => {
                        unreachable!("")
                    },
                    std::cmp::Ordering::Greater => {
                        Preview::Discard
                    },
                }
            } else {
                Preview::Keep
            }
        });

        for (k, v) in map {
            println!("{:?} {}", k, v);
        }

    }
}