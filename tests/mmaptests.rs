#[cfg(test)]
mod mmap_learn_tests {
    use std::{io::{Read, Write}, fs::{OpenOptions, remove_file}, path::PathBuf};
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

    // #[test]
    // fn test_unknown() -> std::io::Result<()> {
    //     let mut kv: HashMap<i32, i32> = vec![(1,1), (2,2), (3,3)].into_iter().collect();
    //     let v: Vec<&i32> = kv.keys().into_iter().collect();
    //     println!("{}", kv.len());
    //     drop(kv);
    //     println!("{}", v.len());
    //     Ok(())
    // }
}