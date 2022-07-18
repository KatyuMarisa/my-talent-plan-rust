use std::collections::{HashSet};
use std::fs::create_dir_all;
use std::path::PathBuf;

use crate::Result;
use crate::errors::KvError;

use super::disk::{
    ManifestFile, Storage, MmapFile, ManifestRecord, FILE_SIZE_LIMIT, Pos, Readable, Appendable,
};

pub struct Manifest {
    next_fid: FileId,
    manifest_file: ManifestFile,
    files: HashSet<FileId>,
    root: PathBuf,
}

impl Manifest {
    pub fn open(root_dir: impl Into<PathBuf>) -> Result<ManifestAllFiles> {
        let mut pb: PathBuf = root_dir.into();
        pb.push("MANIFEST");
        let manifest_file = ManifestFile::open(
            Box::new(MmapFile::open(&pb)?)
        )?;
        pb.pop();
        let mut manifest = Manifest {
            next_fid: 1,
            manifest_file,
            files: HashSet::new(),
            root: pb,
        };

        let files = manifest.init()?;

        Ok((manifest, files))
    }

    pub fn new(root_dir: impl Into<PathBuf>) -> Result<ManifestAllFiles> {
        let mut pb: PathBuf = root_dir.into();
        create_dir_all(&pb)?;
        pb.push("MANIFEST");
        let manifest_file = ManifestFile::init(
            Box::new(MmapFile::new(&pb)?)
        )?;
        pb.pop();
        let mut manifest = Manifest {
            next_fid: 1,
            manifest_file,
            files: HashSet::new(),
            root: pb,
        };

        let files = manifest.init()?;

        Ok((manifest, files))
    }

    pub fn create_file<FileType: Storage>(&mut self) -> Result<(FileId, Box<FileType>)> {
        let name = self.filename(self.next_fid);
        let fid = self.next_fid;

        // let storage = Box::new(
        //     FileType::new(PathBuf::from(name))?
        // );
        // self.files.insert(fid);
        // self.next_fid += 1;
        // Ok((fid, storage))
        match FileType::new(PathBuf::from(&name)) {
            Err(err) => {
                println!("create file at {} error: {}", name, err.to_string());
                println!("does path exist? {}", self.root.as_path().exists());
                println!("next_fid: {}, fids: {:?}", self.next_fid, self.all_fids());
                return Err(err)
            }
            Ok(storage) => {
                let storage = Box::new(storage);
                self.files.insert(fid);
                self.next_fid += 1;
                Ok((fid, storage))
            }
        }

        // let storage = Box::new(
        //     FileType::new(PathBuf::from(name))?
        // );
        // self.files.insert(fid);
        // self.next_fid += 1;        
        // Ok((fid, storage))
    }

    pub fn open_file<FileType: Storage>(&mut self, fid: FileId) -> Result<Box<FileType>> {
        assert!(self.files.contains(&fid));
        let pb = PathBuf::from(self.filename(fid));
        Ok(Box::new(FileType::open(&pb)?))
    }

    #[allow(dead_code)]
    pub fn remove_file(&mut self, fid: FileId) -> Result<()> {
        self.atomic_add_remove(vec![], vec![fid])
    }

    #[allow(dead_code)]
    pub fn validate_file(&mut self, fid: FileId) -> Result<()> {
        self.atomic_add_remove(vec![fid], vec![])
    }

    pub fn atomic_add_remove(&mut self, added: Vec<FileId>, removed: Vec<FileId>) -> Result<()> {
        // all allocated fids should be tracked...
        assert!(added.iter().all(|fid| self.files.contains(fid)));
        assert!(removed.iter().all(|fid| self.files.contains(fid)));

        self.manifest_file.append_record(
            &ManifestRecord {
                added,
                removed: removed.clone()
            } 
        )?;

        for fid in removed {
            self.files.remove(&fid);
            self.remove_file_by_fid(fid).unwrap_or_default();
        }

        self.manifest_file.sync()
    }

    pub fn remove_file_by_fid(&mut self, fid: FileId) -> Result<()> {
        if let Err(err) = std::fs::remove_file(PathBuf::from(self.filename(fid))) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(KvError::from(err).into())
            }
        }
        Ok(())
    }

    pub fn all_fids(&self) -> Vec<FileId> {
        return self.files.iter().copied().collect();
    }

    #[allow(dead_code)]
    pub fn disk_usage(&self) -> usize {
        self.files.len() * FILE_SIZE_LIMIT
    }

    fn init(&mut self) -> Result<Vec<(FileId, Box<dyn Storage>)>> {
        let (mut valid_fids, mut invalid_fids) = (HashSet::<FileId>::new(), HashSet::<FileId>::new());

        let mut max_fid = 0;

        for (_, record) in self.manifest_file.all_records()? {
            for fid in record.added {
                max_fid = std::cmp::max(max_fid, fid);
                valid_fids.insert(fid);
            }
            for fid in record.removed {
                max_fid = std::cmp::max(max_fid, fid);
                invalid_fids.insert(fid);
            }
        }        
        for fid in invalid_fids.iter() {
            self.remove_file_by_fid(*fid)?;
            valid_fids.remove(fid);
        }

        let mut files: Vec<(FileId, Box<dyn Storage>)> = Vec::new();
        for fid in valid_fids.iter() {
            files.push((*fid,
                Box::new(MmapFile::open(self.filename(*fid))?
            )))
        }

        self.next_fid = max_fid + 1;
        self.files = valid_fids;
        Ok(files)
    }

    fn filename(&self, fid: FileId) -> String {
        format!("{}/DBFile00{}.dbf", self.root.to_str().unwrap(), fid)
    }
}


type ManifestAllFiles = (Manifest, Vec<(FileId, Box<dyn Storage>)>);
pub type FileId = u32;
pub type Rid = (FileId, Pos);
