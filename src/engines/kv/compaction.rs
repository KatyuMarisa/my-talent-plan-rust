use std::{
    sync::{Arc, Mutex, Barrier, Condvar, 
        atomic::{Ordering::Relaxed, AtomicBool}
    }, 
    collections::LinkedList, iter::Zip
};

use lockfree::map::ReadGuard;

use crate::Result;
use super::{
    bufring::{BufRing, BufNodePtr},
    inner::KvStoreInner,
    memtable::{ShouldFlushOrCompact, MemtableValue},
    manifest::Rid,
    writebatch::WriteBatch,
    disk::{KVRecord, KVRecordKind, FILE_SIZE_LIMIT},
};

enum FlushCompactionTask {
    Flush((BufNodePtr, usize)),
    Compaction(usize),
}

pub struct Compactor {
    inner: Arc<KvStoreInner>,
    // buffer ring
    ring: Arc<Mutex<BufRing>>,
    // pending flush/compaction task
    pending_task: Arc<Mutex<LinkedList<FlushCompactionTask>>>,
    // running mark
    running: AtomicBool,
    // shared between `Memtable` and `Compactor`
    monitor_cond: Arc<Condvar>,
    // shared between monitor thread and compaction thread
    flush_cond: Arc<Condvar>,
    // shutdown waitgroup
    shutdown: Arc<Barrier>,
}

impl Compactor {
    pub const INIT_COMPACT_LIMIT: usize = 10 * FILE_SIZE_LIMIT;

    pub fn new(inner: Arc<KvStoreInner>, cond: Arc<Condvar>, ring: Arc<Mutex<BufRing>>) -> Self {
        Self {
            inner,
            monitor_cond: cond,
            flush_cond: Arc::new(Condvar::new()),
            running: AtomicBool::new(true),
            ring,
            pending_task: Arc::new(Mutex::new(LinkedList::new())),
            shutdown: Arc::new(Barrier::new(3)),
        }
    }

    /// Sechedule a background compaction task.
    /// A waiting compaction task will discard all waiting flush task, because compaction task will do anything that
    /// flush should do.
    fn schedule_compaction(&self) {
        let (old, vid) = self.update_buf(None, true);
        let mut ring = self.ring.lock().unwrap();
        ring.release_buf(old);

        let mut pending_flush_tasks = self.pending_task.lock().unwrap();
        for task in pending_flush_tasks.iter() {
            match task {
                FlushCompactionTask::Flush((old, _)) => {
                    ring.release_buf(*old);
                },
                FlushCompactionTask::Compaction(_) => {},
            }
        }
        pending_flush_tasks.clear();
        pending_flush_tasks.push_back(FlushCompactionTask::Compaction(vid));
        self.flush_cond.notify_all();
    }

    /// Schedule a background flush task.
    fn schedule_flush(&self) {
        let old = self.update_buf(None, false);
        self.pending_task
            .lock().unwrap()
            .push_back(FlushCompactionTask::Flush(old));
        self.flush_cond.notify_all();
    }

    /// Allocate a new buf from ring, and swap out the old buf in memtable.
    fn update_buf(&self, default: Option<BufNodePtr>, is_compaction: bool) -> (BufNodePtr, usize) {
        println!("update buf!");
        let new_buf_ptr = if let Some(buf_ptr) = default {
            buf_ptr
        } else {
            loop {
                let buf = self.ring.lock().unwrap().alloc_buf();
                match buf {
                    Some(buf_ptr) => break buf_ptr,
                    None => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                }
            }
        };

        let pause_write = self.inner.memtable.pause_for_swap_buf();
        let old = self.inner.memtable.swap_buf(new_buf_ptr, is_compaction);
        let vid = self.inner.memtable.advance_version();
        self.inner.memtable.finish(pause_write);
        (old, vid)
    }
}

impl Compactor {
    pub fn monitor_loop(&self) {
        let unused = Mutex::new(false);

        loop {
            {
                let running = self.running.load(Relaxed);
                if !running {
                    break;
                }
            }

            match self.inner.memtable.should_flush_or_compact() {
                ShouldFlushOrCompact::No => { },

                ShouldFlushOrCompact::Flush => {
                    self.schedule_flush();
                }

                ShouldFlushOrCompact::Compact => {
                    self.schedule_compaction();
                }
            }

            let _ = self.monitor_cond.wait_timeout(
                unused.lock().unwrap(),
                std::time::Duration::from_millis(50))
            .unwrap();
        }

        // exit whit drop thread
        self.shutdown.wait();
    }

    pub fn flush_compaction_loop(&self) {
        loop {
            {
                let running = self.running.load(Relaxed);
                if !running {
                    break;
                }
            }

            loop {
                let task = self.pending_task.lock().unwrap().pop_front();
                match task {
                    None => { break; },

                    Some(FlushCompactionTask::Flush((old, vid))) => {
                        self.do_flush(old, vid).expect("flush failed");
                    }

                    Some(FlushCompactionTask::Compaction(vid)) => {
                        self.do_compaction(vid).expect("compaction failed");
                    }
                }
            }

            {
                let pending_task = self.pending_task.lock().unwrap();
                let _ = self.flush_cond.wait_timeout(
                    pending_task,
                    std::time::Duration::from_millis(50),
                ).unwrap();
            }
        }

        // drain
        while let Some(task) = self.pending_task.lock().unwrap().pop_back() {
            match task {
                FlushCompactionTask::Flush((old, vid)) => {
                    self.do_flush(old, vid).expect("flush failed");
                }

                _ => {
                    continue;
                }
            }
        }

        // exit with drop thread
        self.shutdown.wait();
    }

    fn stop(&self) {
        self.running.store(false, Relaxed);
        self.monitor_cond.notify_all();
        self.flush_cond.notify_all();
    }
}

impl Compactor {
    fn do_flush(&self, old: BufNodePtr, vid: usize) -> Result<()> {
        let mut statistic = Statistic::new("flush");
        let start = std::time::Instant::now();
        let uncompacted_keys = old.as_ref().as_inner();
        let mut uncompacted_keys = uncompacted_keys.iter().map(|e| {
            self.inner.memtable.raw_get(e.as_ref())
        }).collect::<Vec<_>>();
        let rids = self.sink(&mut uncompacted_keys, false, &mut statistic)?;
        self.update_inner_fids(uncompacted_keys.into_iter().zip(rids.into_iter()), vid, false, &mut statistic);
        statistic.timecost_total = start.elapsed().as_millis();
        self.ring.lock().unwrap().release_buf(old);
        println!("{:?}", statistic);
        Ok(())
    }

    fn do_compaction(&self, vid: usize) -> Result<()> {
        let mut statistic = Statistic::new("compaction");
        let start = std::time::Instant::now();
        let iter = self.inner.memtable.iter();
        let mut uncompacted_keys = iter.collect::<Vec<_>>();        
        let rids = self.sink(&mut uncompacted_keys, true, &mut statistic)?;
        self.update_inner_fids(uncompacted_keys.into_iter().zip(rids.into_iter()), vid, true, &mut statistic);
        statistic.timecost_total = start.elapsed().as_millis();
        println!("{:?}", statistic);
        Ok(())
    }

    fn sink(&self,
        records: &mut Vec<ReadGuard<String, MemtableValue>>, 
        is_compaction: bool,
        statistic: &mut Statistic
    )
    -> Result<Vec<Option<Rid>>> {
        // sort by file_id and position in memory to make code more cache friendly.
        let start = std::time::Instant::now();
        println!("sink {} records", records.len());

        records.sort_by(|a, b| {
            match (a.val(), b.val()) {
                (MemtableValue::Value((_, aa)), MemtableValue::Value((_, bb))) => {
                    match (aa, bb) {
                        (None, None) => {
                            std::cmp::Ordering::Equal
                        }
                        (None, Some(_)) => {
                            std::cmp::Ordering::Less
                        },
                        (Some(_), None) => {
                            std::cmp::Ordering::Greater
                        }
                        (Some(aa), Some(bb)) => {
                            aa.cmp(bb)
                        },
                    }
                }

                (MemtableValue::Rid(_), MemtableValue::Value(_)) => {
                    std::cmp::Ordering::Greater
                },

                (MemtableValue::Value(_), MemtableValue::Rid(_)) => {
                    std::cmp::Ordering::Less
                },

                (MemtableValue::Rid(aa), MemtableValue::Rid(bb)) => {
                    let (a_fid, (a_offset, _)) = aa;
                    let (b_fid, (b_offset, _)) = bb;
                    if a_fid != b_fid {
                        a_fid.cmp(b_fid)
                    } else {
                        a_offset.cmp(b_offset)
                    }
                }
            }
        });

        let mut rid: Rid;
        let mut rids = Vec::with_capacity(records.len());
        let mut batch = WriteBatch::new(self.inner.manifest.clone())?;

        for record in records.into_iter() {
            match record.val() {
                MemtableValue::Rid((_, (fid, pos))) => {
                    let ff = self.inner.sstables
                        .get(fid)
                        .unwrap();
                    let bytes = ff
                        .val()
                        .raw_read(pos.0, pos.1)?;
                    rid = batch.append_bytes(bytes)?;
                    rids.push(Some(rid));
                }

                MemtableValue::Value((_, Some(val))) => {
                    let record = KVRecord {
                        key: record.key().to_owned(),
                        value: val.to_owned(),
                        kind: KVRecordKind::KV,
                    };

                    rid = batch.append_bytes(
                        &bincode::serialize(&record)?
                    )?;
                    rids.push(Some(rid));
                }

                MemtableValue::Value((_, None)) => {
                    if is_compaction {
                        rids.push(None);
                    } else {
                        let record = KVRecord {
                            key: record.key().to_owned(),
                            value: "".to_owned(),
                            kind: KVRecordKind::Tomb
                        };

                        let rid = batch.append_bytes(
                            &bincode::serialize(&record)?
                        )?;
                        rids.push(Some(rid)); 
                    }
                }
            }
        }

        let disk_usage = batch.disk_usage();
        self.batch_commit(batch)?;
        statistic.num_records = rids.len();
        statistic.batch_disk_usage = disk_usage;
        statistic.timecost_sink = start.elapsed().as_millis();
        statistic.total_disk_usage = self.inner.manifest.lock().unwrap().disk_usage();
        Ok(rids)
    }

    fn update_inner_fids<'a> (
        &'a self,
        iter: Zip <impl Iterator<Item = ReadGuard<'a, String, MemtableValue>>, impl Iterator<Item = Option<Rid>>>,
        vid: usize,
        is_compaction: bool,
        statistic: &mut Statistic)
    {
        // pause all incoming write requests to avoid vid conflict.
        let start = std::time::Instant::now();
        let guard = {
            if is_compaction {
                self.inner.memtable.pause_for_reset_compaction_rids()
            } else {
                self.inner.memtable.pause_for_reset_flush_rids()
            }
        };

        for (key_value, rid) in iter.into_iter() {
            // never worry about vid conflict because we have paused all write requests.
            match key_value.val() {
                MemtableValue::Rid((vid2, _)) => {
                    debug_assert!(vid > *vid2);
                    match rid {
                        None => {
                            self.inner.memtable.raw_set(key_value.key(), MemtableValue::Value((vid, None)));
                        }
                        Some(rid) => {
                            self.inner.memtable.raw_set(key_value.key(), MemtableValue::Rid((vid, rid)));
                        }
                    }
                }

                MemtableValue::Value((vid2, _)) => {
                    debug_assert!(vid >= *vid2);
                    if *vid2 == vid {
                        match rid {
                            None => {
                                self.inner.memtable.raw_set(key_value.key(), MemtableValue::Value((vid, None)));
                            }
                            Some(rid) => {
                                self.inner.memtable.raw_set(key_value.key(), MemtableValue::Rid((vid, rid)));
                            }
                        }
                    }
                }
            }
        }
        // ok!
        statistic.timecost_reset_rid = start.elapsed().as_millis();
        self.inner.memtable.finish(guard);
    }

    fn batch_commit(&self, batch: WriteBatch) -> Result<()> {
        let new_sstables = batch.commit()?;
        self.inner.sstables.extend(new_sstables);
        Ok(())
    }
}

impl Compactor {
    pub fn suicide(&self) {
        self.stop();
        // make sure all threads that holds Arc<Compactor> hard ref join.
        std::thread::sleep(std::time::Duration::from_millis(1));
        self.shutdown.wait();
        // release last buf.
        self.inner.memtable.prepare_force_flush();
        let nullptr = BufNodePtr::from(std::ptr::null_mut());
        let (last_ptr, vid) = self.update_buf(Some(nullptr), false);
        self.do_flush(last_ptr, vid).expect("force flush error");
    }
}

#[derive(Debug)]
pub struct Statistic {
    _kind: &'static str,
    num_records: usize,
    batch_disk_usage: usize,
    total_disk_usage: usize,
    timecost_total: u128,
    timecost_sink: u128,
    timecost_reset_rid: u128,
}

impl Statistic {
    fn new(kind: &'static str) -> Self {
        Statistic {
            _kind: kind,
            num_records: 0,
            batch_disk_usage: 0,
            total_disk_usage: 0,
            timecost_total: 0,
            timecost_sink: 0,
            timecost_reset_rid: 0,
        }
    }
}
