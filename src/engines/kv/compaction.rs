use std::{sync::{Arc, Mutex, Barrier, Condvar}, collections::LinkedList, iter::Zip};

use lockfree::map::ReadGuard;

use crate::Result;
use super::{
    bufring::{BufRing, create_buf_ring, BufNodePtr},
    inner::KvStoreInner,
    memtable::{ShouldFlushOrCompact, MemtableValue},
    manifest::Rid,
    writebatch::WriteBatch,
    disk::{KVRecord, KVRecordKind, FILE_SIZE_LIMIT},
};

enum FlushCompactionTask {
    Flush(BufNodePtr),
    Compaction,
}

pub struct Compactor {
    inner: Arc<KvStoreInner>,
    // buffer ring
    ring: Arc<Mutex<BufRing>>,
    // pending flush/compaction task
    // statistic: Arc<Mutex<CompactionStatistic>>,

    pending_task: Arc<Mutex<LinkedList<FlushCompactionTask>>>,
    running: Mutex<bool>,
    // shared between `Memtable` and `Compactor`
    monitor_cond: Arc<Condvar>,
    // shared between monitor thread and compaction thread
    flush_cond: Arc<Condvar>,
    // shutdown waitgroup
    shutdown: Arc<Barrier>,
}

impl Compactor {
    pub const INIT_COMPACT_LIMIT: usize = 10 * FILE_SIZE_LIMIT;

    pub fn new(inner: Arc<KvStoreInner>,
        cond: Arc<Condvar>,
    ) -> Self {
        Self {
            inner,
            monitor_cond: cond,
            flush_cond: Arc::new(Condvar::new()),
            running: Mutex::new(true),
            ring: Arc::new(create_buf_ring()),
            pending_task: Arc::new(Mutex::new(LinkedList::new())),
            shutdown: Arc::new(Barrier::new(3)),
        }
    }

    /// Sechedule a background compaction task.
    /// A waiting compaction task will discard all waiting flush task, because compaction task will do anything that
    /// flush should do.
    fn schedule_compaction(&self) {
        let old = self.plugin_buf(true);
        let mut ring = self.ring.lock().unwrap();
        ring.release_buf(old);

        let mut pending_flush_tasks = self.pending_task.lock().unwrap();
        for task in pending_flush_tasks.iter() {
            match task {
                FlushCompactionTask::Flush(old) => {
                    ring.release_buf(*old);
                },
                FlushCompactionTask::Compaction => {},
            }
        }
        pending_flush_tasks.clear();
        pending_flush_tasks.push_back(FlushCompactionTask::Compaction);
        self.monitor_cond.notify_all();
    }

    /// Schedule a background flush task.
    fn schedule_flush(&self) {
        let old = self.plugin_buf(false);
        self.pending_task
            .lock().unwrap()
            .push_back(FlushCompactionTask::Flush(old));
    }

    /// Allocate a new buf from ring, and swap out the old buf in memtable.
    fn plugin_buf(&self, is_compaction: bool) -> BufNodePtr {
        let new_buf_ptr = loop {
            let buf = self.ring.lock().unwrap().alloc_buf();
            match buf {
                Some(buf_ptr) => break buf_ptr,
                None => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
        };

        let pause_write = self.inner.memtable.pause_for_swap_buf();
        let old = self.inner.memtable.swap_buf(new_buf_ptr, is_compaction);
        self.inner.memtable.finish(pause_write);
        old
    }
}

impl Compactor {
    pub fn monitor_loop(&self) {
        loop {
            {
                let running = self.running.lock().unwrap();
                if *running == false {
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

            {
                let running = self.running.lock().unwrap();
                let _ = self.monitor_cond.wait(running).unwrap();
            }
        }

        // exit whit drop thread
        self.shutdown.wait();
    }

    pub fn flush_compaction_loop(&self) {
        loop {
            {
                let running = self.running.lock().unwrap();
                if *running == false {
                    break;
                }
            }

            let task = {
                let mut pending_task = self.pending_task.lock().unwrap();
                pending_task.pop_front()
            };

            match task {
                None => continue,

                Some(FlushCompactionTask::Flush(old)) => {
                    self.do_flush(old).expect("flush failed");
                }

                Some(FlushCompactionTask::Compaction) => {
                    self.do_compaction().expect("compaction failed");
                }
            }

            {
                let pending_task = self.pending_task.lock().unwrap();
                let _ = self.flush_cond.wait(pending_task).unwrap();
            }
        }
        // exit with drop thread
        self.shutdown.wait();
    }

    fn stop(&self) {
        *self.running.lock().unwrap() = false;
    }
}

impl Compactor {
    fn do_flush(&self, old: BufNodePtr) -> Result<()> {
        let mut statistic = Statistic::new("flush");
        let start = std::time::Instant::now();
        let uncompacted_keys = old.as_ref().as_inner();
        let mut uncompacted_keys = uncompacted_keys.iter().map(|e| {
            self.inner.memtable.raw_get(e.as_ref())
        }).collect::<Vec<_>>();
        let rids = self.sink(&mut uncompacted_keys, false, &mut statistic)?;
        self.update_inner_fids(uncompacted_keys.into_iter().zip(rids.into_iter()), false, &mut statistic);
        statistic.timecost_total = start.elapsed().as_millis();
        println!("{:?}", statistic);
        Ok(())
    }

    fn do_compaction(&self) -> Result<()> {
        let mut statistic = Statistic::new("compaction");
        let start = std::time::Instant::now();
        let iter = self.inner.memtable.iter();
        let mut uncompacted_keys = iter.collect::<Vec<_>>();        
        let rids = self.sink(&mut uncompacted_keys, true, &mut statistic)?;
        self.update_inner_fids(uncompacted_keys.into_iter().zip(rids.into_iter()), true, &mut statistic);
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
        is_compaction: bool,
        statistic: &mut Statistic)
    {
        // pause all incoming write requests to avoid vid conflict.
        let start = std::time::Instant::now();
        let guard = {
            if is_compaction {
                self.inner.memtable.pause_for_reset_flush_rids()
            } else {
                self.inner.memtable.pause_for_reset_compaction_rids()
            }
        };

        let vid = self.inner.memtable.advance_version();

        for (key_value, rid) in iter.into_iter() {
            match key_value.val() {
                MemtableValue::Rid((vid2, _)) => {
                    debug_assert!(*vid2 >= vid);
                    // never worry about vid conflict because we have paused all write requests.
                    if *vid2 == vid {
                        match rid {
                            None => self.inner.memtable.raw_set(key_value.key(), MemtableValue::Value((vid, None))),
                            Some(rid) => self.inner.memtable.raw_set(key_value.key(), MemtableValue::Rid((vid, rid))),
                        }
                    }
                }

                MemtableValue::Value((vid2, _)) => {
                    debug_assert!(*vid2 > vid);
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

impl Drop for Compactor {
    fn drop(&mut self) {
        // TODO: drain pending tasks.
        self.stop();
        self.shutdown.wait();
        // wait for two background thread exit.
        std::thread::sleep(std::time::Duration::from_millis(1));
        // force flush memtable to sstable.
        self.inner.memtable.prepare_force_flush();
        let old = self.plugin_buf(false);
        self.do_flush(old).expect("force flush error");
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