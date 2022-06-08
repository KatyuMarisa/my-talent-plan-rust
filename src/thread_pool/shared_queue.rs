use std::sync::{Condvar, Mutex, Arc};
use std::thread::JoinHandle;

use crossbeam::channel::{bounded, Sender, Receiver};
use crossbeam::queue::ArrayQueue as LockFreeQueue;

use crate::Result;
use super::ThreadPool;

type TaskType = Box<dyn Send + 'static + FnOnce()>;

enum QueueMessageType {
    TASK(TaskType),
    SHUTDOWN,
}

pub struct SharedQueueThreadPool {
    nthread: usize,
    sender: Sender<QueueMessageType>,
}

struct TaskGuard {
    receiver: Receiver<QueueMessageType>,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let receiver = self.receiver.clone();
            std::thread::spawn(|| {
                handle_task(TaskGuard {
                    receiver
                })
            });
        }
    }
}

fn handle_task(task_guard: TaskGuard) {
    loop {
        if let Ok(msg) = task_guard.receiver.recv() {
            match msg {
                QueueMessageType::TASK(task) => {
                    task();
                }

                QueueMessageType::SHUTDOWN => {
                    break;
                }
            }
        } else {
            unreachable!("this should not exit!")
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(nthread: u32) -> Result<Self>
    where
        Self: Sized {
        let nthread = nthread as usize;

        let (sender, receiver) = bounded::<QueueMessageType>(nthread * 2);
        for _ in 0..nthread {
            let task_guard = TaskGuard {
                receiver: receiver.clone(),
            };
            std::thread::spawn(|| handle_task(task_guard));
        }

        Ok(Self {
            nthread,
            sender,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: Send + 'static + FnOnce() {
        self.sender.send(QueueMessageType::TASK(Box::new(job))).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.nthread {
            self.sender.send(QueueMessageType::SHUTDOWN).unwrap();
        }
    }
}
