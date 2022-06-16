use std::sync::Arc;

use crossbeam::channel::{bounded, Sender, Receiver};

use crate::Result;
use super::ThreadPool;

type TaskType = Box<dyn Send + 'static + FnOnce()>;

enum QueueMessageType {
    Task(TaskType),
    ShutDown,
}

#[derive(Clone)]
pub struct SharedQueueThreadPool {
    sender: Arc<SenderWrapper>
}

#[derive(Clone)]
struct TaskGuard {
    tid: usize,
    receiver: Receiver<QueueMessageType>,
}


impl Drop for TaskGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let receiver = self.receiver.clone();
            let tid = self.tid;
            std::thread::spawn(move || {
                handle_task(
                    TaskGuard{ tid, receiver})
            });
        }
    }
}

fn handle_task(task_guard: TaskGuard) {
    loop {
        if let Ok(msg) = task_guard.receiver.recv() {
            match msg {
                QueueMessageType::Task(task) => {
                    task();
                }

                QueueMessageType::ShutDown => {
                    println!("thread {} exit!", task_guard.tid);
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

        let (sender, receiver)
            = bounded::<QueueMessageType>(nthread * 2);
        for tid in 0..nthread {
            let task_guard = TaskGuard {
                tid,
                receiver: receiver.clone(),
            };
            std::thread::spawn(|| handle_task(task_guard));
        }

        Ok(Self {
            sender: Arc::new(SenderWrapper::new(sender, nthread)),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: Send + 'static + FnOnce() {
        self.sender.send(QueueMessageType::Task(Box::new(job)));
    }
}


#[derive(Clone)]
struct SenderWrapper {
    ntask: usize,
    sender: Sender<QueueMessageType>,
}

impl SenderWrapper {
    pub fn new(sender: Sender<QueueMessageType>, ntask: usize) -> Self {
        Self { ntask, sender }
    }

    pub fn send(&self, msg: QueueMessageType) {
        self.sender.send(msg)
            .expect("send task error!");
    }
}

impl Drop for SenderWrapper {
    fn drop(&mut self) {
        for _ in 0..self.ntask {
            self.sender.send(QueueMessageType::ShutDown)
                .expect("send shutdown error!");
        }
    }
}
