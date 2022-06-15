mod rayon;
mod shared_queue;
mod naive;

use crate::Result;

pub trait ThreadPool: Clone + Send + Sync + 'static {
    fn new(nthread: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: Send + 'static + FnOnce();    
}

pub use self::rayon::RayonThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;
pub use self::naive::NaiveThreadPool;
