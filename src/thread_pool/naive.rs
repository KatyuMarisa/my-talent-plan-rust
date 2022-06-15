use super::ThreadPool;

#[derive(Clone)]
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_nthreads: u32) -> crate::Result<Self>
    where 
        Self: Sized {
            Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: Send + FnOnce() + 'static {
        std::thread::spawn(job);
    }
}
