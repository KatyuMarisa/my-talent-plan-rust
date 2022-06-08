use super::ThreadPool;


pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(nthreads: u32) -> crate::Result<Self>
    where 
        Self: Sized {
        Ok(
            RayonThreadPool {
                pool: rayon::ThreadPoolBuilder::new()
                .num_threads(nthreads as usize)
                .build().unwrap()
            }
        )
    }

    fn spawn<F>(&self, job: F)
    where
        F: Send + FnOnce() + 'static {
        self.pool.spawn(job)
    }
}
