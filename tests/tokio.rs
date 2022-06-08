use tokio;

#[cfg(test)]
mod tokio_learn_test {
    #[test]
    fn hello_world_test() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
    }
}