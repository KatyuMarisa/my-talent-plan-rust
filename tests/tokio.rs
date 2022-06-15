#[cfg(test)]
mod tokio_learn_test {
    async fn sleep_for_a_while(d: u64) -> u64 {
        tokio::time::sleep(tokio::time::Duration::from_secs(d)).await;
        d
    }
    
    #[test]
    fn hello_world_test() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        
        // only the second branch is triggered and the remaining branches is canceled.
        rt.block_on(async {
            tokio::select! {
                _ = sleep_for_a_while(5) => { println!("sleep for 5 sec!"); },
                _ = sleep_for_a_while(3) => { println!("sleep for 3 sec!"); } 
            }
        });

        std::thread::sleep(std::time::Duration::from_secs(6));
        println!("end!");        
    }
}