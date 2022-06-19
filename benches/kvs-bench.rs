use crossbeam::sync::WaitGroup;
use futures::future::join_all;
use kvs::{KvStore, KvsEngine, SledKvStore, thread_pool::ThreadPool, AsyncKvsEngine};

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use rand::{Rng, prelude::ThreadRng};

const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789)(*&^%$#@!~";

fn random_string(rng: &mut ThreadRng, len: usize) -> String {
    (0..len)
            .map(|_| {
                let idx = rng.gen_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect()
}

fn gen_data(n: usize, data_max_len: usize) -> Vec<(String, String)> {
    let mut rng = rand::thread_rng();
    let mut datas = Vec::new();

    for _ in 0..n {
        let len: usize = rng.gen_range(1..data_max_len);
        let pair = (random_string(&mut rng, len), random_string(&mut rng, len));
        datas.push(pair);        
    }
    datas
}

fn bench_with_engine<E: KvsEngine>(engine: E, nthread: usize, data: &Vec<(String, String)>) {
    let pool = kvs::thread_pool::RayonThreadPool::new(nthread as u32).unwrap();

    let wg = WaitGroup::new();
    for chunk in data.chunks(data.len() / nthread) {
        let chunk = chunk.to_owned();
        let engine2 = engine.clone();
        let wg2 = wg.clone();
        pool.spawn(move || {
            for (key, value) in chunk {
                engine2.set(key.to_owned(), value.to_owned()).unwrap();
            }
            drop(wg2);
        });
    }
    wg.wait();
}

fn bench_with_async_engine<E: AsyncKvsEngine>(engine: E, nthread: usize, data: &Vec<(String, String)>) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(nthread)
        .build()
        .unwrap();
    
    let mut futures = Vec::new();
    for chunk in data.chunks(data.len() / nthread) {
        for i in 0..nthread {
            let engine2 = engine.clone();
            let chunk = chunk.to_owned();
            let fu = async move {
                for (key, value) in chunk {
                    engine2.async_set(key, value).await.unwrap();
                }
            };
            futures.push(fu);
        }
    }

    rt.block_on(async {
        join_all(futures).await;
    });
}

fn store_scale_long_data_bench(c: &mut Criterion) {
    const W: usize = 10000;
    let nthread = 10;
    let mut group = c.benchmark_group("scale_bench");
    group.sample_size(20);

    for scale in [
            1*W, 
            2*W, 
            3*W, 
            4*W, 
            5*W
        ] {
        let data = gen_data(scale, 10000);

        group.bench_with_input(BenchmarkId::new("kvs", scale), &data,
        |b, dt| b.iter(|| {
            let root_dir = tempfile::tempdir().unwrap();
            let store = KvStore::new(root_dir.path()).unwrap();
            bench_with_engine(store, nthread, &dt);
        }));

        group.bench_with_input(BenchmarkId::new("sled", scale), &data,
        |b, dt| b.iter(|| {
            let root_dir = tempfile::tempdir().unwrap();
            let store = SledKvStore::new(sled::open(root_dir.path()).unwrap());
            bench_with_engine(store, nthread, &dt);
        }));
    }
    group.finish();
}

criterion_group!(benches, store_scale_long_data_bench);
criterion_main!(benches);
