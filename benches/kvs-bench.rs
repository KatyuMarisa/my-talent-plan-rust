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
            // We must explicitly drop engine2 here. Otherwise, the engine2 may be dropped after wg2. When all 
            // WaitGroup's reference is dropped, the function bench_with_engine is returned and the temporary
            // directory created by this bench maybe already removed. After that, the last KvStore's handle 
            // hold by thread in threadpool is dropped and KvStore's destructor is called, then it will panic
            // because it finds that the directory is already removed(In current version, program will panic at
            // kvs::engines::writebatch::WriteBatch::new).
            drop(engine2);
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
        for _ in 0..nthread {
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

fn sync_storage_bench_with_params(c: &mut Criterion, name: String, scales: Vec<usize>,
    nthread: usize, data_max_len: usize) {

    let mut group = c.benchmark_group(name);
    group.sample_size(20);

    for scale in scales {
        let data = gen_data(scale, data_max_len);

        group.bench_with_input(BenchmarkId::new("kvs", scale), &data,
        |b, dt| b.iter(|| {
            let root_dir = tempfile::tempdir().unwrap();
            let store = KvStore::new(root_dir.path()).unwrap();
            bench_with_engine(store, nthread, &dt);
            root_dir.close().unwrap();
        }));

        group.bench_with_input(BenchmarkId::new("sled", scale), &data,
        |b, dt| b.iter(|| {
            let root_dir = tempfile::tempdir().unwrap();
            let store = SledKvStore::new(sled::open(root_dir.path()).unwrap());
            bench_with_engine(store, nthread, &dt);
            root_dir.close().unwrap();
        }));
    }
}


fn sync_storage_random_short_length_bench(c: &mut Criterion) {
    const W: usize = 10000;
    sync_storage_bench_with_params(c, 
        "random_short_value_bench".to_owned(), 
        vec![10*W, 20*W, 50*W, 80*W, 120*W], 
        8,
        50,
    );
}

fn sync_storage_random_long_length_bench(c: &mut Criterion) {
    const W: usize = 10000;
    sync_storage_bench_with_params(c,
        "random_long_value_bench".to_owned(),
        vec![1*W, 2*W, 3*W, 4*W, 5*W],
        8,
        10000
    );
}

criterion_group!(benches,
    sync_storage_random_short_length_bench,
    // sync_storage_random_long_length_bench    // very very slow for sled....
);
criterion_main!(benches);
