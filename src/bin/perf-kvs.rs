use kvs::{KvStore, KvsEngine, thread_pool::*};

use crossbeam::sync::WaitGroup;
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
    let mut data = Vec::new();
    data.reserve(n);

    for _ in 0..n {
        let len: usize = rng.gen_range(1..data_max_len);
        let pair = (random_string(&mut rng, len), random_string(&mut rng, len));
        data.push(pair);        
    }
    data
}

fn with_engine<E: KvsEngine>(engine: E, nthread: usize, data: &Vec<(String, String)>) {
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

fn perf_short_keyvalue() {
    let root_dir = tempfile::tempdir().unwrap();
    let data = gen_data(200 * 10000, 50);
    with_engine(
        KvStore::new(root_dir.path()).unwrap(),
        8, &data);
}

fn perf_long_keyvalue() {
    let root_dir = tempfile::tempdir().unwrap();
    let data = gen_data(5 * 10000, 10000);
    with_engine(
        KvStore::new(root_dir.path()).unwrap(),
        10, &data
    );
}

fn main() {
    perf_short_keyvalue();
}
