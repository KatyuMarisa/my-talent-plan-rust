use futures::future::join_all;
use kvs::thread_pool::RayonThreadPool;
use kvs::{AsyncKvStore, AsyncKvsEngine, Result};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use walkdir::WalkDir;

fn get_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(8)
        .build()
        .unwrap()
}


// Should get previously stored value
#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = AsyncKvStore::<RayonThreadPool>::new(temp_dir.path(), 1)?;
    let rt = get_runtime();

    let store2 = store.clone();
    rt.block_on(async move {
        store2.async_set("key1".to_owned(), "value1".to_owned()).await.unwrap();
        store2.async_set("key2".to_owned(), "value2".to_owned()).await.unwrap();
        assert_eq!(
            store2.async_get("key1".to_owned()).await.unwrap(),
            Some("value1".to_owned())
        );
        assert_eq!(
            store2.async_get("key2".to_owned()).await.unwrap(),
            Some("value2".to_owned())
        );
    });

    // Open from disk again and check persistent data
    drop(store);
    let store = AsyncKvStore::<RayonThreadPool>::open(temp_dir.path(), 1)?;
    rt.block_on(async move {
        assert_eq!(
            store.async_get("key1".to_owned()).await.unwrap(),
            Some("value1".to_owned())
        );
        assert_eq!(
            store.async_get("key2".to_owned()).await.unwrap(),
            Some("value2".to_owned())
        );
    });
    Ok(())
}

// Should overwrite existent value
#[test]
fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = AsyncKvStore::<RayonThreadPool>::new(temp_dir.path(), 1)?;

    let rt = get_runtime();
    let store2 = store.clone();
    rt.block_on(async move {
        store2.async_set("key1".to_owned(), "value1".to_owned()).await.unwrap();
        assert_eq!(
            store2.async_get("key1".to_owned()).await.unwrap(),
            Some("value1".to_owned())
        );
        store2.async_set("key1".to_owned(), "value2".to_owned()).await.unwrap();
        assert_eq!(
            store2.async_get("key1".to_owned()).await.unwrap(),
            Some("value2".to_owned())
        );
    });

    // Open from disk again and check persistent data
    drop(store);
    let store = AsyncKvStore::<RayonThreadPool>::open(temp_dir.path(), 1)?;
    let store2 = store.clone();
        rt.block_on(async move {
        let store2 = store2.clone();
        assert_eq!(
            store2.async_get("key1".to_owned()).await.unwrap(),
            Some("value2".to_owned())
        );
        store2.async_set("key1".to_owned(), "value3".to_owned()).await.unwrap();
        assert_eq!(
            store2.async_get("key1".to_owned()).await.unwrap(),
            Some("value3".to_owned())
        );
    });

    drop(store);
    Ok(())
}

// Should get `None` when getting a non-existent key
#[test]
fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = AsyncKvStore::<RayonThreadPool>::new(temp_dir.path(), 1)?;

    let store2 = store.clone();
    let rt = get_runtime();
    rt.block_on(async move {
        store2.async_set("key1".to_owned(), "value1".to_owned()).await.unwrap();
        assert_eq!(store2.async_get("key2".to_owned()).await.unwrap(), None);
    });
    // Open from disk again and check persistent data
    drop(store);
    
    let store = AsyncKvStore::<RayonThreadPool>::open(temp_dir.path(), 1)?;
    rt.block_on(async move {    
        assert_eq!(store.async_get("key2".to_owned()).await.unwrap(), None);
    });
    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = AsyncKvStore::<RayonThreadPool>::new(temp_dir.path(), 1)?;
    let rt = get_runtime();
    rt.block_on(async move {
        assert!(store.async_remove("key1".to_owned()).await.is_err());
    });
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = AsyncKvStore::<RayonThreadPool>::new(temp_dir.path(), 1)?;
    let rt = get_runtime();
    rt.block_on(async move {
        store.async_set("key1".to_owned(), "value1".to_owned()).await.unwrap();
        assert!(store.async_remove("key1".to_owned()).await.is_ok());
        assert_eq!(store.async_get("key1".to_owned()).await.unwrap(), None);
    });
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = AsyncKvStore::<RayonThreadPool>::new(temp_dir.path(), 1)?;

    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let rt = get_runtime();
    rt.block_on(async {
        let mut current_size = dir_size();
        for iter in 0..1000 {
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                let value = format!("{}", iter);
                store.async_set(key, value).await.unwrap();
            }

            let new_size = dir_size();
            if new_size > current_size {
                current_size = new_size;
                continue;
            }
            // Compaction triggered

            drop(store);
            // reopen and check content
            let store = AsyncKvStore::<RayonThreadPool>::open(temp_dir.path(), 1)?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(store.async_get(key).await.unwrap(), Some(format!("{}", iter)));
            }
            return Ok(());
        }
        panic!("No compaction detected");
    })
}

#[test]
fn concurrent_set() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    // concurrent set in 8 threads
    let store = AsyncKvStore::<RayonThreadPool>::new(temp_dir.path(), 8)?;
    let rt = get_runtime();
    let mut futures = Vec::new();
    for i in 0..10000 {
        let fu = store.async_set(format!("key{}", i), format!("value{}", i));
        futures.push(fu);
    }

    rt.block_on(async move {
        let results = join_all(futures).await;
        for r in results {
            r.unwrap();
        }
    });

    // We only check concurrent set in this test, so we check sequentially here
    // TODO: 注释掉这个drop后，store的drop就不会执行，why?
    drop(store);
    let store = AsyncKvStore::<RayonThreadPool>::open(temp_dir.path(), 1)?;
    rt.block_on(async move {
        for i in 0..10000 {
            assert_eq!(
                store.async_get(format!("key{}", i)).await.unwrap(),
                Some(format!("value{}", i))
            );
        }
    });
    Ok(())
}

#[test]
fn concurrent_get() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = AsyncKvStore::<RayonThreadPool>::new(temp_dir.path(), 8)?;
    let rt = get_runtime();

    let store2 = store.clone();
    rt.block_on(async move {
        for i in 0..100 {
            store2
                .async_set(format!("key{}", i), format!("value{}", i))
                .await
                .unwrap();
        }
    });

    // We only check concurrent get in this test, so we set sequentially here
    let store3 = store.clone();
    let mut futures = Vec::new();
    for thread_id in 0..100 {
        for i in 0..100 {
            let key_id = (i + thread_id) % 100;
            futures.push(store3.async_get(format!("key{}", key_id)));
        }
    }
    rt.block_on(async move {
        let results = join_all(futures).await;
        for r in results {
            r.unwrap();
        }
    });
    Ok(())
}
