use std::{process::exit, env::current_dir};

use clap::{crate_version, crate_authors, command, Arg};

use kvs::{Result, AsyncKvsEngine, AsyncKvStore, AsyncKvServer, SledKvStore, thread_pool::RayonThreadPool};

fn run_with_engine<E: AsyncKvsEngine>(engine: E, ip_addr: String) -> Result<()> {
    let mut server = AsyncKvServer::new(engine, ip_addr);
    let rt = tokio::runtime::Builder
        ::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .unwrap();
    
    rt.block_on(server.run())
}

fn main() -> Result<()> {
    let matches = command!()
        .author(crate_authors!())
        .version(crate_version!())
        .name("async-kvs-server")
        .about("start a async-kv-server")
        .arg(
            Arg::new("ip-port")
            .takes_value(true)
            .required(false)
            .long("--addr")
            .default_missing_value("127.0.0.1:4000")
            .help("key string")
        )
        .arg(
            Arg::new("engine")
            .takes_value(true)
            .required(false)
            .long("--engine")
            .default_missing_value("kvs")
            .help("set a default engine")
        )
        .get_matches();

    let ip_port = matches.value_of("ip-port").unwrap();
    match matches.value_of("engine").unwrap() {
        "sled" => {
            run_with_engine(SledKvStore::new(sled::open(current_dir()?)?), ip_port.to_owned())?
        }
        "kvs" => {
            run_with_engine(AsyncKvStore::<RayonThreadPool>::new(current_dir()?, 8)?, ip_port.to_owned())?
        }
        _ => {
            println!("No such engine");
            exit(1);
        }
    };

    Ok(())
}

// #[tokio::main]
// async fn main() -> Result<()> {

//     Ok(())
// }

// use clap::{crate_name, crate_version, crate_authors, command, Command, Arg};
// use kvs::{Result, KvError, KvsEngine};
// 
// fn main() -> Result<()> {
//     let matches = command!()
//         .author(crate_authors!())
//         .version(crate_version!())
//         .name(crate_name!())
//         .subcommand(
//             Command::new("set")
//             .about("set a key-value mapping")
//             .arg(
//                 Arg::new("key")
//                 .takes_value(true)
//                 .required(true)
//                 .help("key string")
//             )
//             .arg(
//                 Arg::new("value")
//                 .takes_value(true)
//                 .required(true)
//                 .help("value string")
//             )
//         )
//         .subcommand(
//             Command::new("get")
//             .about("get a stored mapping")
//             .arg(
//                 Arg::new("key")
//                 .takes_value(true)
//                 .required(true)
//                 .help("key string")
//             )
//         )
//         .subcommand(
//             Command::new("rm")
//             .about("removed a mapping")
//             .arg(
//                 Arg::new("key")
//                 .takes_value(true)
//                 .required(true)
//                 .help("key string")
//             )
//         )
//         .get_matches();
// 
//     let root = tempfile::tempdir()?;
//     let root_dir = root.path();
//     let mut kv = kvs::KvStore::new(root_dir.to_owned())?;
// 
//     match matches.subcommand() {
//         Some(("set", sub_matches)) => {
//             let key = sub_matches.value_of("key").unwrap();
//             let value = sub_matches.value_of("value").unwrap();
//             kv.set(key.to_owned(), value.to_owned())?;
//         }
// 
//         Some(("get", sub_matches)) => {
//             let key = sub_matches.value_of("key").unwrap();
//             if let Some(value) = kv.get(key.to_owned())? {
//                 println!("{}", value);
//             } else {
//                 println!("Key not found");
//             }
//         }
// 
//         Some(("rm", sub_matches)) => {
//             let key = sub_matches.value_of("key").unwrap();
//             if let Err(err) = kv.remove(key.to_owned()) {
//                 if let Some(KvError::KeyNotFoundError{key: _}) = err.downcast_ref::<_>() {
//                     println!("Key not found");
//                 }
//                 return Err(err)
//             }
//         }
// 
//         _ => { unreachable!("not a valid command"); }
//     }
// 
//     Ok(())
// }
