// use std::{process::exit, env::current_dir};
// 
// use clap::{crate_version, crate_authors, command, Arg};
// 
// use kvs::{Result, KvsEngine, SledKvsEngine, KvServer};
// 
// fn run_with_engine<E: KvsEngine>(engine: E, ip_addr: String) -> Result<()> {
//     let mut server = KvServer::new(engine, ip_addr);
//     server.run()
// }
// 
// fn main() -> Result<()> {
//     let matche = command!()
//         .author(crate_authors!())
//         .version(crate_version!())
//         .name("kvs-server")
//         .about("start a kv-server")
//         .arg(
//             Arg::new("ip-port")
//             .takes_value(true)
//             .required(false)
//             .long("--addr")
//             .default_missing_value("127.0.0.1:4000")
//             .help("key string")
//         )
//         .arg(
//             Arg::new("engine")
//             .takes_value(true)
//             .required(false)
//             .long("--engine")
//             .default_missing_value("kvs")
//             .help("set a default engine")
//         )
//         .get_matches();
// 
//     let ip_port = matche.value_of("ip-port").unwrap();
//     match matche.value_of("engine").unwrap() {
//         "sled" => {
//             run_with_engine(SledKvsEngine::new(sled::open(current_dir()?)?), ip_port.to_owned())?
//         }
//         "kvs" => {
//             run_with_engine(KvStore::new(current_dir()?)?, ip_port.to_owned())?
//         }
//         _ => {
//             println!("No such engine");
//             exit(1);
//         }
//     };
//     Ok(())
// }

fn main() { }