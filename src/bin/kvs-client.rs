use std::process::exit;

use clap::{crate_version, crate_authors, command, Command, Arg};

use kvs::{Request, KvsError, Client};
type Result<T> = std::result::Result<T, KvsError>;


#[tokio::main]
async fn main() -> Result<()> {
    let matches = command!()
        .author(crate_authors!())
        .version(crate_version!())
        .name("kvs-client")
        .subcommand(
            Command::new("set")
            .about("set a key-value mapping")
            .arg(
                Arg::new("key")
                .takes_value(true)
                .required(true)
                .help("key string")
            )
            .arg(
                Arg::new("value")
                .takes_value(true)
                .required(true)
                .help("value string")
            )
            .arg(
                Arg::new("dst")
                .takes_value(true)
                .required(false)
                .long("--addr")
                .default_missing_value("127.0.0.1:4000")
            )
        )
        .subcommand(
            Command::new("get")
            .about("get a stored mapping")
            .arg(
                Arg::new("key")
                .takes_value(true)
                .required(true)
                .help("key string")
            )
            .arg(
                Arg::new("dst")
                .takes_value(true)
                .required(false)
                .long("--addr")
                .default_missing_value("127.0.0.1:4000")
            )
        )
        .subcommand(
            Command::new("rm")
            .about("removed a mapping")
            .arg(
                Arg::new("key")
                .takes_value(true)
                .required(true)
                .help("key string")
            )
            .arg(
                Arg::new("dst")
                .takes_value(true)
                .required(false)
                .long("--addr")
                .default_missing_value("127.0.0.1:4000")
            )
        )
        .get_matches();


    let req: Request;
    let addr: &str;
    let mut is_get: bool = false;

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            req = Request::SET {
                key: sub_matches.value_of("key").unwrap().to_owned(),
                value: sub_matches.value_of("value").unwrap().to_owned()
            };
            addr = sub_matches.value_of("dst").unwrap();
        }

        Some(("get", sub_matches)) => {
            req = Request::GET {
                key: sub_matches.value_of("key").unwrap().to_owned(),
            };
            is_get = true;
            addr = sub_matches.value_of("dst").unwrap();
        }

        Some(("rm", sub_matches)) => {
            req = Request::REMOVE {
                key: sub_matches.value_of("key").unwrap().to_owned(),
            };
            addr = sub_matches.value_of("dst").unwrap();
        }

        _ => { unreachable!() }
    }

    let mut cli = Client::new(addr).await?;
    let reply = cli.ping_pong(req).await?;
    match reply {
        Ok(reply) => {
            if is_get && reply.is_none() {
                println!("Key not found");
            }
        }

        Err(err) => {
            eprintln!("{}", err);
            exit(1);
        }
    }
    Ok(())
}