use std::net::TcpStream;

use clap::{crate_name, crate_version, crate_authors, command, Command, Arg};
use kvs::{Result};

fn main() -> Result<()> {
    let matches = command!()
        .author(crate_authors!())
        .version(crate_version!())
        .name(crate_name!())
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

    let key: &str;
    let value: &str;
    let addr: &str;
    let action: u8;
    
    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            key = sub_matches.value_of("key").unwrap();
            value = sub_matches.value_of("value").unwrap();
            addr = sub_matches.value_of("dst").unwrap();
            action = 0;
        }

        Some(("get", sub_matches)) => {
            key = sub_matches.value_of("key").unwrap();
            value = sub_matches.value_of("value").unwrap();
            addr = sub_matches.value_of("dst").unwrap();
            action = 1;
        }

        Some(("rm", sub_matches)) => {
            key = sub_matches.value_of("key").unwrap();
            value = sub_matches.value_of("value").unwrap();
            addr = sub_matches.value_of("dst").unwrap();
            action = 2;
        }

        _ => { unreachable!() }
    }

    let mut conn = TcpStream::connect(addr)?;
    // match action {
    //     0 => {

    //     }

    //     1 => {

    //     }

    //     2 => {

    //     }
    // }

    Ok(())
}