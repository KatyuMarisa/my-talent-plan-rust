use clap::{crate_name, crate_version, crate_authors, command, Command, Arg};
use kvs::{Result, KvError, KvsEngine};

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
        )
        .get_matches();

    let root = tempfile::tempdir()?;
    let root_dir = root.path();
    let mut kv = kvs::KvStore::new(root_dir.to_owned())?;

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches.value_of("key").unwrap();
            let value = sub_matches.value_of("value").unwrap();
            kv.set(key.to_owned(), value.to_owned())?;
        }

        Some(("get", sub_matches)) => {
            let key = sub_matches.value_of("key").unwrap();
            if let Some(value) = kv.get(key.to_owned())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }

        Some(("rm", sub_matches)) => {
            let key = sub_matches.value_of("key").unwrap();
            if let Err(err) = kv.remove(key.to_owned()) {
                if let Some(KvError::KeyNotFoundError{key: _}) = err.downcast_ref::<_>() {
                    println!("Key not found");
                }
                return Err(err)
            }
        }

        _ => { unreachable!("not a valid command"); }
    }

    Ok(())
}