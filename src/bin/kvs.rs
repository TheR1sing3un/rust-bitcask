use std::{env, process};

use clap::{Parser, Subcommand};
use kvs::KvStore;

#[derive(Parser)]
#[clap(name = "kvs", author = "TheR1sing3un <ther1sing3un@163.com>", version, about = "an in-memory key/value store", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    #[clap(arg_required_else_help = true, name = "set")]
    Set { key: String, value: String },
    #[clap(arg_required_else_help = true, name = "get")]
    Get { key: String },
    #[clap(arg_required_else_help = true, name = "rm")]
    Remove { key: String },
}

fn main() {
    let cli = Cli::parse();
    let path = env::current_dir().unwrap();
    let mut kv: KvStore = KvStore::open(path).unwrap();
    match &cli.command {
        Commands::Get { key } => {
            if let Ok(Some(value)) = kv.get(key.clone()) {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Commands::Set { key, value } => match kv.set(key.clone(), value.clone()) {
            Ok(_) => {}
            Err(err) => println!("{}", err),
        },
        Commands::Remove { key } => {
            if let Ok(_) = kv.remove(key.clone()) {
            } else {
                println!("Key not found");
                process::exit(-1);
            }
        }
    }
}
