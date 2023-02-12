use std::process::exit;

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
    let mut kv: KvStore = KvStore::new();
    match &cli.command {
        Commands::Get { key } => {
            eprintln!("unimplemented");
            exit(1);
            // if let Some(value) = kv.get(key.clone()) {
            //     println!("get key = {}, value = {}", key, value);
            // } else {
            //     println!("gey key = {}, but value not found", key);
            // }
        }
        Commands::Set { key, value } => {
            // kv.set(key.clone(), value.clone());
            // println!("set key = {}, value = {}", key, value);
            eprintln!("unimplemented");
            exit(1);
        }
        Commands::Remove { key } => {
            // kv.remove(key.clone());
            // println!("remove key = {}", key);
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
