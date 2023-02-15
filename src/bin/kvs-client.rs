use std::net::SocketAddr;

use clap::{Parser, Subcommand};
use kvs::Client;
use log::info;
use tokio::net::TcpStream;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:13131";

#[derive(Parser, Debug)]
#[clap(name = "kvs-client", author, version, about = "client to operate key value storage", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
    #[clap(long = "addr", name = "SOCKET_ADDRESS", required = false, default_value = DEFAULT_LISTENING_ADDRESS)]
    address: SocketAddr,
}
#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(arg_required_else_help = true, name = "set")]
    Set { key: String, value: String },
    #[clap(arg_required_else_help = true, name = "get")]
    Get { key: String },
    #[clap(arg_required_else_help = true, name = "rm")]
    Remove { key: String },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    env_logger::init();
    info!("client start up with args: {:?}", cli);
    let socket = TcpStream::connect(cli.address).await.unwrap();
    let mut client = Client::new(socket);
    match &cli.command {
        Commands::Get { key } => {
            if let Ok(Some(value)) = client.get(key.clone()).await {
                println!("Get key: {}, value: {} success!", key, value);
            } else {
                println!("Get key: {} not found", key);
            }
        }
        Commands::Set { key, value } => match client.set(key.clone(), value.clone()).await {
            Ok(_) => {
                println!("Set key: {}, value: {} success!", key, value);
            }
            Err(err) => println!("Set key: {}, value: {} error: {}", key, value, err),
        },
        Commands::Remove { key } => {
            if let Err(err) = client.remove(key.clone()).await {
                eprintln!("Remove key: {} error: {}", key, err);
            } else {
                println!("Remove key: {} success!", key);
            }
        }
    }
}
