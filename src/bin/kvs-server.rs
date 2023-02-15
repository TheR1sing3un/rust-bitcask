use clap::{Parser, ValueEnum};
use kvs::{BitcaskEngine, Server};
use log::{error, info};
use std::{env, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:13131";
const DEFAULT_ENGIN: &str = "kvs";

const DEFAULT_PATH: &str = "/Users/lcy/kvs";

#[derive(Parser, Debug)]
#[clap(
    name = "kvs-server",
    author,
    version,
    about = "server of key value storage"
)]
struct Cli {
    #[clap(long = "addr", name = "SOCKET_ADDRESS", required = false, default_value = DEFAULT_LISTENING_ADDRESS)]
    address: SocketAddr,
    #[clap(long = "engine", name = "ENGINE", required = false, value_enum, default_value = DEFAULT_ENGIN, value_enum)]
    engin: Engine,
}

#[derive(Debug, Clone, ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}
impl Engine {
    fn name(&self) -> String {
        match self {
            Self::Kvs => "kvs".to_owned(),
            Self::Sled => "sled".to_owned(),
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = Cli::parse();
    info!("server start up with cmd: {:?}", cli);
    let sled_path = env::current_dir().unwrap().join("sled");
    let bitcask_path = env::current_dir().unwrap().join("kvs");
    if (sled_path.exists() && matches!(cli.engin, Engine::Kvs))
        || (bitcask_path.exists() && matches!(cli.engin, Engine::Sled))
    {
        error!("engine confilcts");
        panic!();
    }
    let path = env::current_dir().unwrap().join(cli.engin.name());
    let kv = BitcaskEngine::open(&path).unwrap();
    info!("kv open successfully!");
    let listener = TcpListener::bind(cli.address).await.unwrap();
    info!("starting server");
    let _ = Server::start(listener, Arc::new(kv)).await.unwrap();
}
