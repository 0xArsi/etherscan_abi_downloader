use log::{error, info, LevelFilter};
use std::process;

use etherscan_abi_downloader::abi_downloader::*;
use clap::Parser;
use std::path::PathBuf;
use env_logger::{Builder, Env};
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the file containing contract addresses
    #[clap(short, long, value_parser)]
    addresses: String,

    /// Directory to output the parquet files
    #[clap(short, long, value_parser)]
    output_dir: PathBuf,

    /// Path to the config file
    #[clap(short, long, value_parser)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = Builder::from_env(Env::default());
    builder.filter_level(LevelFilter::Info);
    builder.init();

    let args = Args::parse();

    let api_key = match args.config.to_str() {
        Some(config_path) => match read_api_key(config_path) {
            Ok(key) => key,
            Err(e) => {
                error!("Failed to read API key from {}: {}", config_path, e);
                process::exit(1);
            }
        },
        None => {
            error!("Invalid UTF-8 sequence in config path");
            process::exit(1);
        }
    };

    let client = match create_etherscan_client(&api_key) {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to create Etherscan client: {}", e);
            process::exit(1);
        }
    };

    let addresses = match read_addresses(&args.addresses) {
        Ok(addresses) => addresses,
        Err(e) => {
            error!("Failed to read addresses from {}: {}", args.addresses, e);
            process::exit(1);
        }
    };

    let (function_files, event_files) = match download_abis(&client, &addresses, &args.output_dir).await {
        Ok(abis) => abis,
        Err(e) => {
            error!("Failed to download ABIs: {}", e);
            process::exit(1);
        }
    };
    
    let all_functions_path = args.output_dir.join("all_functions.parquet");
    let all_events_path = args.output_dir.join("all_events.parquet");

    if let Err(e) = concatenate_parquet_files(&function_files, all_functions_path.to_str().unwrap()).await {
        error!("Failed to concatenate function files: {}", e);
        process::exit(1);
    }

    if let Err(e) = concatenate_parquet_files(&event_files, all_events_path.to_str().unwrap()).await {
        error!("Failed to concatenate event files: {}", e);
        process::exit(1);
    }

    info!("ABI download and processing completed successfully.");
    Ok(())
}
