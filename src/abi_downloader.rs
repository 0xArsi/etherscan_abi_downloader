use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use configparser::ini::Ini;
use foundry_block_explorers::Client;
use alloy_json_abi::{JsonAbi, Function, Event};
use alloy_chains::Chain;
use alloy_primitives::Address;
use polars::prelude::*;
use anyhow::{anyhow, Result};
use tokio::time;
use log::{info, warn};
use tiny_keccak::{Hasher, Keccak};

const RATE_LIMIT: Duration = Duration::from_millis(333);

#[derive(Debug)]
pub struct AbiRecord {
    pub record_type: String,
    pub contract_address: String,
    pub name: String,
    pub signature: String,
    pub selector: String,
}

pub fn write_parquet(records: &[AbiRecord], filename: &Path) -> Result<()> {
    let mut df = DataFrame::new(vec![
        Series::new("record_type", records.iter().map(|r| r.record_type.clone()).collect::<Vec<_>>()),
        Series::new("contract_address", records.iter().map(|r| r.contract_address.clone()).collect::<Vec<_>>()),
        Series::new("name", records.iter().map(|r| r.name.clone()).collect::<Vec<_>>()),
        Series::new("signature", records.iter().map(|r| r.signature.clone()).collect::<Vec<_>>()),
        Series::new("selector", records.iter().map(|r| r.selector.clone()).collect::<Vec<_>>()),
    ])?;

    let mut file = File::create(filename)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

pub async fn concatenate_parquet_files(input_files: &[PathBuf], output_file: &str) -> Result<()> {
    let lf = LazyFrame::scan_parquet_files(input_files.into(), ScanArgsParquet::default())?;
    let mut df = lf.collect()?;
    ParquetWriter::new(File::create(output_file)?).finish(&mut df)?;
    Ok(())
}
pub fn read_api_key(config_path: &str) -> Result<String> {
    let config = Ini::new().load(config_path)
        .map_err(|e| anyhow!("Failed to load config file: {}", e))?;
    
    let api_keys = config.get("api_keys")
        .ok_or_else(|| anyhow!("Could not find API key section in config file"))?;
    
    match api_keys.get(&"ETHERSCAN_API_KEY".to_lowercase()) {
        Some(v) => {
            match v {
                Some(s) => Ok(s.clone()),
                _ => Err(anyhow!("Could not find ETHERSCAN_API_KEY in config file")),
            }
        }
        _ => Err(anyhow!("Could not find ETHERSCAN_API_KEY in config file")),
    }
}


pub fn create_etherscan_client(api_key: &str) -> Result<Client> {
    Client::new(Chain::mainnet(), api_key)
        .map_err(|e| anyhow!("Failed to create Etherscan client: {}", e))
}

pub fn read_addresses(filename: &str) -> Result<Vec<String>> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().filter_map(|line| line.ok()).collect())
}

pub async fn download_abis(client: &Client, addresses: &[String], output_dir: &PathBuf) 
-> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    let functions_dir = output_dir.join("functions");
    let events_dir = output_dir.join("events");

    let mut function_files = Vec::new();
    let mut event_files = Vec::new();

    std::fs::create_dir_all(&functions_dir)
    .map_err(|e| anyhow!("failed to create functions output dir. {:?}", e))?;
    std::fs::create_dir_all(&events_dir)
    .map_err(|e| anyhow!("failed to create events output dir. {:?}", e))?;

    let total = addresses.len();
    for (index, address_str) in addresses.iter().enumerate() {
        info!("Downloading ABI for address {} ({}/{})", address_str, index + 1, total);
        std::io::stdout().flush()?;
        let addr_rep = Address::from_str(&address_str)?;
        match client.contract_abi(addr_rep).await {
            Ok(abi_json) => {
                let (functions, events) = process_contract(address_str, &abi_json)?;
                let function_file = functions_dir.join(format!("{}_functions.parquet", address_str));
                let event_file = events_dir.join(format!("{}_events.parquet", address_str));
                write_parquet(&functions, &function_file)?;
                write_parquet(&events, &event_file)?;
                function_files.push(function_file);
                event_files.push(event_file);
            },

            Err(e) => {
                print!("\n");
                warn!("Failed to fetch ABI for address {}: {}", address_str, e);
            }
        }
        time::sleep(RATE_LIMIT).await;
    }
    print!("\n");
    Ok((function_files, event_files))
}



pub fn process_contract(address: &str, abi_json: &JsonAbi) -> Result<(Vec<AbiRecord>, Vec<AbiRecord>)> {
    let function_records = abi_json.functions()
    .map(|f| {
        AbiRecord {
            name: f.name.clone(),
            record_type: "function".to_string(),
            contract_address: address.to_lowercase(),
            signature: create_function_signature(f),
            selector: create_function_selector(f)
        }
    }).collect::<Vec<_>>();

    let event_records = abi_json.events()
    .map(|e| {
        AbiRecord {
            name: e.name.clone(),
            record_type: "event".to_string(),
            contract_address: address.to_lowercase(),
            signature: create_event_signature(e),
            selector: create_event_selector(e)
        }
    }).collect::<Vec<_>>();
    

    Ok((function_records, event_records))
}

pub fn create_empty_record(address: &str) -> AbiRecord {
    AbiRecord {
        record_type: String::new(),
        contract_address: address.to_string(),
        name: String::new(),
        signature: String::new(),
        selector: String::new(),
    }
}

pub fn create_function_signature(f: &Function) -> String {
    let input_types: Vec<String> = f.inputs.iter()
        .filter_map(|input| input.selector_type().into())
        .map(String::from)
        .collect();
    format!("{}({})", f.name, input_types.join(","))
}

pub fn create_event_signature(e: &Event) -> String {
    let input_types: Vec<String> = e.inputs.iter()
        .filter_map(|input| input.selector_type().into())
        .map(String::from)
        .collect();
    format!("{}({})", e.name, input_types.join(","))
}

pub fn create_function_selector(f: &Function) -> String {
    let signature = create_function_signature(f);
    let mut keccak = Keccak::v256();
    keccak.update(signature.as_bytes());
    let mut output = [0u8; 32];
    keccak.finalize(&mut output);
    format!("0x{}", hex::encode(&output[..4]))
}

pub fn create_event_selector(e: &Event) -> String {
    let signature = create_event_signature(e);
    let mut keccak = Keccak::v256();
    keccak.update(signature.as_bytes());
    let mut output = [0u8; 32];
    keccak.finalize(&mut output);
    format!("0x{}", hex::encode(output))
}