## Etherscan ABI Downloader
This tool downloads ABI tables containing some (but not all) of the ABI info for a given list of contracts.

## Requirements
This tool depends on an Etherscan API key for downloading data. You should have a `.ini` file with the following structure:
```
[api_keys]
ETHERSCAN_API_KEY = <your_etherscan_api_key>
```


## Installation
You can do 
```
cargo install --git https://github.com/0xArsi/etherscan_abi_downloader.git
```
or
```
git clone https://github.com/0xArsi/etherscan_abi_downloader.git
cd etherscan_abi_downloader
cargo install --path .
```

## Usage
```
Usage: etherscan_abi_downloader --addresses <ADDRESSES> --output-dir <OUTPUT_DIR> --config <CONFIG>

Options:
  -a, --addresses <ADDRESSES>    Path to the file containing contract addresses
  -o, --output-dir <OUTPUT_DIR>  Directory to output the parquet files
  -c, --config <CONFIG>          Path to the config file
  -h, --help                     Print help
  -V, --version                  Print version
  ```