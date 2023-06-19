## Rust Challenge

## Requirements For Initial Setup
- Install cargo :
`$ curl https://sh.rustup.rs -sSf | sh`
- install cmake :
`$ brew install cmake`

## Setting Up
### 1. Clone/Download the Repository
`$ git clone https://github.com/mberiane/Rust_Challenge.git `

### 2. Configure exports
`$ export SYMBOL="btcusdt"`
`$ export RUST_LOG=debug`

### 2.  Run the code:
- open two different terminals
`$ cargo run --bin orderbook-server`
`$ cargo run --bin orderbook-client`