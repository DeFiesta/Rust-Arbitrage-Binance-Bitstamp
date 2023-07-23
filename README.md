## Rust Challenge
1. connects to two exchanges' websocket feeds at the same time,
2. pulls order books, using these streaming connections, for a given traded pair of currencies (configurable), from each exchange,
3. merges and sorts the order books to create a combined order book,
4. from the combined book, publishes the spread, top ten bids, and top ten asks, as a stream, through a gRPC server.

## Requirements For Initial Setup
- Install cargo :
`$ curl https://sh.rustup.rs -sSf | sh`
- install cmake :
`$ brew install cmake`

## Setting Up
### 1. Clone/Download the Repository
`$ git clone https://github.com/mberiane/Rust_Challenge.git `

### 2. Configure exports
`$ export SYMBOL="ethbtc"`
`$ export RUST_LOG=debug`

### 2.  Run the code:
Launch these two commands from two separate terminals
`$ cargo run --bin orderbook-server`
`$ cargo run --bin orderbook-client`
