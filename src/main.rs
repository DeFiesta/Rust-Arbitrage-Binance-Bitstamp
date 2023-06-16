// WebSocket crates
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::Message as TMessage;
use tungstenite::Message;
use url::Url;

use futures::Stream;
use futures::StreamExt;
use futures::SinkExt;

use std::env;
use std::error::Error;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::pin::Pin;
use core::time::Duration;
use log::{info, error};
use serde_json::json;

use orderbook::{orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer}, Summary, Empty};
use tonic::{transport::Server, Request, Response, Status};

// used to parse orderbook update
use serde_json::Value;
use env_logger;

// your gRPC service implementations
mod orderbook {
    tonic::include_proto!("orderbook"); 
}


#[derive(Debug)]
pub struct OrderBook {
    bids: Vec<orderbook::Level>,
    asks: Vec<orderbook::Level>,
}

#[derive(Debug)]
pub struct OrderbookAggregatorImpl {
    pub order_book: Arc<Mutex<OrderBook>>,
}

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorImpl {
    type BookSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send + Sync + 'static>>;
    
    async fn book_summary(&self, _request: Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
        let order_book = Arc::clone(&self.order_book);
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            loop {
                // Fetch and lock the order book data
                let data = order_book.lock().await;
                // Create a summary message
                let summary = Summary {
                    spread: calculate_spread(&data),
                    bids: data.bids.clone(),
                    asks: data.asks.clone(),
                };
                // Send the summary
                tx.send(Ok(summary)).await.unwrap();
                // Wait for a bit before the next summary
                tokio::time::sleep(Duration::from_millis(100)).await
            }
        });
        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize the logger
    env_logger::init();

    // get symbol from env
    let symbol = env::var("SYMBOL")?;

    // initialize shared state
    let order_book = Arc::new(Mutex::new(OrderBook {
        bids: Vec::new(),
        asks: Vec::new(),
    }));

    let binance_url = format!("wss://stream.binance.com:9443/ws/{}@depth20@100ms", symbol);
    let bitstamp_url = format!("wss://ws.bitstamp.net");

    let binance_future = connect_to_exchange(binance_url, "binance", &symbol, Arc::clone(&order_book));
    let bitstamp_future = connect_to_exchange(bitstamp_url, "bitstamp", &symbol, Arc::clone(&order_book));

    println!("Created futures for binance and bitstamp");

    // start gRPC server in a new thread
    let grpc_service = OrderbookAggregatorImpl {
        order_book: Arc::clone(&order_book),
    };
    
    let grpc_future = tokio::spawn(async move {
        Server::builder()
            .add_service(OrderbookAggregatorServer::new(grpc_service))
            .serve("127.0.0.1:50051".parse().unwrap())
            .await
    });


    // run all futures to completion
    let (websocket_result1, websocket_result2, grpc_server) = futures::future::join3(binance_future, bitstamp_future, grpc_future).await;

    println!("Futures completed");

    match websocket_result1 {
        Ok(_) => info!("Websocket connection to Binance closed successfully."),
        Err(e) => error!("An error occurred in the websocket connection to Binance: {}", e),
    }
    
    match websocket_result2 {
        Ok(_) => info!("Websocket connection to Bitstamp closed successfully."),
        Err(e) => error!("An error occurred in the websocket connection to Bitstamp: {}", e),
    }
    
    match grpc_server {
        Ok(_) => info!("gRPC server closed successfully."),
        Err(e) => error!("An error occurred in the gRPC server: {}", e),
    }

    Ok(())
}



async fn connect_to_exchange(url: String, exchange: &str, symbol: &str, order_book: Arc<Mutex<OrderBook>>) -> anyhow::Result<()> {
    
    let modified_url = Url::parse(&url).unwrap();
    let domain = modified_url.domain().unwrap().to_string();
    let addr = modified_url.socket_addrs(|| None).unwrap().first().unwrap().to_string();
    let stream = TcpStream::connect(addr).await.unwrap();
    let connector = tokio_native_tls::TlsConnector::from(native_tls::TlsConnector::new()?);
    let tls_stream = connector.connect(&domain, stream).await.unwrap();
    
    let (mut ws_stream, _) = tokio_tungstenite::client_async(&url, tls_stream).await
        .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", exchange, e))?;
    
     // Send subscription message based on the exchange.
    let bitstamp_symbol = if symbol.ends_with("t") {
        &symbol[..symbol.len()-1]  // remove last character
    } else {
        symbol
    };
     if exchange == "bitstamp" {
        let subscribe_message_bitstamp = json!({
            "event": "bts:subscribe",
            "data": {
                "channel": format!("detail_order_book_{}", bitstamp_symbol)
            }
        }).to_string();

        ws_stream.send(Message::Text(subscribe_message_bitstamp)).await.unwrap();
        println!("Subscribed to the {} trades channel for {}", exchange, bitstamp_symbol);

    } else if exchange == "binance" {
        let subscribe_message_binance = format!(
            r#"{{
                "method": "SUBSCRIBE",
                "params": [
                    "{}@aggTrade",
                    "{}@depth"
                ],
                "id": 1
            }}"#,
            symbol, symbol
        );
        ws_stream.send(Message::Text(subscribe_message_binance)).await?;
    }


    // main loop for handling messages
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(TMessage::Text(text)) => {
                println!("raw data: {}", text);
                let order_book_update = parse_order_book_update(&text, exchange)?;
                //println!("orderbook update : {:?}", order_book_update);

                // Update shared order book
                let mut order_book_guard = order_book.lock().await;
                order_book_guard.bids = order_book_update.bids;
                order_book_guard.asks = order_book_update.asks;

                //println!("bids parsing result: {:?}", order_book_guard.bids);
                //println!("asks parsing result: {:?}", order_book_guard.asks);
            }
            Err(e) => {
                error!("Error receiving message from {}: {}", exchange, e);
                break;
            }
            _ => (),
        }
    }

    Ok(())
}



fn calculate_spread(order_book: &OrderBook) -> f64 {
    if let (Some(best_bid), Some(best_ask)) = (order_book.bids.first(), order_book.asks.first()) {
        best_ask.price - best_bid.price
    } else {
        0.0
    }
}

fn parse_order_book_update(message: &str, exchange: &str) -> anyhow::Result<OrderBook> {
    
    let v: Value = serde_json::from_str(message)?;

    // Modify this to get 'data' first for Bitstamp
    if exchange == "bitstamp" {
        if let Some(data) = v.get("data") {
            println!("Bitstamp data: {}", data);
            let bids = data["bids"]
                .as_array()
                .ok_or(anyhow::anyhow!("bids is not an array"))?;
            println!("Bitstamp bids: {:?}", bids);
            
            let parsed_bids = bids
                .iter()
                .map(|bid| {
                    let price = bid[0].as_f64().ok_or_else(|| anyhow::anyhow!("price is not a number"))?;
                    let amount = bid[1].as_f64().ok_or_else(|| anyhow::anyhow!("amount is not a number"))?;
                    println!("Parsed Bitstamp bid: price {}, amount {}", price, amount);
                    Ok(orderbook::Level {
                        exchange: exchange.to_string(),
                        price,
                        amount,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            println!("Parsed Bitstamp bids: {:?}", parsed_bids); // <-- Add this

            let asks = data["asks"]
                .as_array()
                .ok_or(anyhow::anyhow!("asks is not an array"))?;
            println!("Bitstamp asks: {:?}", asks); 
            
            let parsed_asks = asks
                .iter()
                .map(|ask| {
                    let price = ask[0].as_f64().ok_or_else(|| anyhow::anyhow!("price is not a number"))?;
                    let amount = ask[1].as_f64().ok_or_else(|| anyhow::anyhow!("amount is not a number"))?;
                    println!("Parsed Bitstamp ask: price {}, amount {}", price, amount); 
                    Ok(orderbook::Level {
                        exchange: exchange.to_string(),
                        price,
                        amount,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            println!("Parsed Bitstamp asks: {:?}", parsed_asks); 

            Ok(OrderBook { bids: parsed_bids, asks: parsed_asks })
        } else {
            Err(anyhow::anyhow!("The message did not contain the 'data' field"))
        }
    } else {
        //println!("Binance data: {}", v);
        let parsed_bids = v["bids"]
            .as_array()
            .ok_or(anyhow::anyhow!("bids is not an array"))?;
        //println!("Binance bids: {:?}", parsed_bids); 
        
        let bids = parsed_bids
            .iter()
            .map(|bid| {
                let price = bid[0].as_str().ok_or_else(|| {
                    let err = format!("Bid price is not a string. Value was: {:?}", bid[0]);
                    println!("{}", &err);
                    anyhow::anyhow!(err)
                })?.parse::<f64>().map_err(|_| {
                    let err = format!("Could not parse bid price as f64. Value was: {:?}", bid[0]);
                    println!("{}", &err);
                    anyhow::anyhow!(err)
                })?;
        
                let amount = bid[1].as_str().ok_or_else(|| {
                    let err = format!("Bid amount is not a string. Value was: {:?}", bid[1]);
                    println!("{}", &err);
                    anyhow::anyhow!(err)
                })?.parse::<f64>().map_err(|_| {
                    let err = format!("Could not parse bid amount as f64. Value was: {:?}", bid[1]);
                    println!("{}", &err);
                    anyhow::anyhow!(err)
                })?;
        
                //println!("Parsed Binance bid: price {}, amount {}", price, amount);
                Ok(orderbook::Level {
                    exchange: exchange.to_string(),
                    price,
                    amount,
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;


        let parsed_asks = v["asks"]
            .as_array()
            .ok_or(anyhow::anyhow!("asks is not an array"))?;
        //println!("Binance asks: {:?}", parsed_asks);

        let asks = parsed_asks
            .iter()
            .map(|ask| {
                let price = ask[0].as_str().ok_or_else(|| {
                    let err = format!("Ask price is not a string. Value was: {:?}", ask[0]);
                    println!("{}", &err);
                    anyhow::anyhow!(err)
                })?.parse::<f64>().map_err(|_| {
                    let err = format!("Could not parse ask price as f64. Value was: {:?}", ask[0]);
                    println!("{}", &err);
                    anyhow::anyhow!(err)
                })?;
        
                let amount = ask[1].as_str().ok_or_else(|| {
                    let err = format!("Ask amount is not a string. Value was: {:?}", ask[1]);
                    println!("{}", &err);
                    anyhow::anyhow!(err)
                })?.parse::<f64>().map_err(|_| {
                    let err = format!("Could not parse ask amount as f64. Value was: {:?}", ask[1]);
                    println!("{}", &err);
                    anyhow::anyhow!(err)
                })?;
        
                //println!("Parsed Binance ask: price {}, amount {}", price, amount);
                Ok(orderbook::Level {
                    exchange: exchange.to_string(),
                    price,
                    amount,
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        Ok(OrderBook { bids, asks })
    }
}






