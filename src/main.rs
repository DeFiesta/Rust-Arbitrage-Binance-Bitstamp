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
use log::error;
use serde_json::json;

use orderbook::{orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer}, Summary, Empty};
use tonic::{Request, Response, Status};

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

impl OrderBook {
    pub fn merge_and_sort(&mut self, new_bids: Vec<orderbook::Level>, new_asks: Vec<orderbook::Level>) {
        // Add new bids and asks
        self.bids.extend(new_bids);
        self.asks.extend(new_asks);

        // Sort bids and asks
        // Bids are sorted in descending order by price
        self.bids.sort_unstable_by(|a, b| b.price.partial_cmp(&a.price).unwrap());

        // Asks are sorted in ascending order by price
        self.asks.sort_unstable_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

        // Truncate bids and asks to get top 10
        self.truncate(10);
    }

    pub fn truncate(&mut self, depth: usize) {
        // Limit the depth of the order book
        self.bids.truncate(depth);
        self.asks.truncate(depth);
    }
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
                let mut data = order_book.lock().await;
                
                // Sort and keep only top 10 bids
                data.bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
                data.bids.truncate(10);

                // Sort and keep only top 10 asks
                data.asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
                data.asks.truncate(10);

                // Create a summary message
                let summary = Summary {
                    spread: calculate_spread(&*data),
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

    let url_binance = format!("wss://stream.binance.com:9443/ws/{}@depth20@100ms", symbol);
    let url_bitstamp = format!("wss://ws.bitstamp.net");
    
    match run(url_binance, url_bitstamp, symbol, order_book).await {
        Ok(()) => println!("Completed without error."),
        Err(err) => eprintln!("Error occurred: {:?}", err),
    }

    Ok(())
}

async fn run(url_binance: String, url_bitstamp: String, symbol: String, order_book: Arc<Mutex<OrderBook>>,) -> anyhow::Result<()> {
    let binance_orderbook = Arc::clone(&order_book);
    let bitstamp_orderbook = Arc::clone(&order_book);
    let binance_symbol = symbol.clone();
    let binance = tokio::spawn(async move {
        connect_to_exchange(url_binance, "binance", &binance_symbol, binance_orderbook).await
    });
    let bitstamp = tokio::spawn(async move {
        connect_to_exchange(url_bitstamp, "bitstamp", &symbol, bitstamp_orderbook).await
    });
    let _ = tokio::try_join!(binance, bitstamp)?;

    let order_book_guard = order_book.lock().await;
    println!("Final Bids: {:?}", order_book_guard.bids);
    println!("Final Asks: {:?}", order_book_guard.asks);


    Ok(())
}



async fn connect_to_exchange(url: String, exchange: &str, symbol: &str, order_book: Arc<Mutex<OrderBook>>) -> anyhow::Result<()> {
    
    if exchange == "binance" {
        let modified_url = Url::parse(&url).unwrap();
        let domain = modified_url.domain().unwrap().to_string();
        let addr = modified_url.socket_addrs(|| None).unwrap().first().unwrap().to_string();
        let stream = TcpStream::connect(addr).await.unwrap();
        let connector = tokio_native_tls::TlsConnector::from(native_tls::TlsConnector::new()?);
        let tls_stream = connector.connect(&domain, stream).await.unwrap();
        
        let (mut ws_stream, _) = tokio_tungstenite::client_async(&url, tls_stream).await
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", exchange, e))?;
        println!("Successfully connected to : {}", exchange);

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

        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(TMessage::Text(text)) => {
                    
                    let order_book_update = parse_order_book_update(&text, exchange)?;
                    // Update shared order book
                    let mut order_book_guard = order_book.lock().await;
                    // Prepare new Levels from the update
                    let new_bids: Vec<orderbook::Level> = order_book_update.bids.into_iter().map(|level| orderbook::Level {
                        exchange: exchange.to_string(),
                        price: level.price,
                        amount: level.amount,
                    }).collect();

                    let new_asks: Vec<orderbook::Level> = order_book_update.asks.into_iter().map(|level| orderbook::Level {
                        exchange: exchange.to_string(),
                        price: level.price,
                        amount: level.amount,
                    }).collect();

                    // Merge and sort the order books
                    order_book_guard.merge_and_sort(new_bids, new_asks);

                    println!("Binance Bids: {:?}", order_book_guard.bids);
                    println!("Binance Asks: {:?}", order_book_guard.asks);
                    break;
                }
                Err(e) => {
                    error!("Error receiving message from {}: {}", exchange, e);
                    break;
                }
                _ => (),
            }
        }
    }
    

    else if exchange == "bitstamp" {

        let modified_url = Url::parse(&url).unwrap();
        let domain = modified_url.domain().unwrap().to_string();
        let addr = modified_url.socket_addrs(|| None).unwrap().first().unwrap().to_string();
        let stream = TcpStream::connect(addr).await.unwrap();
        let connector = tokio_native_tls::TlsConnector::from(native_tls::TlsConnector::new()?);
        let tls_stream = connector.connect(&domain, stream).await.unwrap();

        let (mut ws_stream, _) = tokio_tungstenite::client_async(&url, tls_stream).await
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", exchange, e))?;
        println!("Successfully connected to : {}", exchange);

            
        let subscribe_message_bitstamp = json!({
            "event": "bts:subscribe",
            "data": {
                "channel": format!("order_book_{}", symbol)
            }
        }).to_string();
        
        ws_stream.send(Message::Text(subscribe_message_bitstamp)).await?;
        
        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(TMessage::Text(text)) => {
                    // Check the event type to ensure it is an order book update
                    let v: Value = serde_json::from_str(&text)?;
                    let event = v.get("event").and_then(|e| e.as_str());
                    if event == Some("data") {
                        //println!("raw data: {}", text);
                        let order_book_update = parse_order_book_update(&text, exchange)?;
                        // Update shared order book
                        let mut order_book_guard = order_book.lock().await;
                        let new_bids: Vec<orderbook::Level> = order_book_update.bids.into_iter().map(|level| orderbook::Level {
                            exchange: exchange.to_string(),
                            price: level.price,
                            amount: level.amount,
                        }).collect();
        
                        let new_asks: Vec<orderbook::Level> = order_book_update.asks.into_iter().map(|level| orderbook::Level {
                            exchange: exchange.to_string(),
                            price: level.price,
                            amount: level.amount,
                        }).collect();
        
                        // Merge and sort the order books
                        order_book_guard.merge_and_sort(new_bids, new_asks);
        
                        println!("Bitstamp Bids: {:?}", order_book_guard.bids);
                        println!("Bitstamp Asks: {:?}", order_book_guard.asks);
                        break;
                    }
                }
                Err(e) => {
                    error!("Error receiving message from {}: {}", exchange, e);
                    break;
                }
                _ => (),
            }
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
            let bids = data["bids"]
                .as_array()
                .ok_or(anyhow::anyhow!("bids is not an array"))?
                .iter()
                .map(|bid| {
                    let price = bid[0]
                        .as_str()
                        .ok_or(anyhow::anyhow!("bid price is not a string"))?
                        .parse::<f64>()
                        .map_err(|_| anyhow::anyhow!("failed to parse bid price as f64"))?;

                    let amount = bid[1]
                        .as_str()
                        .ok_or(anyhow::anyhow!("bid amount is not a string"))?
                        .parse::<f64>()
                        .map_err(|_| anyhow::anyhow!("failed to parse bid amount as f64"))?;

                    Ok(orderbook::Level {
                        exchange: exchange.to_string(),
                        price,
                        amount,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            let asks = data["asks"]
                .as_array()
                .ok_or(anyhow::anyhow!("asks is not an array"))?
                .iter()
                .map(|ask| {
                    let price = ask[0]
                        .as_str()
                        .ok_or(anyhow::anyhow!("ask price is not a string"))?
                        .parse::<f64>()
                        .map_err(|_| anyhow::anyhow!("failed to parse ask price as f64"))?;

                    let amount = ask[1]
                        .as_str()
                        .ok_or(anyhow::anyhow!("ask amount is not a string"))?
                        .parse::<f64>()
                        .map_err(|_| anyhow::anyhow!("failed to parse ask amount as f64"))?;

                    Ok(orderbook::Level {
                        exchange: exchange.to_string(),
                        price,
                        amount,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            return Ok(OrderBook { bids, asks });
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

