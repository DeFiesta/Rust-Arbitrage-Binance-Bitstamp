// WebSocket crates
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::Message as TMessage;
use tungstenite::Message;
use url::Url;

use futures::stream::{self, Stream};
use futures::StreamExt;
use futures::SinkExt;

use std::env;
use std::error::Error;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::pin::Pin;
use log::error;
use serde_json::json;

// gRPC crates
use orderbook::orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer};
use orderbook::{Summary, Level, Empty};
use tonic::{Request, Response, Status};
use tonic::transport::Server;


// used to parse orderbook update
use serde_json::Value;
use env_logger;

// gRPC server implementations
mod orderbook {
    tonic::include_proto!("orderbook"); 
}

//initiate the orderbook struct
#[derive(Debug)]
pub struct OrderBook {
    bids: Vec<orderbook::Level>,
    asks: Vec<orderbook::Level>,
    spread: f64,
}

#[derive(Debug)]
pub struct MyOrderbookAggregator {
    pub order_book: Arc<Mutex<OrderBook>>,
}

impl OrderBook {
    pub fn calculate_spread(&mut self) {
        if let (Some(best_bid), Some(best_ask)) = (self.bids.first(), self.asks.first()) {
            self.spread = best_ask.price - best_bid.price;
        } else {
            self.spread = 0.0;
        }
    }
    
    pub fn merge_and_sort(&mut self, new_bids: Vec<orderbook::Level>, new_asks: Vec<orderbook::Level>) {
        self.bids.extend(new_bids);
        self.asks.extend(new_asks);
    
        // Sort bids from high to low
        self.bids.sort_unstable_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
        // Sort asks from low to high
        self.asks.sort_unstable_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
    
        // Limit to top 10
        self.bids.truncate(10);
        self.asks.truncate(10);
    
        // Calculate the spread
        self.calculate_spread();
    }

    pub fn truncate(&mut self, depth: usize) {
        // Limit the depth of the order book
        self.bids.truncate(depth);
        self.asks.truncate(depth);
    }
}
impl MyOrderbookAggregator {
    pub fn new(order_book: Arc<Mutex<OrderBook>>) -> Self {
        Self { order_book }
    }
}

// implementation of the gRPC server-side functions 
#[tonic::async_trait]
impl OrderbookAggregator for MyOrderbookAggregator {
    type BookSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send + Sync + 'static>>;

    async fn book_summary(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        log::info!("Received request: {:?}", request);

        let order_book_clone = self.order_book.clone();

        let output_stream = stream::unfold(order_book_clone, |order_book| async move {
            let data = order_book.lock().await;
            
            let bids = data.bids.iter().map(|level| Level {
                exchange: level.exchange.clone(),
                price: level.price,
                amount: level.amount,
            }).collect();
            
            let asks = data.asks.iter().map(|level| Level {
                exchange: level.exchange.clone(),
                price: level.price,
                amount: level.amount,
            }).collect();

            let update = Summary {
                bids,
                asks,
                spread: data.spread,
            };
            log::info!("Sending response: {:?}", update);
            Some((Ok(update), Arc::clone(&order_book)))
        });

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
        spread: 0.0,
    }));


    let url_binance = format!("wss://stream.binance.com:9443/ws/{}@depth20@100ms", symbol);
    let url_bitstamp = format!("wss://ws.bitstamp.net");
    
    match run(url_binance, url_bitstamp, symbol, Arc::clone(&order_book)).await {
        Ok(()) => println!("Completed without error."),
        Err(err) => eprintln!("Error occurred: {:?}", err),
    }

    // launch gRPC server
    let addr = "[::1]:50051".parse().unwrap();
    let orderbook_aggregator = MyOrderbookAggregator::new(Arc::clone(&order_book));

    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook_aggregator))
        .serve(addr)
        .await?;
   
    Ok(())
}

//Merges orderbooks fetched by websocket functions
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
    let _order_book_guard = order_book.lock().await;

    Ok(())
}


// connect websocket to chosen exchange
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
        //println!("Successfully connected to : {}", exchange);

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
        //println!("Successfully connected to : {}", exchange);

            
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

// parses the data to separate bids and asks fetched and fills the orderbook based on the proto arcchitecture 
fn parse_order_book_update(message: &str, exchange: &str) -> anyhow::Result<OrderBook> {
    
    let v: Value = serde_json::from_str(message)?;

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

            return Ok(OrderBook { bids, asks, spread: 0.0 });
        } else {
            Err(anyhow::anyhow!("The message did not contain the 'data' field"))
        }

    } else {
        let parsed_bids = v["bids"]
            .as_array()
            .ok_or(anyhow::anyhow!("bids is not an array"))?; 
        
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
        
                Ok(orderbook::Level {
                    exchange: exchange.to_string(),
                    price,
                    amount,
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        Ok(OrderBook { bids, asks, spread: 0.0 })
    }
}

