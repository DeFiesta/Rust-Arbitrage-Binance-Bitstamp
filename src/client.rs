use crate::orderbook::orderbook_aggregator_client::OrderbookAggregatorClient;
use tonic::transport::Channel;
use orderbook::Empty;

mod orderbook {
    tonic::include_proto!("orderbook"); 
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://[::1]:50051")
        .connect()
        .await?;

    // Create a client.
    let mut client = OrderbookAggregatorClient::new(channel);
    // Create a request.
    let request = tonic::Request::new(Empty {});
    // Call the `book_summary` method.
    let response = client.book_summary(request).await?;
    // Print the response.
    println!("RESPONSE={:?}", response);

    
    Ok(())

}