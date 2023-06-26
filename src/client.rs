mod orderbook;

use futures::StreamExt;

use orderbook::orderbook_aggregator_client::OrderbookAggregatorClient;
use orderbook::Empty;

async fn listen() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://0.0.0.0:50051").await?;

    let mut stream = client.book_summary(Empty{}).await?.into_inner();

    println!("streaming");
    while let Some(item) = stream.next().await {
        println!("{:?}", item.unwrap());
    }
    println!("stream closed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    listen().await?;
    Ok(())
}
