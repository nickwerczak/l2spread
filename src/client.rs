pub mod orderbook;

use futures::StreamExt;

use orderbook::orderbook_aggregator_client::OrderbookAggregatorClient;
use orderbook::{Empty, Summary, Level};










async fn listen() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://0.0.0.0:50051").await?;

    let mut stream = client.book_summary(Empty{}).await?.into_inner();

    println!("streaming");
    while let Some(item) = stream.next().await {
        match item {
            Ok(item) => println!("{:?}", item),
            Err(err) => {
                if err.code() == tonic::Code::NotFound {
                    println!("watched item has been removed from the inventory.");
                    break;
                } else {
                    return Err(err.into());
                }
            }
        };
    }
    println!("stream closed");

    Ok(())
}




#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    listen().await?;
    Ok(())
}
