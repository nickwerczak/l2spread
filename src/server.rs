use tonic::transport::Server;
use tonic::{Request, Response, Status};

use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

mod orderbook;
use orderbook::orderbook_aggregator_server::{OrderbookAggregatorServer, OrderbookAggregator};
use orderbook::{Empty, Summary, Level};

#[derive(Default)]
pub struct MyServer {}


#[tonic::async_trait]
impl OrderbookAggregator for MyServer {

    type BookSummaryStream = UnboundedReceiverStream<Result<Summary, tonic::Status>>;

    async fn book_summary(&self, request : Request<Empty>) -> Result<Response<Self::BookSummaryStream>, tonic::Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let bids = vec![Level{exchange: "nick".to_string(), price: 1.0, amount: 1.0 }];
        let asks = vec![Level{exchange: "nick".to_string(), price: 1.1, amount: 1.1 }];
        let quotes = vec![Summary{ spread: 0.1, bids: bids, asks: asks}, Summary{ spread: 0.1, bids: bids, asks: asks}];

        tokio::spawn(async move {
            for q in quotes {
                let quote = q.clone();
                tx.send(Ok(Summary {
                    spread: quote.spread,
                    bids: quote.bids,
                    asks: quote.asks,
                })).await.unwrap();
            }
//            tx.send(Ok(summary_stream())).await.unwrap();
        });

        let stream = Self::BookSummaryStream::new(rx);
        //Ok(Response::new(Box::pin(stream)))
        Ok(Response::new(stream))
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;

    let orderbook_aggregator_server = MyServer::default();
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook_aggregator_server))
        .serve(addr)
        .await?;

    Ok(())
}
