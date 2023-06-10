use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};
use tonic::transport::Server;


pub mod orderbook;
use orderbook::orderbook_aggregator_server::{OrderbookAggregatorServer, OrderbookAggregator};
use orderbook::{Empty, Summary, Level};

#[derive(Debug)]
pub struct MyServer {}

impl Default for MyServer {
    fn default() -> Self {
        MyServer {
        }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for MyServer {

    type BookSummaryStream = UnboundedReceiverStream<Result<Summary, Status>>;

    async fn book_summary(&self, request : Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
        //let (mut tx, rx) = mpsc::channel(4);
        let (tx, rx) = mpsc::unbounded_channel();
        //let mut rx = UnboundedReceiverStream::new(rx);

        let bids = vec![Level{exchange: "nick".to_string(), price: 1.0, amount: 1.0 }];
        let asks = vec![Level{exchange: "nick".to_string(), price: 1.1, amount: 1.1 }];
        let quotes = vec![Summary{ spread: 0.1, bids: bids.clone(), asks: asks.clone()}, Summary{ spread: 0.2, bids: bids.clone(), asks: asks.clone()}];

        tokio::spawn(async move {
            for q in quotes {
                let quote = q.clone();
                if let Err(err) = tx.send(Ok(Summary {
                    spread: quote.spread,
                    bids: quote.bids,
                    asks: quote.asks,
                })) {
                     println!("ERROR: failed to update stream client: {:?}", err);
                        return;
                    }
            }
        });

        let stream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(stream as Self::BookSummaryStream))
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
