use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use tokio::io::{AsyncWriteExt};

use std::sync::Arc;
use tokio::sync::Mutex;

mod orderbook;
mod binance_ob;
mod bitstamp_ob;
mod exchange_ob;

use orderbook::orderbook_aggregator_server::{OrderbookAggregatorServer, OrderbookAggregator};
use orderbook::{Empty, Summary, Level};

#[derive(Debug)]
pub struct MyServer {
}

impl MyServer {
    fn new(
    ) -> Self {
        MyServer {
        }
    }

    async fn run(&self) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let bnce_sender = tx.clone();
            tokio::spawn(async move {
                println!("starting binance listener...");
                binance_ob::binance_ob_listener(&bnce_sender).await.unwrap();
                println!("created binance listener...");
            });

            let stmp_sender = tx.clone();
            tokio::spawn(async move {
                println!("starting bitstamp listener...");
                bitstamp_ob::bitstamp_ob_listener(&stmp_sender).await.unwrap();
                println!("created bitstamp listener...");
            });

            while let Some(data) = rx.recv().await {
                tokio::io::stdout().write(&data).await.unwrap();
            }
        });
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for MyServer {

    type BookSummaryStream = UnboundedReceiverStream<Result<Summary, Status>>;

    async fn book_summary(&self, request : Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
        let (tx, rx) = mpsc::unbounded_channel();

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

    println!("starting server...");
    let addr = "0.0.0.0:50051".parse()?;
    let mut orderbook_aggregator_server = MyServer::new();
    orderbook_aggregator_server.run().await;
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook_aggregator_server))
        .serve(addr)
        .await?;

    Ok(())
}
