use tonic::transport::Server;
use tonic::{Request, Response, Status};

use tokio::sync::mpsc;

use futures_core::Stream;
//use async_stream::stream;

//use futures_util::pin_mut;
//use futures_util::stream::StreamExt;


mod orderbook;
use orderbook::orderbook_aggregator_server::{OrderbookAggregatorServer, OrderbookAggregator};
use orderbook::{Empty, Summary, Level};

use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Default)]
pub struct MyServer {}

#[tonic::async_trait]
impl Stream for BookSummaryStream {
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Option<()>>
    {
        if self.rem == 0 {
            // No more delays
            return Poll::Ready(None);
        }

        match Pin::new(&mut self.delay).poll(cx) {
            Poll::Ready(_) => {
                self.rem -= 1;
                Poll::Ready(Some(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl OrderbookAggregator for MyServer {

//    let s = Stream! {
//        let bids = vec![Level{exchange: "nick".to_string(), price: 1.0, amount: 1.0 }];
//        let asks = vec![Level{exchange: "nick".to_string(), price: 1.1, amount: 1.1 }];
//        let quotes = vec![Summary{ spread: 0.1, bids: bids, asks: asks}, Summary{ spread: 0.1, bids: bids, asks: asks}];
//        return quotes;
//    }

    //type BookSummaryStream: futures_core::Stream<Item=Result<Summary, Status>>
    type BookSummaryStream: Stream<Item=Result<Summary, Status>>
        + Send
        + Sync
        + 'static = mpsc::Receiver<Result<Summary, Status>>;

    async fn book_summary(&self, request : Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
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
        });

        Ok(Response::new(rx))
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
