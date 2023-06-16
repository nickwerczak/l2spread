use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use bincode;

use std::sync::Arc;
use tokio::sync::Mutex;

mod orderbook;
mod binance_ob;
mod bitstamp_ob;
mod exchange_ob;
use exchange_ob::ExchangeOrderbook;

use orderbook::orderbook_aggregator_server::{OrderbookAggregatorServer, OrderbookAggregator};
use orderbook::{Empty, Summary, Level};

#[derive(Debug)]
pub struct MyServer {
    clients: Arc<Mutex<Vec<mpsc::UnboundedSender<Summary>>>>,
}

impl MyServer {
    fn new() -> Self {
        MyServer {
            clients: Arc::new(Mutex::new(Vec::<mpsc::UnboundedSender<Summary>>::new())),
        }
    }

    async fn connect(&self) -> mpsc::UnboundedReceiver<Summary> {
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let mut clients_connections = self.clients.lock().await;
        clients_connections.push(client_tx);
        client_rx
    }

    fn compare(sum1: &Summary, sum2: &Summary) -> bool {
        if sum1.spread != sum2.spread {
            return false;
        }
        if sum1.bids.len() != sum2.bids.len() {
            return false;
        }
        for i in 0..sum1.bids.len() {
            if sum1.bids[i].exchange != sum2.bids[i].exchange {
                return false;
            }
            if sum1.bids[i].price != sum2.bids[i].price {
                return false;
            }
            if sum1.bids[i].amount != sum2.bids[i].amount {
                return false;
            }
        }
        if sum1.asks.len() != sum2.asks.len() {
            return false;
        }
        for i in 0..sum1.asks.len() {
            if sum1.asks[i].exchange != sum2.asks[i].exchange {
                return false;
            }
            if sum1.asks[i].price != sum2.asks[i].price {
                return false;
            }
            if sum1.asks[i].amount != sum2.asks[i].amount {
                return false;
            }
        }
        true
    }

    async fn run(&self) {
        let (listener_tx, mut listener_rx) = mpsc::unbounded_channel();
        let clients = self.clients.clone();
        tokio::spawn(async move {
            let bnce_sender = listener_tx.clone();
            tokio::spawn(async move {
                println!("starting binance listener...");
                binance_ob::binance_ob_listener(&bnce_sender).await.unwrap();
            });

            let stmp_sender = listener_tx.clone();
            tokio::spawn(async move {
                println!("starting bitstamp listener...");
                bitstamp_ob::bitstamp_ob_listener(&stmp_sender).await.unwrap();
            });

            let mut summary = Summary {
                spread: 0.0,
                bids: vec![Level {exchange: String::from(""), price: 0.0, amount: 0.0,}; 10],
                asks: vec![Level {exchange: String::from(""), price: f64::MAX, amount: 0.0,}; 10],
            };
            while let Some(data) = listener_rx.recv().await {
                let exchange_ob: ExchangeOrderbook = bincode::deserialize::<ExchangeOrderbook>(&data).unwrap();
                let curr = MyServer::merge_incoming(&mut summary, &exchange_ob);

                if !MyServer::compare(&summary, &curr) {
                    summary = curr.clone();
                    let clients_connections = clients.lock().await;
                    for i in 0..clients_connections.len() {
                        let _ = clients_connections[i].send(curr.clone());
                    }
                }
            }
        });
    }

    fn merge_bids(summary_bids: &Vec<Level>, exchange_ob: &ExchangeOrderbook) -> Vec<Level> {
        let mut i = 0;
        let mut j = 0;
        let mut k = 0;
        let mut bids = vec![Level {exchange: String::from(""), price: 0.0, amount: 0.0,}; 10];
        while i < exchange_ob.bids.len() && j < summary_bids.len() && k < 10 {
            let incoming_price = exchange_ob.bids[i][0];
            let summary_price = summary_bids[j].price;
            if incoming_price > summary_price {
                bids[k] = Level {
                    exchange: exchange_ob.exchange.clone(),
                    price: incoming_price,
                    amount: exchange_ob.bids[i][1]
                };
                k = k + 1;
                i = i + 1;
            }
            else if incoming_price < summary_price {
                bids[k] = Level {
                    exchange: summary_bids[j].exchange.clone(),
                    price: summary_price,
                    amount: summary_bids[j].amount
                };
                k = k + 1;
                j = j + 1;
            }
            else if incoming_price == summary_price {
                if exchange_ob.exchange == summary_bids[j].exchange {
                    bids[k] = Level {
                        exchange: exchange_ob.exchange.clone(),
                        price: incoming_price,
                        amount: exchange_ob.bids[i][1]
                    };
                    k = k + 1;
                    i = i + 1;
                    j = j + 1;
                }
                else {
                    bids[k] = Level {
                        exchange: exchange_ob.exchange.clone(),
                        price: incoming_price,
                        amount: exchange_ob.bids[i][1]
                    };
                    k = k + 1;
                    i = i + 1;
                }
            }
        }
        bids
    }

    fn merge_asks(summary_asks: &Vec<Level>, exchange_ob: &ExchangeOrderbook) -> Vec<Level> {
        let mut i = 0;
        let mut j = 0;
        let mut k = 0;
        let mut asks = vec![Level {exchange: String::from(""), price: f64::MAX, amount: 0.0,}; 10];
        while i < exchange_ob.asks.len() && j < summary_asks.len() && k < 10 {
            let incoming_price = exchange_ob.asks[i][0];
            let summary_price = summary_asks[j].price;
            if incoming_price < summary_price {
                asks[k] = Level {
                    exchange: exchange_ob.exchange.clone(),
                    price: incoming_price,
                    amount: exchange_ob.asks[i][1]
                };
                k = k + 1;
                i = i + 1;
            }
            else if incoming_price > summary_price {
                asks[k] = Level {
                    exchange: summary_asks[j].exchange.clone(),
                    price: summary_price,
                    amount: summary_asks[j].amount
                };
                k = k + 1;
                j = j + 1;
            }
            else if incoming_price == summary_price {
                if exchange_ob.exchange == summary_asks[j].exchange {
                    asks[k] = Level {
                        exchange: exchange_ob.exchange.clone(),
                        price: incoming_price,
                        amount: exchange_ob.asks[i][1]
                    };
                    k = k + 1;
                    i = i + 1;
                    j = j + 1;
                }
                else {
                    asks[k] = Level {
                        exchange: exchange_ob.exchange.clone(),
                        price: incoming_price,
                        amount: exchange_ob.asks[i][1]
                    };
                    k = k + 1;
                    i = i + 1;
                }
            }
        }
        asks
    }

    fn merge_incoming(summary: &mut Summary, exchange_ob:  &ExchangeOrderbook) -> Summary {
        let bids: Vec<Level> = MyServer::merge_bids(&summary.bids, &exchange_ob);
        let asks: Vec<Level> = MyServer::merge_asks(&summary.asks, &exchange_ob);
        let spread = asks[0].price - bids[0].price;
        let ret = Summary {
            spread: spread,
            bids: bids,
            asks: asks,
        };
        ret
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for MyServer {

    type BookSummaryStream = UnboundedReceiverStream<Result<Summary, Status>>;

    async fn book_summary(&self, _request : Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
        let mut server_rx: mpsc::UnboundedReceiver<Summary> =  self.connect().await;
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(data) = server_rx.recv().await {
                if let Err(err) = tx.send(Ok(data.clone())) {
                    println!("ERROR: failed to update client stream: {:?}", err);
                    return;
                }
            }
        });

        let stream: Self::BookSummaryStream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(stream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    println!("starting server...");
    let addr = "0.0.0.0:50051".parse()?;
    let orderbook_aggregator_server = MyServer::new();
    orderbook_aggregator_server.run().await;
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook_aggregator_server))
        .serve(addr)
        .await?;

    Ok(())
}
