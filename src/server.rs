use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use std::env;
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
    clients: Arc<Mutex<HashMap<i64, mpsc::UnboundedSender<Summary>>>>,
    client_id: Arc<Mutex<i64>>,
    pair: String,
}

impl MyServer {
    fn new(instrument: String) -> Self {
        MyServer {
            clients: Arc::new(Mutex::new(HashMap::<i64,
                                         mpsc::UnboundedSender<Summary>>::new())),
            client_id: Arc::new(Mutex::new(0)),
            pair: instrument,
        }
    }

    async fn add_client(&self) -> (i64, mpsc::UnboundedReceiver<Summary>) {
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let mut id = self.client_id.lock().await;
        *id += 1;
        let cid: i64 = *id;
        drop(id);
        let mut clients_connections = self.clients.lock().await;
        clients_connections.insert(cid, client_tx);
        drop(clients_connections);
        println!("added client id {}", cid);
        (cid, client_rx)
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
        let pair = self.pair.clone();
        tokio::spawn(async move {
            let bnce_pair = pair.clone();
            let bnce_sender = listener_tx.clone();
            tokio::spawn(async move {
                println!("starting binance listener...");
                binance_ob::binance_ob_listener(&bnce_sender, &bnce_pair).await.unwrap();
            });

            let stmp_pair = pair.clone();
            let stmp_sender = listener_tx.clone();
            tokio::spawn(async move {
                println!("starting bitstamp listener...");
                bitstamp_ob::bitstamp_ob_listener(&stmp_sender, &stmp_pair).await.unwrap();
            });

            let mut prev_summary = Summary {
                spread: 0.0,
                bids: vec![Level {exchange: String::from(""), price: 0.0, amount: 0.0,}; 10],
                asks: vec![Level {exchange: String::from(""), price: f64::MAX, amount: 0.0,}; 10],
            };
            let mut curr_summary = Summary {
                spread: 0.0,
                bids: vec![Level {exchange: String::from(""), price: 0.0, amount: 0.0,}; 10],
                asks: vec![Level {exchange: String::from(""), price: f64::MAX, amount: 0.0,}; 10],
            };
            let mut binance_ob = ExchangeOrderbook {
                exchange: String::from("binance"),
                bids: vec![[0.0, 0.0]; 10],
                asks: vec![[f64::MAX, 0.0]; 10],
            };
            let mut bitstamp_ob = ExchangeOrderbook {
                exchange: String::from("bitstamp"),
                bids: vec![[0.0, 0.0]; 10],
                asks: vec![[f64::MAX, 0.0]; 10],
            };
            while let Some(exchange_ob) = listener_rx.recv().await {
                if exchange_ob.exchange == "binance" {
                    binance_ob = exchange_ob;
                }
                else if exchange_ob.exchange == "bitstamp" {
                    bitstamp_ob = exchange_ob;
                }
                else {
                    println!("ERROR: unsupported exchange name");
                    return;
                }
                MyServer::merge_orderbooks(&binance_ob, &bitstamp_ob, &mut curr_summary);
                if !MyServer::compare(&prev_summary, &curr_summary) {
                    prev_summary = curr_summary.clone();
                    let clients_connections = clients.lock().await;
                    for tx in clients_connections.values() {
                        let _ = tx.send(curr_summary.clone());
                    }
                }
            }
        });
    }

    fn merge_bids(ob1: &ExchangeOrderbook, ob2: &ExchangeOrderbook, bids: &mut Vec<Level>) {
        let mut i = 0;
        let mut j = 0;
        let mut k = 0;
        while i < ob1.bids.len() && j < ob2.bids.len() && k < 10 {
            let ob1_price = ob1.bids[i][0];
            let ob2_price = ob2.bids[j][0];
            if ob1_price > ob2_price {
                bids[k].exchange = ob1.exchange.clone();
                bids[k].price = ob1_price;
                bids[k].amount = ob1.bids[i][1];
                k += 1; i += 1;
            }
            else if ob1_price < ob2_price {
                bids[k].exchange = ob2.exchange.clone();
                bids[k].price = ob2_price;
                bids[k].amount = ob2.bids[i][1];
                k += 1; j += 1;
            }
            else if ob1_price == ob2_price {
                bids[k].exchange = ob1.exchange.clone();
                bids[k].price = ob1_price;
                bids[k].amount = ob1.bids[i][1];
                k += 1; i += 1;
                if k < 10 {
                    bids[k].exchange = ob2.exchange.clone();
                    bids[k].price = ob2_price;
                    bids[k].amount = ob2.bids[i][1];
                    k += 1; j += 1;
                }
            }
        }
    }

    fn merge_asks(ob1: &ExchangeOrderbook, ob2: &ExchangeOrderbook, asks: &mut Vec<Level>) {
        let mut i = 0;
        let mut j = 0;
        let mut k = 0;
        while i < ob1.asks.len() && j < ob2.asks.len() && k < 10 {
            let ob1_price = ob1.asks[i][0];
            let ob2_price = ob2.asks[j][0];
            if ob1_price < ob2_price {
                asks[k].exchange = ob1.exchange.clone();
                asks[k].price = ob1_price;
                asks[k].amount = ob1.asks[i][1];
                k += 1; i += 1;
            }
            else if ob1_price > ob2_price {
                asks[k].exchange = ob2.exchange.clone();
                asks[k].price = ob2_price;
                asks[k].amount = ob2.asks[i][1];
                k += 1; j += 1;
            }
            else if ob1_price == ob2_price {
                asks[k].exchange = ob1.exchange.clone();
                asks[k].price = ob1_price;
                asks[k].amount = ob1.asks[i][1];
                k += 1; i += 1;
                if k < 10 {
                    asks[k].exchange = ob2.exchange.clone();
                    asks[k].price = ob2_price;
                    asks[k].amount = ob2.asks[i][1];
                    k += 1; j += 1;
                }
            }
        }
    }

    fn merge_orderbooks(ob1:  &ExchangeOrderbook, ob2:  &ExchangeOrderbook, summary: &mut Summary) {
        MyServer::merge_bids(&ob1, &ob2, &mut summary.bids);
        MyServer::merge_asks(&ob1, &ob2, &mut summary.asks);
        summary.spread = summary.asks[0].price - summary.bids[0].price;
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for MyServer {

    type BookSummaryStream = UnboundedReceiverStream<Result<Summary, Status>>;

    async fn book_summary(&self, _request : Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
        let (id, mut client_rx) =  self.add_client().await;

        let (tx, rx) = mpsc::unbounded_channel();

        let clients = self.clients.clone();
        tokio::spawn(async move {
            while let Some(data) = client_rx.recv().await {
                if let Err(err) = tx.send(Ok(data)) {
                    let mut clients_connections = clients.lock().await;
                    clients_connections.remove(&id);
                    drop(clients_connections);
                    println!("ERROR: failed to update client stream for client id {}: {:?}", id, err);
                    println!("deleted client id {}", id);
                    break;
                }
            }
        });

        let stream: Self::BookSummaryStream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(stream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pair = "btcusd".to_string();
    if let Some(arg) = env::args().nth(1) {
        pair = arg;
    }
    println!("starting server for {}...", pair);
    let addr = "0.0.0.0:50051".parse()?;
    let orderbook_aggregator_server = MyServer::new(pair);
    orderbook_aggregator_server.run().await;
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook_aggregator_server))
        .serve(addr)
        .await?;

    Ok(())
}
