use std::collections::HashMap;

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
    clients: Arc<Mutex<HashMap<i64, mpsc::UnboundedSender<Summary>>>>,
    client_id: Arc<Mutex<i64>>,
}

impl MyServer {
    fn new() -> Self {
        MyServer {
            clients: Arc::new(Mutex::new(HashMap::<i64,
                                         mpsc::UnboundedSender<Summary>>::new())),
            client_id: Arc::new(Mutex::new(0)),
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

    fn print_summary(summary: &Summary) {
        print!("{}spread: {} bids: [", "{", summary.spread);
        for i in 0..summary.bids.len() {
            print!("[{}, {}, {}], ", summary.bids[i].exchange, summary.bids[i].price, summary.bids[i].amount);
        }
        print!("] asks: [");
        for i in 0..summary.asks.len() {
            print!("[{}, {}, {}], ", summary.asks[i].exchange, summary.asks[i].price, summary.asks[i].amount);
        }
        println!("]{}", "}");
    }

    fn print_exchange_ob(exchange_ob: &ExchangeOrderbook) {
        print!("{}exchange: {} bids: [", "{", exchange_ob.exchange);
        for i in 0..exchange_ob.bids.len() {
            print!("[{}, {}], ", exchange_ob.bids[i][0], exchange_ob.bids[i][1]);
        }
        print!("] asks: [");
        for i in 0..exchange_ob.asks.len() {
            print!("[{}, {}], ", exchange_ob.asks[i][0], exchange_ob.asks[i][1]);
        }
        println!("]{}", "}");
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
            while let Some(data) = listener_rx.recv().await {
                let exchange_ob: ExchangeOrderbook = bincode::deserialize::<ExchangeOrderbook>(&data).unwrap();
                MyServer::print_summary(&summary);
                MyServer::print_exchange_ob(&exchange_ob);
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
                let curr = MyServer::merge_orderbooks(&binance_ob, &bitstamp_ob);
                if !MyServer::compare(&summary, &curr) {
                    summary = curr.clone();
                    println!("run(): acquiring lock ...");
                    let clients_connections = clients.lock().await;
                    for tx in clients_connections.values() {
                        let _ = tx.send(curr.clone());
                    }
                    drop(clients_connections);
                    println!("run(): dropped lock ...");
                }
            }
        });
    }

    fn merge_bids(ob1: &ExchangeOrderbook, ob2: &ExchangeOrderbook) -> Vec<Level> {
        let mut i = 0;
        let mut j = 0;
        let mut k = 0;
        let mut bids = vec![Level {exchange: String::from(""), price: 0.0, amount: 0.0,}; 10];
        while i < ob1.bids.len() && j < ob2.bids.len() && k < 10 {
            let ob1_price = ob1.bids[i][0];
            let ob2_price = ob2.bids[j][0];
            if ob1_price > ob2_price {
                bids[k] = Level {
                    exchange: ob1.exchange.clone(),
                    price: ob1_price,
                    amount: ob1.bids[i][1]
                };
                k += 1; i += 1;
            }
            else if ob1_price < ob2_price {
                bids[k] = Level {
                    exchange: ob2.exchange.clone(),
                    price: ob2_price,
                    amount: ob2.bids[j][1]
                };
                k += 1; j += 1;
            }
            else if ob1_price == ob2_price {
                bids[k] = Level {
                    exchange: ob1.exchange.clone(),
                    price: ob1_price,
                    amount: ob1.bids[i][1]
                };
                k += 1; i += 1;
                if k < 10 {
                    bids[k] = Level {
                        exchange: ob2.exchange.clone(),
                        price: ob2_price,
                        amount: ob2.bids[j][1]
                    };
                    k += 1; j += 1;
                }
            }
        }
        bids
    }

    fn merge_asks(ob1: &ExchangeOrderbook, ob2: &ExchangeOrderbook) -> Vec<Level> {
        let mut i = 0;
        let mut j = 0;
        let mut k = 0;
        let mut asks = vec![Level {exchange: String::from(""), price: 0.0, amount: 0.0,}; 10];
        while i < ob1.asks.len() && j < ob2.asks.len() && k < 10 {
            let ob1_price = ob1.asks[i][0];
            let ob2_price = ob2.asks[j][0];
            if ob1_price < ob2_price {
                asks[k] = Level {
                    exchange: ob1.exchange.clone(),
                    price: ob1_price,
                    amount: ob1.asks[i][1]
                };
                k += 1; i += 1;
            }
            else if ob1_price > ob2_price {
                asks[k] = Level {
                    exchange: ob2.exchange.clone(),
                    price: ob2_price,
                    amount: ob2.asks[j][1]
                };
                k += 1; j += 1;
            }
            else if ob1_price == ob2_price {
                asks[k] = Level {
                    exchange: ob1.exchange.clone(),
                    price: ob1_price,
                    amount: ob1.asks[i][1]
                };
                k += 1; i += 1;
                if k < 10 {
                    asks[k] = Level {
                        exchange: ob2.exchange.clone(),
                        price: ob2_price,
                        amount: ob2.asks[j][1]
                    };
                    k += 1; j += 1;
                }
            }
        }
        asks
    }

    fn merge_orderbooks(ob1:  &ExchangeOrderbook, ob2:  &ExchangeOrderbook) -> Summary {
        let bids: Vec<Level> = MyServer::merge_bids(&ob1, &ob2);
        let asks: Vec<Level> = MyServer::merge_asks(&ob1, &ob2);
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
