use tokio::sync::mpsc::UnboundedSender;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::exchange_ob;
use exchange_ob::{ExchangeOrderbookDataLevel, ExchangeOrderbook};

type BinanceOrderbookDataLevel = [String; 2];

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct BinanceOrderbook {
    lastUpdateId: i64,
    bids: Vec<BinanceOrderbookDataLevel>,
    asks: Vec<BinanceOrderbookDataLevel>,
}

pub fn binance_to_exchange_orderbook(json: &BinanceOrderbook) -> ExchangeOrderbook {
    let mut bids: Vec<ExchangeOrderbookDataLevel> = Vec::new();
    let v = &json.bids;
    for i in 0..v.len() {
        let price: f64 = (&v[i][0]).parse().unwrap();
        let amount: f64 = (&v[i][1]).parse().unwrap();
        bids.push([price, amount]);
        if i == 9 {
            break;
        }
    }
    let mut asks: Vec<ExchangeOrderbookDataLevel> = Vec::new();
    let v = &json.asks;
    for i in 0..v.len() {
        let price: f64 = (&v[i][0]).parse().unwrap();
        let amount: f64 = (&v[i][1]).parse().unwrap();
        asks.push([price, amount]);
        if i == 9 {
            break;
        }
    }
    let exchange_orderbook = ExchangeOrderbook {
        exchange: String::from("binance"),
        bids: bids,
        asks: asks
    };
    return exchange_orderbook;
}


pub fn format(data: &Vec<u8>) -> Option<ExchangeOrderbook> {
    if data.is_empty() {
        return None;
    }
    let data_str = String::from_utf8(data.to_vec()).unwrap();
    let json: Value = serde_json::from_str(&data_str).unwrap();
    if json.get("lastUpdateId").is_some() {
        let json: BinanceOrderbook = serde_json::from_value(json).unwrap();
        let exchange_orderbook: ExchangeOrderbook = binance_to_exchange_orderbook(&json);
        return Some(exchange_orderbook);
    }
    None
}

pub async fn binance_ob_listener (tx: &UnboundedSender<ExchangeOrderbook>) -> Result<(), ()> {
    let url = url::Url::parse("wss://stream.binance.us:9443/ws").unwrap();
    //let url = url::Url::parse("wss://data-stream.binance.vision/stream?streams=btcusdt@trade").unwrap();
    let (ws_stream, _response) = connect_async(url).await.expect("Failed to connect");
    let (mut write, read) = ws_stream.split();

    write.send(Message::Text(r#"{
        "id": 1,
        "method": "SUBSCRIBE",
        "params": ["btcusd@depth10@100ms"]
    }"#.to_string()+"\n")).await.unwrap();

    let read_future = read.for_each(|message| async {
        let data = message.unwrap().into_data();
        match format(&data) {
            Some(data) => {
                let _ = tx.send(data);
            },
            None => (),
        }
    });
    read_future.await;
    Ok(())
}
