use tokio::sync::mpsc::UnboundedSender;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::exchange_ob;
use exchange_ob::{ExchangeOrderbookDataLevel, ExchangeOrderbook};

type BitstampOrderbookDataLevel = [String; 2];

#[derive(Serialize, Deserialize)]
pub struct BitstampOrderbookData {
        timestamp: String,
        microtimestamp: String,
        bids: Vec<BitstampOrderbookDataLevel>,
        asks: Vec<BitstampOrderbookDataLevel>,
}

#[derive(Serialize, Deserialize)]
pub struct BitstampOrderbook {
    channel: String,
    data: BitstampOrderbookData,
    event: String,
}

pub fn bitstamp_to_exchange_orderbook(json: &BitstampOrderbook) -> ExchangeOrderbook {
    let mut bids: Vec<ExchangeOrderbookDataLevel> = Vec::new();
    let v = &json.data.bids;
    for i in 0..v.len() {
        let price: f64 = (&v[i][0]).parse().unwrap();
        let amount: f64 = (&v[i][1]).parse().unwrap();
        bids.push([price, amount]);
        if i == 9 {
            break;
        }
    }
    let mut asks: Vec<ExchangeOrderbookDataLevel> = Vec::new();
    let v = &json.data.asks;
    for i in 0..v.len() {
        let price: f64 = (&v[i][0]).parse().unwrap();
        let amount: f64 = (&v[i][1]).parse().unwrap();
        asks.push([price, amount]);
        if i == 9 {
            break;
        }
    }
    let exchange_orderbook = ExchangeOrderbook {
        exchange: String::from("bitstamp"),
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
    if json.get("event").is_some() && json["event"] == "data" {
        let json: BitstampOrderbook = serde_json::from_value(json).unwrap();
        let exchange_orderbook: ExchangeOrderbook = bitstamp_to_exchange_orderbook(&json);
        return Some(exchange_orderbook);
    }
    None
}

pub async fn bitstamp_ob_listener (tx: &UnboundedSender<ExchangeOrderbook>) -> Result<(), ()> {
    let url = url::Url::parse("wss://ws.bitstamp.net").unwrap();
    let (ws_stream, _response) = connect_async(url).await.expect("Failed to connect");
    let (mut write, read) = ws_stream.split();

    write.send(Message::Text(r#"{
        "event": "bts:subscribe",
        "data": {"channel": "order_book_btcusdt"}
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
