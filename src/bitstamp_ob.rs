use tokio::sync::mpsc::UnboundedSender;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::exchange_ob;
use exchange_ob::ExchangeOrderbook;

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

pub fn bitstamp_to_exchange_orderbook(json: &BitstampOrderbook, exchange_ob: &mut ExchangeOrderbook) {
    for i in 0..10 {
        if i < json.data.bids.len() {
            exchange_ob.bids[i][0] = json.data.bids[i][0].parse().unwrap();
            exchange_ob.bids[i][1] = json.data.bids[i][1].parse().unwrap();
        }
        else {
            exchange_ob.bids[i][0] = 0.0;
            exchange_ob.bids[i][1] = 0.0;
        }
    }
    for i in 0..10 {
        if i < json.data.asks.len() {
            exchange_ob.asks[i][0] = json.data.asks[i][0].parse().unwrap();
            exchange_ob.asks[i][1] = json.data.asks[i][0].parse().unwrap();
        }
        else {
            exchange_ob.bids[i][0] = f64::MAX;
            exchange_ob.bids[i][1] = 0.0;
        }
    }
}

pub fn format(data: &Vec<u8>, exchange_ob: &mut ExchangeOrderbook) -> bool {
    if data.is_empty() {
        return false;
    }
    let data_str = String::from_utf8(data.to_vec()).unwrap();
    let json: Value = serde_json::from_str(&data_str).unwrap(); 
    if json.get("event").is_some() && json["event"] == "data" {
        let json: BitstampOrderbook = serde_json::from_value(json).unwrap();
        bitstamp_to_exchange_orderbook(&json, exchange_ob);
        return true;
    }
    false
}

pub async fn bitstamp_ob_listener (tx: &UnboundedSender<ExchangeOrderbook>, pair: &str) -> Result<(), ()> {
    let url = url::Url::parse("wss://ws.bitstamp.net").unwrap();
    let (ws_stream, _response) = connect_async(url).await.expect("Failed to connect");
    let (mut write, mut read) = ws_stream.split();

    let channel = "\"order_book_".to_string() + pair + "\"";
    let subscribe_str = r#"{"event": "bts:subscribe", "data": {"channel": "#.to_string()
                        + &channel + "}}";
    write.send(Message::Text(subscribe_str)).await.unwrap();

    let mut exchange_ob = ExchangeOrderbook {
        exchange: "bitstamp".to_string(),
        bids: vec![[0.0, 0.0]; 10],
        asks: vec![[f64::MAX, 0.0]; 10],
    };

    while let Some(msg) = read.next().await {
        let data = msg.unwrap().into_data();
        if format(&data, &mut exchange_ob) {
            let _ = tx.send(exchange_ob.clone());
        }
    }
    Ok(())
}
