use tokio::sync::mpsc::UnboundedSender;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::exchange_ob;
use exchange_ob::ExchangeOrderbook;

type BinanceOrderbookDataLevel = [String; 2];

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct BinanceOrderbook {
    lastUpdateId: i64,
    bids: Vec<BinanceOrderbookDataLevel>,
    asks: Vec<BinanceOrderbookDataLevel>,
}

pub fn binance_to_exchange_orderbook(json: &BinanceOrderbook, exchange_ob: &mut ExchangeOrderbook) {
    for i in 0..10 {
        if i < json.bids.len() {
            exchange_ob.bids[i][0] = json.bids[i][0].parse().unwrap();
            exchange_ob.bids[i][1] = json.bids[i][1].parse().unwrap();
        }
        else {
            exchange_ob.bids[i][0] = 0.0;
            exchange_ob.bids[i][1] = 0.0;
        }
    }
    for i in 0..10 {
        if i < json.asks.len() {
            exchange_ob.asks[i][0] = json.asks[i][0].parse().unwrap();
            exchange_ob.asks[i][1] = json.asks[i][0].parse().unwrap();
        }
        else {
            exchange_ob.asks[i][0] = f64::MAX;
            exchange_ob.asks[i][1] = 0.0;
        }
    }
}

pub fn format(data: &Vec<u8>, exchange_ob: &mut ExchangeOrderbook) -> bool {
    if data.is_empty() {
        return false;
    }
    let data_str = String::from_utf8(data.to_vec()).unwrap();
    let json: Value = serde_json::from_str(&data_str).unwrap();
    if json.get("lastUpdateId").is_some() {
        let json: BinanceOrderbook = serde_json::from_value(json).unwrap();
        binance_to_exchange_orderbook(&json, exchange_ob);
        return true;
    }
    false
}

pub async fn binance_ob_listener (tx: &UnboundedSender<ExchangeOrderbook>, pair: &str) -> Result<(), ()> {
    let url = url::Url::parse("wss://stream.binance.us:9443/ws").unwrap();
    //let url = url::Url::parse("wss://data-stream.binance.vision/stream?streams=btcusdt@trade").unwrap();
    let (ws_stream, _response) = connect_async(url).await.expect("Failed to connect");
    let (mut write, mut read) = ws_stream.split();

    let channel = "[\"".to_string() + pair + "@depth10@100ms" + "\"]";
    let subscribe_str = r#"{"id": 1, "method": "SUBSCRIBE", "params": "#.to_string()
                        + &channel + "}";
    write.send(Message::Text(subscribe_str)).await.unwrap();

    let mut exchange_ob = ExchangeOrderbook {
        exchange: "binance".to_string(),
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
