//use tokio::io::{AsyncWriteExt, Result};
use tokio::sync::mpsc::UnboundedSender;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};

pub async fn bitstamp_ob_listener (tx: &UnboundedSender<Vec<u8>>) -> Result<(), ()> {
    let url = url::Url::parse("wss://ws.bitstamp.net").unwrap();

    let (ws_stream, _response) = connect_async(url).await.expect("Failed to connect");
    println!("Bitstamp WebSocket handshake has been successfully completed");

    let (mut write, read) = ws_stream.split();

    println!("sending");

    write.send(Message::Text(r#"{
        "event": "bts:subscribe",
        "data": {"channel": "order_book_btcusd"}
      }"#.to_string()+"\n")).await.unwrap();

    println!("sent");

    let read_future = read.for_each(|message| async {
        println!("receiving...");
         let data = message.unwrap().into_data();
         //tokio::io::stdout().write(&data).await.unwrap();
         tx.send(data);
         println!("received...");
    });

    read_future.await;

    Ok(())
}
