use serde::{Deserialize, Serialize};


pub type ExchangeOrderbookDataLevel = [f64; 2];

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
pub struct ExchangeOrderbook {
    pub exchange: String,
    pub bids: Vec<ExchangeOrderbookDataLevel>,
    pub asks: Vec<ExchangeOrderbookDataLevel>,
}
