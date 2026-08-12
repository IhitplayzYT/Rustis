pub mod endpoints{
    use axum::{Json, extract::{Path, State}};
use reqwest::StatusCode;

use crate::slave::redundancy::redundancy::Rustis_Node;


    pub const CONTAINS_K: &str = "/key/{key}";
    pub const CONTAINS_V: &str = "/value/{value}";
    pub const GET_KEYS: &str = "/key";
    pub const GET_VALUES: &str = "/value";
    pub const GET_KV: &str = "item/";
    pub const ADD: &str = "item/{key}/{value}"; // TTL can be a query param
    pub const GET: &str = "item/{key}";
    pub const DELETE: &str = "item/{key}";
    pub const UPDATE: &str = "item/{key}"; // value and TTL can be a query param(either atleats one has to be query param)
    pub const HEALTH: &str = "health";


    pub async fn contains_key_handler(State(state): State<Rustis_Node>,Path(key):Path<String>) -> Result<Json<bool>, StatusCode>{
        let cache_net = state.cache.read().await;
        Ok(Json(cache_net.master.contains_key(key)))
    }

}