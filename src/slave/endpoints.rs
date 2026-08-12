pub mod endpoints{
    use axum::{Json, extract::{Path, State, Query}};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

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
    pub const INSERT_KVS: &str = "item/"; // Json body containing key value


    // This endpoint will be used in both directions even by the master where they will ask the version number and vnodes hashes(Assume orchestrator has a input file cotaining all the initial Node Ips and ports )
    // So when a slave Cache_Node gets this req they have to save the Ip/port they recieved the req from since it is the masters(This occurs during init)
    pub const COMM_MASTER: &str = "comm/"; // Json body to communicate between the Cache_Node and orchestrator

    #[derive(Deserialize)]
    pub struct TtlQuery {
        ttl: Option<usize>,
    }

    #[derive(Deserialize)]
    pub struct UpdateQuery {
        value: Option<String>,
        ttl: Option<usize>,
    }

    #[derive(Serialize)]
    pub struct KeyValueResponse {
        key: String,
        value: String,
    }

    #[derive(Deserialize)]
    pub struct CommMasterRequest {
        ip: String,
        port: u16,
    }

    pub async fn contains_key_handler(State(state): State<Rustis_Node>,Path(key):Path<String>) -> Result<Json<bool>, StatusCode>{
        let cache_net = state.cache.read().await;
        Ok(Json(cache_net.cache.contains_key(key)))
    }

    pub async fn contains_value_handler(State(state): State<Rustis_Node>,Path(value):Path<String>) -> Result<Json<Option<String>>, StatusCode>{
        let cache_net = state.cache.read().await;
        Ok(Json(cache_net.cache.contains_value(value)))
    }

    pub async fn get_keys_handler(State(state): State<Rustis_Node>) -> Result<Json<Vec<String>>, StatusCode>{
        let cache_net = state.cache.read().await;
        Ok(Json(cache_net.cache.get_keys()))
    }

    pub async fn get_values_handler(State(state): State<Rustis_Node>) -> Result<Json<Vec<String>>, StatusCode>{
        let cache_net = state.cache.read().await;
        Ok(Json(cache_net.cache.get_values()))
    }

    pub async fn get_kv_handler(State(state): State<Rustis_Node>) -> Result<Json<Vec<(String,String)>>, StatusCode>{
        let cache_net = state.cache.read().await;
        Ok(Json(cache_net.cache.get_items()))
    }

    pub async fn add_handler(State(state): State<Rustis_Node>,Path((key, value)): Path<(String, String)>,Query(ttl_query): Query<Option<usize>>) -> Result<Json<bool>, StatusCode>{
        let mut cache_net = state.cache.write().await;
        cache_net.cache.add(key, value, ttl_query);
        Ok(Json(true))
    }

    pub async fn get_handler(State(state): State<Rustis_Node>,Path(key):Path<String>) -> Result<Json<Option<String>>, StatusCode>{
        let cache_net = state.cache.read().await;
        Ok(Json(cache_net.cache.get(key)))
    }

    pub async fn delete_handler(State(state): State<Rustis_Node>,Path(key):Path<String>) -> Result<Json<bool>, StatusCode>{
        let mut cache_net = state.cache.write().await;
        cache_net.cache.delete(key);
        Ok(Json(true))
    }

    pub async fn update_handler(State(state): State<Rustis_Node>,Path(key): Path<String>,Query(update_query): Query<UpdateQuery>) -> Result<Json<bool>, StatusCode>{
        let mut cache_net = state.cache.write().await;
        let updated = cache_net.cache.update(key, update_query.value, update_query.ttl);
        Ok(Json(updated))
    }

    pub async fn health_handler() -> Result<Json<String>, StatusCode>{
        Ok(Json("OK".to_string()))
    }

    pub async fn insert_kvs_handler(
        State(state): State<Rustis_Node>,
        Json(kvs): Json<Vec<(String, String, Option<usize>)>>
    ) -> Result<Json<bool>, StatusCode>{
        let mut cache_net = state.cache.write().await;
        cache_net.cache.add_all(kvs);
        Ok(Json(true))
    }

    pub async fn comm_master_handler(State(state): State<Rustis_Node>,Json(req): Json<CommMasterRequest>) -> Result<Json<bool>, StatusCode>{
        let mut cache_net = state.cache.write().await;
        cache_net.save_orchestrator(req.ip, req.port);
        Ok(Json(true))
    }

}