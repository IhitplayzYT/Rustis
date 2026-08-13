pub mod endpoints{
    use axum::{Json, extract::{Path, Query, State}};
use reqwest::StatusCode;

use crate::{ master::orchestrate::orchestrate::Orchestrator, slave::endpoints::endpoints::{Handover, Topo, UpdateQuery}};


        pub async fn handle_node_data(State(orch):State<Orchestrator>,Json(topo):Json<Topo>){
            let n_m = topo.name;
            let mut vnode_map = orch.vnode_map.lock().await;
            topo.vnodes.iter().for_each(|x| {vnode_map.insert(*x, n_m.clone());});
        }

        // Handles Routing name_node to the following node will use /transfer endpoint only
        pub async fn handle_node_change(State(orch):State<Orchestrator>,Json(handover):Json<Handover>){
            let n_m = &handover.name;
            let vnode_map = orch.vnode_map.lock().await;
            let ids = vnode_map.iter().filter_map(|(k,v)| if v == n_m{Some(*k)}else{None}).collect::<Vec<usize>>();
            drop(vnode_map);
            
            let mut ring = orch.ring.lock().await;
            ids.iter().for_each(|x| {ring.remove(x);});
            drop(ring);
            
            let mut vnode_map = orch.vnode_map.lock().await;
            ids.iter().for_each(|x| {vnode_map.remove(x);});
            // All vnodes removed by here

            // Added the name map and ring
            let mut peers = orch.peers.lock().await;
            peers.insert(handover.name.clone(),(handover.route.ip.clone(),handover.route.port));
            drop(peers);
            
            // Update the vnodes and 
            let conn = reqwest::Client::new();
            let ret = conn.get(Orchestrator::build_url(&handover.route.ip, handover.route.port)).send().await.unwrap();
            let topo = ret.json::<Topo>().await.unwrap();
            let mut vnode_map = orch.vnode_map.lock().await;
            topo.vnodes.iter().for_each(|x| {vnode_map.insert(*x, topo.name.clone());});
            let mut ring = orch.ring.lock().await;
            topo.vnodes.iter().for_each(|x| {ring.insert(*x);});
        }

        pub async fn handle_get(State(orch): State<Orchestrator>,Path(key):Path<String>) -> Result<Json<String>,StatusCode>{
            Ok(Json(orch.get(&key).await))
        }

        pub async fn handle_add(State(orch): State<Orchestrator>,Path((key,value)):Path<(String,String)>,Query(ttl):Query<Option<usize>>) -> Result<Json<bool>,StatusCode>{
            Ok(Json(orch.add(key, value, ttl).await))
        }
        pub async fn handle_add_kvs(State(orch): State<Orchestrator>,Json(kvs):Json<Vec<(String,String,Option<usize>)>>) -> Result<Json<bool>,StatusCode>{
            Ok(Json(orch.add_kvs(kvs).await))
        }

        pub async fn handle_delete(State(orch): State<Orchestrator>,Path(key):Path<String>) -> Result<Json<bool>,StatusCode>{
            Ok(Json(orch.delete(&key).await))
        }

        pub async fn handle_update(State(orch): State<Orchestrator>,Path(key): Path<String>,Query(update_query): Query<UpdateQuery>) -> Result<Json<bool>, StatusCode>{
            Ok(Json(orch.update(key,update_query.value,update_query.ttl).await))
        }

        pub async fn handle_contains_value(State(orch): State<Orchestrator>,Path(value): Path<String>) -> Result<Json<bool>,StatusCode>{
            Ok(Json(orch.contains_value(&value).await))
        }
        
        pub async fn handle_contains_key(State(orch): State<Orchestrator>,Path(key): Path<String>) -> Result<Json<bool>,StatusCode>{
            Ok(Json(orch.contains_key(&key).await))
        }

        pub async fn handle_get_keys(State(orch): State<Orchestrator>) -> Result<Json<Vec<String>>,StatusCode>{
            Ok(Json(orch.get_keys().await))
        }

        pub async fn handle_get_values(State(orch): State<Orchestrator>) -> Result<Json<Vec<String>>,StatusCode>{
            Ok(Json(orch.get_values().await))
        }

        pub async fn handle_get_items(State(orch): State<Orchestrator>) -> Result<Json<Vec<(String,String)>>,StatusCode>{
            Ok(Json(orch.get_items().await))
        }

    
    
    




}