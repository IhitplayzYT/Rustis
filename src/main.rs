
use std::error::Error;

use axum::{Router, routing::{get, post, delete, put}};
use std::sync::Arc;

use crate::{helper::Helper::{CLI, Role}, slave::{dll::dll::{EvictionRegistry, fifo, lifo, lru}, endpoints::endpoints::*, redundancy::redundancy::Rustis_Node}, master::{orchestrate::orchestrate::Orchestrator, endpoints::endpoints::*}};

mod helper;
mod slave;
mod master;


#[tokio::main]
async fn main() -> Result<(),Box<dyn Error>>{
    let mut clargs = CLI::new();
    clargs.Parse_Args();


    if clargs.dbg{
        println!("{clargs:?}");
    }
    let mut registry = EvictionRegistry::<String>::new();
    registry.register("fifo", fifo::<String>);
    registry.register("lifo", lifo::<String>);
    registry.register("lru", lru::<String>);

    match clargs.role{
        Role::Master => {
            let mut orchestrator = Orchestrator::new(Some(clargs.port));

            if let Err(e) = orchestrator.init_peers().await {
                eprintln!("Error initializing peers: {}", e);
            }

            let app = Router::new()
                .route(COMM_MASTER, post(handle_node_data))
                .route(TRANSFER, post(handle_node_change))
                .route(GET, get(handle_get))
                .route(ADD, post(handle_add))
                .route(INSERT_KVS, post(handle_add_kvs))
                .route(DELETE, delete(handle_delete))
                .route(UPDATE, put(handle_update))
                .route(CONTAINS_V, get(handle_contains_value))
                .route(CONTAINS_K, get(handle_contains_key))
                .route(GET_KEYS, get(handle_get_keys))
                .route(GET_VALUES, get(handle_get_values))
                .route(GET_KV, get(handle_get_items))
                .with_state(orchestrator);

            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", clargs.port)).await?;
            println!("Master server listening on port {}", clargs.port);
            axum::serve(listener, app).await?;
        },        
        Role::Slaves => {
            let mut policy = None;
            if let Some(x) =  clargs.evic_policy{
                policy = Some(registry.get(&x).expect("Unknown eviction policy"));
            }

            let rustis_node = Rustis_Node::new(clargs.cache_cap, policy,Some(clargs.port));
            let rustis_node_clone = rustis_node.cache.clone();

            let app = Router::new()
                .route(CONTAINS_K, get(contains_key_handler))
                .route(CONTAINS_V, get(contains_value_handler))
                .route(GET_KEYS, get(get_keys_handler))
                .route(GET_VALUES, get(get_values_handler))
                .route(GET_KV, get(get_kv_handler))
                .route(ADD, post(add_handler))
                .route(GET, get(get_handler))
                .route(DELETE, delete(delete_handler))
                .route(UPDATE, put(update_handler))
                .route(HEALTH, get(health_handler))
                .route(INSERT_KVS, post(insert_kvs_handler))
                .route(TRANSFER, post(handle_handover))
                .route(COMM_MASTER, post(comm_master_post_handler).get(comm_master_get_handler))
                .with_state(rustis_node);

            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", clargs.port)).await?;
            println!("Slave server listening on port {}", clargs.port);

            // Spawn TTL decrement task
            let ttl_task = async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    let mut cache_net = rustis_node_clone.write().await;
                    let keys_to_remove: Vec<String> = cache_net.cache.get_keys();
                    for key in keys_to_remove {
                        if let Some(status) = cache_net.cache.decr_ttl(key.clone()) {
                            use crate::slave::cache::cache::s_TTL;
                            if matches!(status, s_TTL::Dead) {
                                println!("[TTL] Key {} expired, removing", key);
                                cache_net.cache.delete(key);
                            }
                        }
                    }
                }
            };

            // Run both the server and TTL task
            tokio::select! {
                _ = axum::serve(listener, app) => {},
                _ = ttl_task => {},
                _ = tokio::signal::ctrl_c() => {println!("Received shutdown signal");}
            }
        }
    }



    Ok(())
}
