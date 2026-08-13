pub mod endpoints{
    use axum::{Json, extract::State};

use crate::{master::orchestrate::orchestrate::Orchestrator, slave::endpoints::endpoints::{Handover, Topo}};


        pub async fn handle_node_data(State(orch):State<Orchestrator>,Json(topo):Json<Topo>){
            let n_m = topo.name;
            topo.vnodes.iter().for_each(|x| {(*orch.vnode_map.try_lock().unwrap()).insert(*x, n_m.clone());});
        }

        // Handles Routing name_node to the following node
        pub async fn handle_node_change(State(mut orch):State<Orchestrator>,Json(handover):Json<Handover>){
            let n_m = &handover.name;
            let ids = (*orch.vnode_map.try_lock().unwrap()).iter().filter_map(|(k,v)| if v == n_m{Some(*k)}else{None}).collect::<Vec<usize>>();
            ids.iter().for_each(|x| {orch.ring.remove(x);});
            ids.iter().for_each(|x| {(*orch.vnode_map.try_lock().unwrap()).remove(x);});

            // All vnodes removed

            // Added the name map and ring
            (*orch.peers.try_lock().unwrap()).insert(handover.name,(handover.route.ip.clone(),handover.route.port));
            // Update the vnodes and 
            let conn = reqwest::Client::new();
            let ret = conn.get(Orchestrator::build_url(&handover.route.ip, handover.route.port)).send().await.unwrap();
            orch.handle_topo(ret.json::<Topo>().await.unwrap());
        }

    




}