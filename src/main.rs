
use std::{error::Error, fs};

use axum::Router;

use crate::{helper::Helper::{CLI, Role}, slave::{dll::dll::{DLL, EvictionRegistry, fifo, lifo}, redundancy::redundancy::Rustis_Node}};

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

    match clargs.role{
        Role::Master => {



        },        
        Role::Slaves => {
            let mut policy = None;
            if let Some(x) =  clargs.evic_policy{
                policy = Some(registry.get(&x).expect("Unknown eviction policy"));
            }

            let rustis_node = Rustis_Node::new(clargs.cache_cap, policy);
            
            let router = Router::new();



        }
    }



    Ok(())
}
