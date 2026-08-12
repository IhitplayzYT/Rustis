pub mod orchestrate{

    use core::num;
use std::{collections::{BTreeSet, HashMap}, error::Error, sync::{LazyLock, RwLock}};

    static numbering: LazyLock<RwLock<usize>> = LazyLock::new(|| RwLock::new(0));

    #[derive(Debug)]
    pub struct Orchestrator{
        pub ring: BTreeSet<usize>, // Ordered representation of the vnodes 
        pub peers: HashMap<String,(String,u16)>, // Maps node name to connection route
        pub vnode_map: HashMap<usize,String>,  // Maps vnode hash to node name
    }

    impl Orchestrator{

        pub fn new() -> Self{
            Self { ring: BTreeSet::new(),peers:HashMap::new(),vnode_map:HashMap::new()}
        }

        pub fn load_peers(&mut self,fpth: &str) -> Result<(),Box<dyn Error>>{
            let file = std::fs::read_to_string(fpth)?;
            for i in file.split("\n"){
                let parts = i.trim().split(" ").collect::<Vec<&str>>();
                if parts.len() == 2{
                    self.peers.insert(format!("Node{}",*numbering.try_read()?),(parts[0].to_string(),parts[1].parse().expect("Port has to be unsigned 16 bit int")));
                    *numbering.try_write()? += 1;
                }else if parts.len() == 3{
                    self.peers.insert(parts[0].to_string(),(parts[1].to_string(),parts[2].parse().expect("Port has to be unsigned 16 bit int")));
                }else{}
            }
            Ok(())
        }

        pub fn init_peers(&mut self){
            // TODO: Consume the peers hashmap and build up the vnode_map and ring
        }

        // TODO: Add the add,update,delete,get_keys,get_values,get_items,contains_key,contains_value


    }





}