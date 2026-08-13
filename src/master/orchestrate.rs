pub mod orchestrate{

    use std::{collections::{BTreeSet, HashMap}, error::Error, net::IpAddr, sync::{Arc, LazyLock, RwLock}};
    use std::net::UdpSocket;

use tokio::sync::Mutex;

use crate::slave::endpoints::endpoints::{COMM_MASTER, CommMasterRequest, Topo};

    pub static NUMBERING: LazyLock<RwLock<usize>> = LazyLock::new(|| RwLock::new(0));

    #[derive(Debug)]
    pub struct Orchestrator{
        pub ring: BTreeSet<usize>, // Ordered representation of the vnodes 
        pub peers: Arc<Mutex<HashMap<String,(String,u16)>>>, // Maps node name to connection route
        pub vnode_map: Arc<Mutex<HashMap<usize,String>>>,  // Maps vnode hash to node name
        pub port: u16
    }


    fn get_local_ip() -> std::io::Result<std::net::IpAddr> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect("8.8.8.8:80")?;
        Ok(socket.local_addr()?.ip())
    }



    impl Orchestrator{

        pub fn new(port: Option<u16>) -> Self{
            Self { ring: BTreeSet::new(),peers:Arc::new(Mutex::new(HashMap::new())),vnode_map:Arc::new(Mutex::new(HashMap::new())),port:port.unwrap_or(8080)}
        }

        pub fn get_new_name(&self) -> String{
            let ret = format!("Node{}",*NUMBERING.try_read().unwrap());
            *NUMBERING.try_write().unwrap() += 1;
            ret
        }

        pub fn load_peers(&mut self,fpth: &str) -> Result<(),Box<dyn Error>>{
            let file = std::fs::read_to_string(fpth)?;
            for i in file.split("\n"){
                let parts = i.trim().split(" ").collect::<Vec<&str>>();
                if parts.len() == 2{
                    (*self.peers.try_lock().unwrap()).insert(format!("Node{}",*NUMBERING.try_read()?),(parts[0].to_string(),parts[1].parse().expect("Port has to be unsigned 16 bit int")));
                    *NUMBERING.try_write()? += 1;
                }else if parts.len() == 3{
                    (*self.peers.try_lock().unwrap()).insert(parts[0].to_string(),(parts[1].to_string(),parts[2].parse().expect("Port has to be unsigned 16 bit int")));
                }else{}
            }
            Ok(())
        }

        pub fn build_url(ip: &str,port: u16) -> String{
            format!("http://{ip}:{port}",)
        }

        pub fn handle_topo(&mut self,topo: Topo){
            topo.vnodes.iter().for_each(|x| {(*self.vnode_map.try_lock().unwrap()).insert(*x, topo.name.clone());});
            topo.vnodes.iter().for_each(|x| {self.ring.insert(*x);});
        }


        pub async fn init_peers(&mut self) -> Result<(),Box<dyn Error>>{
            let my_ip = get_local_ip().unwrap();
                let mut k;  // Our Ip
                match my_ip{
                    IpAddr::V4(x) => {
                      k = x.octets().map(|x| x.to_string()).join(".");
                    },
                    IpAddr::V6(y) => {
                      let m = y.segments();
                      let mut k_32 = vec![];
                      for i in 0..4{
                        k_32.push(m[2*i] as u32 + m[2*i+1] as u32);
                      }
                      k = "[".to_string();
                      k += &k_32.iter().map(|x| format!("{:x}",x)).collect::<Vec<String>>().join(":");
                      k += "]"
                    }
                }

            let conn = reqwest::Client::new();

            for (name,route) in &*self.peers.try_lock().unwrap(){
                let mut url = Orchestrator::build_url(&route.0, route.1);
                url += COMM_MASTER;
                let p = CommMasterRequest{ip:k.clone(),port:self.port,name:name.to_string()};
                conn.post(&url).json(&p).send().await.unwrap();

                let resp = conn.get(&url).send().await.unwrap();
                let topo = resp.json::<Topo>().await.unwrap();
                topo.vnodes.iter().for_each(|x| {(*self.vnode_map.try_lock().unwrap()).insert(*x, topo.name.clone());});
                topo.vnodes.iter().for_each(|x| {self.ring.insert(*x);});
            }
                Ok(())
            }
            
        // TODO: Add the add,update,delete,get_keys,get_values,get_items,contains_key,contains_value

        


    }





}