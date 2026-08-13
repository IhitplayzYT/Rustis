pub mod orchestrate{

    use std::{collections::{BTreeSet, HashMap, HashSet}, error::Error, hash::{DefaultHasher, Hash, Hasher}, net::IpAddr, sync::{Arc, LazyLock, RwLock}};
    use std::net::UdpSocket;

use tokio::sync::Mutex;

use crate::slave::endpoints::endpoints::{ADD, COMM_MASTER, CONTAINS_K, CONTAINS_V, CommMasterRequest, DELETE, GET, GET_KEYS, GET_KV, GET_VALUES, Topo, UPDATE};

    pub static NUMBERING: LazyLock<RwLock<usize>> = LazyLock::new(|| RwLock::new(0));

    #[derive(Debug,Clone)]
    pub struct Orchestrator{
        pub ring: Arc<Mutex<BTreeSet<usize>>>, // Ordered representation of the vnodes 
        pub peers: Arc<Mutex<HashMap<String,(String,u16)>>>, // Maps node name to connection route
        pub vnode_map: Arc<Mutex<HashMap<usize,String>>>,  // Maps vnode hash to node name
        pub port: u16
    }


    pub fn get_local_ip() -> std::io::Result<std::net::IpAddr> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect("8.8.8.8:80")?;
        Ok(socket.local_addr()?.ip())
    }



    impl Orchestrator{

        pub fn new(port: Option<u16>) -> Self{
            Self { ring: Arc::new(Mutex::new(BTreeSet::new())),peers:Arc::new(Mutex::new(HashMap::new())),vnode_map:Arc::new(Mutex::new(HashMap::new())),port:port.unwrap_or(8080)}
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
            let mut ring = self.ring.try_lock().unwrap();
            topo.vnodes.iter().for_each(|x| {ring.insert(*x);});
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
                      k = "[".to_string();
                      k += &m.iter().map(|x| format!("{:x}", x)).collect::<Vec<String>>().join(":");
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
                let mut ring = self.ring.try_lock().unwrap();
                topo.vnodes.iter().for_each(|x| {ring.insert(*x);});
            }
                Ok(())
            }
            
        // TODO: Add the get_items,contains_key,contains_value

        pub fn get_vnode(&self,k: &str) -> (String,u16){
            let mut hasher = DefaultHasher::new();
            k.hash(&mut hasher);
            let key_hash = hasher.finish() as usize;
            let ring = self.ring.try_lock().unwrap();
            let vnode = ring.range(key_hash..).next().copied().or_else(|| ring.iter().next().copied()).expect("Ring is Empty, please use add_peers followed by init_peers then retry");
            let node_name = (*self.vnode_map.try_lock().unwrap()).get(&vnode).expect("This cannot fail").to_string();
            (*self.peers.try_lock().unwrap()).get(&node_name).expect("No route associated with a Node").clone()
        }
        
       pub async fn add(&self,k:String,v: String,ttl: Option<usize>) -> bool{
            let dets = self.get_vnode(&k);
            let mut url = Orchestrator::build_url(&dets.0, dets.1);
            url += &ADD.replace("{key}", &k).replace("{value}", &v);
            let conn = reqwest::Client::new();
            let ret;
            if let Some(x) = ttl{
                ret = conn.post(url).query(&[("ttl",x)]).send().await.unwrap();
            }else{
                ret = conn.post(url).send().await.unwrap();
            }
            ret.json::<bool>().await.unwrap()
       }

       pub async fn add_kvs(&self,kvs: Vec<(String,String,Option<usize>)>) -> bool{
        let mut ret = true;
        for (k,v,ttl) in kvs{
            ret &= self.add(k, v, ttl).await;
        }
        ret
       }


       pub async fn update(&self,k:String,v: Option<String>,ttl: Option<usize>) -> bool{
            let dets = self.get_vnode(&k);
            let mut url = Orchestrator::build_url(&dets.0, dets.1);
            url += &UPDATE.replace("{key}", &k);
            let conn = reqwest::Client::new();
            let mut params= vec![];
            if let Some(ref value) = v {
                params.push(("value", value.clone()));
            }
            if let Some(ttl) = ttl {
                params.push(("ttl", ttl.to_string()));
            }

            let resp = conn.put(url).query(&params).send().await.unwrap();
            resp.json::<bool>().await.unwrap()
       }

        pub async fn contains_key(&self,k:&str) -> bool{
            let dets = self.get_vnode(k);
            let mut url = Orchestrator::build_url(&dets.0, dets.1);
            url += &CONTAINS_K.replace("{key}", k);
            let conn = reqwest::Client::new();
            let resp = conn.get(url).send().await.unwrap();
            resp.json::<bool>().await.unwrap()
       }

        pub async fn contains_value(&self,v:&str) -> bool{
            let vnodes = &*self.vnode_map.try_lock().unwrap();
            let uniq: HashSet<&String> = vnodes.iter().map(|(_,v)|{v}).collect::<Vec<&String>>().into_iter().collect();
            let conn = reqwest::Client::new();
            let peers =&*self.peers.try_lock().unwrap();
            for i in uniq{
                let rt = peers.get(i).expect("Not possible");
                let url = Orchestrator::build_url(&rt.0,rt.1) + &CONTAINS_V.replace("{value}", v);
                let resp = conn.get(url).send().await.unwrap();
                if resp.json::<bool>().await.unwrap(){
                    return true;
                }
            }
           false 
        }


        pub async fn get_keys(&self) -> Vec<String>{
            let vnodes = &*self.vnode_map.try_lock().unwrap();
            let uniq: HashSet<&String> = vnodes.iter().map(|(_,v)|{v}).collect::<Vec<&String>>().into_iter().collect();
            let conn = reqwest::Client::new();
            let mut ret = vec![];
            let peers =&*self.peers.try_lock().unwrap();
            for i in uniq{
                let rt = peers.get(i).expect("Not possible");
                let url = Orchestrator::build_url(&rt.0,rt.1) + &GET_KEYS;
                let resp = conn.get(url).send().await.unwrap();
                ret.append(&mut resp.json::<Vec<String>>().await.unwrap());
            }
            ret   
        }

        pub async fn get_values(&self) -> Vec<String>{
            let vnodes = &*self.vnode_map.try_lock().unwrap();
            let uniq: HashSet<&String> = vnodes.iter().map(|(_,v)|{v}).collect::<Vec<&String>>().into_iter().collect();
            let conn = reqwest::Client::new();
            let mut ret = vec![];
            let peers = &*self.peers.try_lock().unwrap();
            for i in uniq{
                let rt = peers.get(i).expect("Not possible");
                let url = Orchestrator::build_url(&rt.0,rt.1) + &GET_VALUES;
                let resp = conn.get(url).send().await.unwrap();
                ret.append(&mut resp.json::<Vec<String>>().await.unwrap());
            }
            ret   
        }

        pub async fn get_items(&self) -> Vec<(String,String)>{
            let vnodes = &*self.vnode_map.try_lock().unwrap();
            let uniq: HashSet<&String> = vnodes.iter().map(|(_,v)|{v}).collect::<Vec<&String>>().into_iter().collect();
            let conn = reqwest::Client::new();
            let mut ret = vec![];
            let peers = &*self.peers.try_lock().unwrap();
            for i in uniq{
                let rt = peers.get(i).expect("Not possible");
                let url = Orchestrator::build_url(&rt.0,rt.1) + &GET_KV;
                let resp = conn.get(url).send().await.unwrap();
                ret.append(&mut resp.json::<Vec<(String,String)>>().await.unwrap());
            }
            ret
        }


        pub async fn get(&self,k: &str) -> String{
            let dets = self.get_vnode(&k);
            let mut url = Orchestrator::build_url(&dets.0, dets.1);
            url += &GET.replace("{key}", k);
            let conn = reqwest::Client::new();
            let resp = conn.get(url).send().await.unwrap();
            resp.json::<String>().await.unwrap()
        }



        pub async fn delete(&self,k:&str) -> bool{
            let dets = self.get_vnode(&k);
            let mut url = Orchestrator::build_url(&dets.0, dets.1);
            url += &DELETE.replace("{key}", &k);
            let conn = reqwest::Client::new();
            let resp = conn.delete(url).send().await.unwrap();
            resp.json::<bool>().await.unwrap()
       }


    }





}