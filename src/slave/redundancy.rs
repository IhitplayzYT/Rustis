pub mod redundancy{
    use crate::{helper::Helper::{HIGH, LOW}, slave::{cache::cache::Cache, dll::dll::DLL}};
    use tokio::sync::RwLock;
use xxhash_rust::xxh3::xxh3_64;
    use std::{collections::{BTreeSet, BinaryHeap}, sync::Arc};
    use rand::{Rng, distr::Alphanumeric};

    #[derive(Debug,Clone,PartialEq, Eq)]
    pub enum Priority{
        Low,
        Default,
        Medium,
        High
    }    

    fn hash(input: &str) -> usize {
        xxh3_64(input.as_bytes()) as usize
    }


    fn random_salt(mn:usize,mx:usize) -> String {
        let mut rng = rand::rng();
        let len = rng.random_range(mn..=mx);
        (&mut rng).sample_iter(Alphanumeric).take(len).map(char::from).collect()
    }

    impl Ord for Priority{
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            let rank =|p: &Priority| match p{
                Priority::Low => 0,
                Priority::Default => 1,
                Priority::Medium => 2,
                Priority::High => 3
            };
            rank(self).cmp(&rank(other))
        }

        fn min(self, other: Self) -> Self
        where
            Self: Sized,
        {
            let rank =|p: &Priority| match p{
                Priority::Low => 0,
                Priority::Default => 1,
                Priority::Medium => 2,
                Priority::High => 3
            };
            
            if rank(&self) >= rank(&other){
                self
            }else{
                other
            }

        }

        fn max(self, other: Self) -> Self where Self: Sized
        {
            let rank =|p: &Priority| match p{
                Priority::Low => 0,
                Priority::Default => 1,
                Priority::Medium => 2,
                Priority::High => 3
            };
            
            if rank(&self) >= rank(&other){
                other 
            }else{
                self
            }
        } 

    }

    impl PartialOrd for Priority{
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }


    impl Ord for Routes{
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.prio.cmp(&other.prio)
        }
    }
    
    impl PartialOrd for Routes{
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.prio.cmp(&other.prio))
        }
    }


    #[derive(Debug,Clone,PartialEq, Eq)]
    pub struct Routes{
        ip: String,
        port: u16,
        prio: Priority,
    }

    impl Routes{
        pub fn new(ip:String,port:u16,prio:Option<Priority>) -> Self{
            Self { ip, port, prio: prio.unwrap_or(Priority::Default)}
        }

        pub fn get_url(&self) -> String{
            format!("http://{}:{}", self.ip, self.port)
        }
    }



    #[derive(Debug)]
    pub struct Cache_Net{
        pub cache : Cache,
        pub vnodes: BTreeSet<usize>,
        pub redundancies: BinaryHeap<Routes>,
        pub is_down:bool,
        pub orchestrator: Option<Routes>,
    }

    
    impl Cache_Net{
        pub fn new(cap:Option<usize>,evic: Option<fn (&mut DLL<String>) -> Option<String>>) -> Self{
            Self { cache: Cache::new(cap, evic), redundancies: BinaryHeap::new(),vnodes:BTreeSet::new(),is_down:false,orchestrator:None}
        }

        pub fn save_orchestrator(&mut self,ip: String,port:u16){
            self.orchestrator = Some(Routes { ip, port, prio: Priority::High }) // Prio is useless here since mastyer will always be given priority
        }

        pub fn add_vnodes(&mut self,n:usize){
            // Weird Ahh hash fxn i cooked up
            let mut rng = rand::rng();
            let mut i = 0;
            let (l,h) = (*LOW.try_read().unwrap(),*HIGH.try_read().unwrap());
            while i < n{
                let mn = rng.random_range(l..h);
                let mut mx = rng.random_range(l..=h);
                while mx < mn{
                    mx = rng.random_range(l..=h)
                }
                let gen_bits = rng.random_range(1_u8..=7_u8);
                let mut hashable_str = "".to_string();
                if gen_bits & (1 << 0) != 0{
                    hashable_str += &format!("{:?}",self.cache);
                }
                if gen_bits & (1 << 1) != 0{
                    hashable_str += &format!("{:?}",self.vnodes);
                }
                if gen_bits & (1 << 2) != 0{
                    hashable_str += &format!("{:?}",self.redundancies);
                }
                hashable_str += &random_salt(mn,mx);
                let hsh = hash(&hashable_str);
                if !self.vnodes.contains(&hsh){
                    self.vnodes.insert(hsh);
                    i += 1;
                }

            }
        }

        pub fn add_redundancies(&mut self,routes: Vec<(String,u16,Option<Priority>)>) {
            routes.iter().for_each(|x| {self.redundancies.push(Routes::new(x.0.clone(), x.1, x.2.clone()));});
        }

        fn get_next_highest_priority(&self) -> Option<Routes> {
            let mut heap: BinaryHeap<Routes> = self.redundancies.clone();
            heap.pop()
        }

        pub async fn send_data(&self) -> Result<(), Box<dyn std::error::Error>> {
            if let Some(target) = self.get_next_highest_priority() {
                let target_url = target.get_url();
                
                if reqwest::get(&format!("{}/health", target_url)).await?.status().is_success() {
                    let kvs: Vec<(String, String, Option<usize>)> = self.cache.get_items().into_iter().map(|(k, v)| (k.clone(), v, self.cache.get_ttl(k))).collect();
                    
                    let client = reqwest::Client::new();
                    let resp = client.post(&format!("{}/item/", target_url)).json(&kvs).send().await?;
                    if resp.status().is_success() {
                        println!("[SEND_DATA] Successfully sent {} KV pairs to {}", kvs.len(), target_url);
                        return Ok(());
                    } else {
                        println!("[SEND_DATA] Failed to send data to {}: {}", target_url, resp.status());
                        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Failed to send KV data")));
                    }
                } else {
                    println!("[SEND_DATA] Target {} is not healthy", target_url);
                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Target not healthy")));
                }
            }
            println!("[SEND_DATA] No available redundancy targets");
            Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No redundancy targets")))
        }

        pub async fn resync(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            if let Some(target) = self.get_next_highest_priority() {
                
                let target_url = target.get_url();

                if reqwest::get(&format!("{}/health", target_url)).await?.status().is_success() {
                    let resp = reqwest::get(&format!("{}/item/", target_url)).await?;
                    
                    if resp.status().is_success() {
                        let kvs: Vec<(String, String, Option<usize>)> = resp.json().await?;
                        let kvs_len = kvs.len();
                        self.cache.add_all(kvs);
                        println!("[RESYNC] Successfully received {} KV pairs from {}", kvs_len, target_url);
                        return Ok(());
                    } else {
                        println!("[RESYNC] Failed to retrieve data from {}: {}", target_url, resp.status());
                        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Failed to retrieve KV data")));
                    }
                } else {
                    println!("[RESYNC] Target {} is not healthy", target_url);
                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Target not healthy")));
                }
            }
            println!("[RESYNC] No available redundancy targets");
            Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No redundancy targets")))
        }

        pub async fn down(&mut self) {
            if let Err(e) = self.send_data().await {
                eprintln!("[DOWN] Error sending data: {}", e);
            }
            self.is_down = true;
        }
        
        pub async fn up(&mut self) {
            self.is_down = false;
            if let Err(e) = self.resync().await {
                eprintln!("[UP] Error resyncing: {}", e);
            }
        }

    }


    #[derive(Clone)]
    pub struct Rustis_Node{
        pub cache: Arc<RwLock<Cache_Net>> 
    }


    impl Rustis_Node{
        pub fn new(cap:Option<usize>,evic: Option<fn (&mut DLL<String>) -> Option<String>>) -> Self{
            Self { cache: Arc::new(RwLock::new(Cache_Net::new(cap, evic))) }
        }
    }





}