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


    #[derive(Debug,PartialEq, Eq)]
    pub struct Routes{
        ip: String,
        port: u16,
        prio: Priority,
    }

    impl Routes{
        pub fn new(ip:String,port:u16,prio:Option<Priority>) -> Self{
            Self { ip, port, prio: prio.unwrap_or(Priority::Default)}
        }
    }



    #[derive(Debug)]
    pub struct Cache_Net{
        pub master: Cache,
        pub vnodes: BTreeSet<usize>,
        pub redundancies: BinaryHeap<Routes>,
        pub is_down:bool,
    }

    
    impl Cache_Net{
        pub fn new(cap:Option<usize>,evic: Option<fn (&mut DLL<String>) -> Option<String>>) -> Self{
            Self { master: Cache::new(cap, evic), redundancies: BinaryHeap::new(),vnodes:BTreeSet::new(),is_down:false}
        }

        pub fn add_vnodes(&mut self,n:usize){
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
                    hashable_str += &format!("{:?}",self.master);
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

        pub fn down(&mut self){
            send_data();
            self.is_down = true;
        }
        pub fn up(&mut self){
            self.is_down = false;
            resync();
        }

    }


    pub struct Rustis_Node{
        pub cache: Arc<RwLock<Cache_Net>> 
    }


    impl Rustis_Node{
        pub fn new(cap:Option<usize>,evic: Option<fn (&mut DLL<String>) -> Option<String>>) -> Self{
            Self { cache: Arc::new(RwLock::new(Cache_Net::new(cap, evic))) }
        }
    }





}