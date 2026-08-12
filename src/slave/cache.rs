pub mod cache{
    use std::{collections::HashMap, sync::{Arc, Mutex}};


    use crate::slave::dll::dll::{DLL, Node};
    


    #[derive(Debug)]
    pub struct Cache{
        pub map: HashMap<String,(Arc<Mutex<Node<String>>>,Option<usize>)>,
        pub dll:DLL<String>,
    }

    pub enum s_TTL{
        Alive,
        Dead,
        NA
    }

    impl Cache{

        pub fn new(cap:Option<usize>,evic:Option<fn(&mut DLL<String>) -> Option<String>>) -> Self{
            Self { map: HashMap::new(), dll: DLL::new(cap, evic)}
        }

        pub fn contains_key(&self,k: String) -> bool{
            let ret = self.map.contains_key(&k);
            println!("[CONTAINS_KEY]: Node {} key: {}",if ret{"contains"}else{"doesn't contain"},&k);
            ret
        }

        pub fn contains_value(&self,v: String) -> Option<String>{
            let mut ret = None;
            for (k,val) in self.dll.iter(){
                if val == v {
                    ret = Some(k);
                    break;
                } 
            }
            print!("[CONTAINS_VALUE]: Node {} value: {}",if ret.is_some(){"contains"}else{"doesn't contain"},&v);
            if let Some(z) = &ret{
                println!(", the key is {z}");
            }else{
                println!();
            }
            ret
        }

        pub fn get_keys(&self) -> Vec<String>{
            let ret:Vec<String> = self.map.keys().map(|x| x.to_owned()).collect();
            println!("[GET_KEYS] There are {} keys",ret.len());
            ret
        }

        pub fn get_values(&self) -> Vec<String>{
            let ret:Vec<String> = self.dll.iter().map(|x| x.1).collect();
            println!("[GET_VALUES] There are {} values",ret.len());
            ret
        }

        pub fn get_items(&self) -> Vec<(String,String)>{
            let ret:Vec<(String, String)> = self.dll.iter().collect();
            println!("[GET_ITEMS] There are {} items",ret.len());
            ret
        }

        pub fn add(&mut self,k: String,v:String,ttl: Option<usize>){
            let old_key = self.dll.push_back(k.clone(), v);
            if let Some(o) = old_key{
                println!("[ADD][Eviction] Evicting Key {o}")
            }
            self.map.insert(k,(self.dll.tail.clone().expect("This shouldn't be possible"),ttl));
            println!("[ADD] Inserted item");

        }

        pub fn add_all(&mut self,data:Vec<(String,String,Option<usize>)>){
            for (k,v,ttl) in data{
                let old_key = self.dll.push_back(k.clone(), v);
                if let Some(o) = old_key{
                    println!("[ADD_ALL][Eviction] Evicting Key {o}")
                }
                self.map.insert(k,(self.dll.tail.clone().expect("This shouldn't be possible"),ttl));
            }
            println!("[ADD_ALL] Inserted items");
        }


        pub fn get(&self,k: String) -> Option<String> {
            if let Some(z) = self.map.get(&k){
                println!("[GET] Key Found");
                Some(z.0.lock().unwrap().get_value())
            }else{
                println!("[GET] Key Not Found");
                None
            }
        }

        pub fn get_ttl(&self,k: String) -> Option<usize>{
            if let Some(o) = self.map.get(&k){
                o.1
            }else{
                None
            }
        }

        pub fn decr_ttl(&mut self,k: String) -> Option<s_TTL>{
            if let Some(o) = self.map.get_mut(&k){
                if let Some(p) = o.1{
                    let new_p = p.saturating_sub(1);
                    o.1 = Some(new_p);
                    if new_p > 0 {
                        return Some(s_TTL::Alive);
                    }else{
                        return Some(s_TTL::Dead);
                    }
                }else{
                    return Some(s_TTL::NA);
                }
            }else{
                return None;
            }
        }

        pub fn delete(&mut self,k: String){
            if let Some(z) = self.map.get(&k){
                if Node::remove_self(&z.0){
                    self.dll.len -= 1;
                }
                self.map.remove(&k);
                println!("[DELETE] Key deleted");
            } else{
                println!("[DELETE] Key to delete NOT found");
            }

        }

        pub fn update(&mut self,k: String,v:Option<String>,ttl:Option<usize>) -> bool{
            if let Some(x) = self.map.get_mut(&k){
                if let Some(value) = v{
                    x.0.lock().unwrap().set_value(value);
                }
                x.1 = ttl;
                println!("[UPDATE] Key updated");
                return true;
            }else{
                println!("[UPDATE] Key to be updated NOT found");
                return false;
            }
        }


    }




 



}