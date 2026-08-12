pub mod cache{
    use std::{cell::RefCell, collections::HashMap, rc::Rc};
    

    use crate::slave::dll::dll::{DLL, Node};
    


    #[derive(Debug)]
    pub struct Cache{
        pub map: HashMap<String,(Rc<RefCell<Node<String>>>,Option<usize>)>,
        pub dll:DLL<String>,
    }

    pub enum s_TTL{
        Alive,
        Dead,
        NA
    }

    impl Cache{
        // TODO: Make TTL actually functional 

        pub fn new(cap:Option<usize>,evic:Option<fn(&mut DLL<String>) -> Option<String>>) -> Self{
            Self { map: HashMap::new(), dll: DLL::new(cap, evic)}
        }

        pub fn contains_key(&self,k: String) -> bool{
            self.map.contains_key(&k)
        }

        pub fn contains_value(&self,v: String) -> Option<String>{
            let mut ret = None;
            for (k,val) in self.dll.iter(){
                if val == v {
                    ret = Some(k);
                    break;
                } 
            }
            ret
        }

        pub fn get_keys(&self) -> Vec<String>{
            self.map.keys().map(|x| x.to_owned()).collect()
        }

        pub fn get_values(&self) -> Vec<String>{
            self.dll.iter().map(|x| x.1).collect()
        }

        pub fn get_items(&self) -> Vec<(String,String)>{
            self.dll.iter().collect()
        }

        pub fn add(&mut self,k: String,v:String,ttl: Option<usize>){
            let old_key = self.dll.push_back(k.clone(), v);

            if let Some(o) = old_key{
                println!("[REMOVE] Removing Key {o}")
            }
            self.map.insert(k,(self.dll.tail.clone().expect("This shouldn't be possible"),ttl));
        }

        pub fn get(&self,k: String) -> Option<String>{
            if let Some(z) = self.map.get(&k){
                Some(z.0.borrow().get_value())
            }else{
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
                if let Some(mut p) = o.1{
                    p = p.saturating_sub(1);
                    if p > 0 {
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
            }
        }

        pub fn update(&mut self,k: String,v:Option<String>,ttl:Option<usize>) -> bool{
            if let Some(x) = self.map.get_mut(&k){
                if let Some(value) = v{
                    x.0.borrow_mut().set_value(value);
                }
                x.1 = ttl;
                return true;
            }
           false 
        }


    }




 



}