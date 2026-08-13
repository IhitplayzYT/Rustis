pub mod dll{
    use std::collections::HashMap;
use std::num::IntErrorKind::NegOverflow;
use std::sync::{Arc, Mutex};


    pub type Link<T> = Option<Arc<Mutex<Node<T>>>>;

    #[derive(Debug,Clone)]
    pub struct Node<T:Clone>{
        pub key: T,
        pub value: T,
        pub next: Link<T>,
        pub prev: Link<T>,
    }

    impl <T:Clone> Node<T>{
        pub fn new(key:T,value: T) -> Arc<Mutex<Node<T>>>{
           Arc::new(Mutex::new(Self { key,value, next: None, prev:None }))
        }

        pub fn get_value(&self) -> T{
            self.value.clone()
        }

        pub fn get_key(&self) -> T{
            self.key.clone()
        }

        pub fn set_value(&mut self,val:T){
            self.value = val;
        }

        pub fn set_key(&mut self,val:T){
            self.key = val;
        }

        pub fn remove_self(node: &Arc<Mutex<Self>>) -> bool{
            let mut ret = false;
            let (prev,next) = {
                let mut n = node.lock().unwrap();
                (n.prev.take(),n.next.take())
            };

            if let Some(pre) = &prev{
                pre.lock().unwrap().next = next.clone();
                ret = true;
            }

            if let Some(nxt) = &next{
                nxt.lock().unwrap().prev = prev.clone();
                ret = true;
            }
            ret
        }

    }



    #[derive(Debug)]
    pub struct DLL<T:Clone>{
        pub head: Link<T>,
        pub tail: Link<T>,
        pub len:usize,
        pub cap: usize,
        pub eviction_policy: fn(&mut DLL<T>) -> Option<T>
    }



    impl <T:Clone> DLL<T>{
        pub fn new(cap:Option<usize>,evic:Option<fn(&mut DLL<T>) -> Option<T>>) -> Self{
            Self { head: None,tail:None, len: 0, cap: cap.unwrap_or(usize::MAX), eviction_policy:evic.unwrap_or(|x| {
                if x.len == x.cap{
                    return x.pop_front();
                }
                None
               }) 
            }
        } 

        pub fn is_full(&self) -> bool{
            self.cap == self.len
        }

        pub fn push_back(&mut self,key:T,value: T) -> Option<T>{
            let mut k = None;
            if self.is_full(){
                k =  (self.eviction_policy)(self);
            }
            let nn = Node::new(key,value);
            match self.tail.take(){
                Some(old) => {
                    old.lock().unwrap().next = Some(Arc::clone(&nn));
                    nn.lock().unwrap().prev = Some(old);
                    self.tail = Some(nn);
                }
                None => {
                    self.head = Some(Arc::clone(&nn));
                    self.tail = Some(nn);
                }
            }
            k    
        }


        pub fn pop_front(&mut self) -> Option<T>{
            match self.head.take(){
                Some(x)  => {
                    let nxt = x.lock().unwrap().next.take();
                    match nxt{
                        Some(o) =>{
                            o.lock().unwrap().prev = None;
                            self.head = Some(o);
                        },
                        _ =>{
                            self.tail = None;
                        }
                    }
                    self.len -= 1;
                    return Some(x.lock().unwrap().get_key());
                },
                _ => {}
            }
            None
        }

        pub fn pop_back(&mut self) -> Option<T>{
            match self.tail.take(){
                Some(x) => {
                    let prev = x.lock().unwrap().prev.take();
                    match prev{
                        Some(o) => {
                            o.lock().unwrap().next = None;
                            self.tail = Some(o);
                        },
                        _ => {self.head = None;}
                    }
                    self.len -= 1;
                    return Some(x.lock().unwrap().get_key());
                },
                _ => {}
            }
            None
        }


        pub fn push_front(&mut self,key:T,value:T) -> Option<T>{
            let mut k = None;
            if self.is_full(){
               k = (self.eviction_policy)(self);
            }
            let nn = Node::new(key,value);
            match self.head.take(){
                Some(o) => {
                    nn.lock().unwrap().next = Some(Arc::clone(&o));
                    o.lock().unwrap().prev = Some(Arc::clone(&nn));
                    self.head = Some(nn);
                    self.len += 1;
                },
                _ => {
                    self.head = Some(Arc::clone(&nn));
                    self.tail = Some(nn);
                    self.len += 1;
                }
            }
            k
        }

    pub fn iter(&self) -> Iter<T> {
        Iter {current: self.head.clone()}
    }


    }


pub struct Iter<T:Clone> {
    current: Link<T>,
}

impl<T: Clone> Iterator for Iter<T> {
    type Item = (T,T);

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.current.take()?;
        let borrowed = node.lock().unwrap();
        let value = borrowed.get_value();
        let key = borrowed.get_key();
        self.current = borrowed.next.clone();
        Some((key,value))
    }
}

pub type EvictionPolicy<T> = fn(&mut DLL<T>) -> Option<T>;

pub fn fifo<T: Clone>(dll: &mut DLL<T>) -> Option<T> {
    dll.pop_front()
}

pub fn lifo<T: Clone>(dll: &mut DLL<T>) -> Option<T> {
    dll.pop_back()
}

pub fn lru<T: Clone>(dll: &mut DLL<T>) -> Option<T> {
    dll.pop_front()
}


pub struct EvictionRegistry<T: Clone> {
    policies: HashMap<String, EvictionPolicy<T>>,
}

impl<T: Clone> EvictionRegistry<T> {
    pub fn new() -> Self {
        Self {policies: HashMap::new()}
    }

    pub fn register(&mut self,name: impl Into<String>,policy: EvictionPolicy<T>) {
        self.policies.insert(name.into(), policy);
    }

    pub fn get(&self,name: &str) -> Option<EvictionPolicy<T>> {
        self.policies.get(name).copied()
    }

    pub fn remove(&mut self,name: &str) -> Option<EvictionPolicy<T>> {
        self.policies.remove(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.policies.contains_key(name)
    }

    pub fn len(&self) -> usize {
        self.policies.len()
    }
}

}