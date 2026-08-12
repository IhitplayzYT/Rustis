pub mod dll{
    use std::num::IntErrorKind::NegOverflow;
use std::rc::Rc;
    use std::cell::RefCell;


    pub type Link<T> = Option<Rc<RefCell<Node<T>>>>;

    #[derive(Debug,Clone)]
    pub struct Node<T:Clone>{
        pub key: T,
        pub value: T,
        pub next: Link<T>,
        pub prev: Link<T>,
    }

    impl <T:Clone> Node<T>{
        pub fn new(key:T,value: T) -> Rc<RefCell<Node<T>>>{
           Rc::new(RefCell::new(Self { key,value, next: None, prev:None }))
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

        pub fn remove_self(node: &Rc<RefCell<Self>>) -> bool{
            let mut ret = false;
            let (prev,next) = {
                let mut n = node.borrow_mut();
                (n.prev.take(),n.next.take())
            };

            if let Some(pre) = &prev{
                pre.borrow_mut().next = next.clone();
                ret = true;
            }

            if let Some(nxt) = &next{
                nxt.borrow_mut().prev = prev.clone();
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
                    old.borrow_mut().next = Some(Rc::clone(&nn));
                    nn.borrow_mut().prev = Some(old);
                    self.tail = Some(nn);
                }
                None => {
                    self.head = Some(Rc::clone(&nn));
                    self.tail = Some(nn);
                }
            }
            k    
        }


        pub fn pop_front(&mut self) -> Option<T>{
            match self.head.take(){
                Some(x)  => {
                    let nxt = x.borrow_mut().next.take();
                    match nxt{
                        Some(o) =>{
                            o.borrow_mut().prev = None;
                            self.head = Some(o);
                        },
                        _ =>{
                            self.tail = None;
                        }
                    }
                    self.len -= 1;
                    return Some(x.borrow().get_key());
                },
                _ => {}
            }
            None
        }

        pub fn pop_back(&mut self) -> Option<T>{
            match self.tail.take(){
                Some(x) => {
                    let prev = x.borrow_mut().prev.take();
                    match prev{
                        Some(o) => {
                            o.borrow_mut().next = None;
                            self.tail = Some(o);
                        },
                        _ => {self.head = None;}
                    }
                    self.len -= 1;
                    return Some(x.borrow().get_key());
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
                    nn.borrow_mut().next = Some(Rc::clone(&o));
                    o.borrow_mut().prev = Some(Rc::clone(&nn));
                    self.head = Some(nn);
                    self.len += 1;
                },
                _ => {
                    self.head = Some(Rc::clone(&nn));
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
        let borrowed = node.borrow();
        let value = borrowed.get_value();
        let key = borrowed.get_key();
        self.current = borrowed.next.clone();
        Some((key,value))
    }
}







}