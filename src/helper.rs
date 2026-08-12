pub mod Helper{
    use std::{process::exit, sync::{LazyLock, RwLock}};



    pub const DBG_STR: &str = "";
    pub const OK:i32 = 0;
    pub const ERR:i32 = -1;
    pub const N_VNODES:usize = 4;
    pub const PORT:usize = 8080;
    pub static LOW: LazyLock<RwLock<usize>> = LazyLock::new(|| RwLock::new(10));
    pub static HIGH: LazyLock<RwLock<usize>> = LazyLock::new(|| RwLock::new(200));


    #[derive(Debug,Clone)]
    pub enum Role{
        Slaves, // This indicates redis node servers that are internally commanded by Master Server
        Master  // This indicates the ring routing server which we as users send the get/set/delete/update reqs to
    }

    impl From<String> for Role{
        fn from(value: String) -> Self {
            match value.to_lowercase().trim(){
                "master" | "main" | "orchestrator" => Role::Master,
                _ => Role::Slaves
            }
        }

    }



    #[derive(Debug,Clone)]
    pub struct CLI{
        pub dbg: bool,
        pub role: Role,
        pub n_vnode: usize,
        pub replica: Vec<String>,
        pub nodes: Vec<String>,
        pub port: u16
    }


    pub fn Help(){
        println!("{DBG_STR}");
        exit(OK);
    }


    impl CLI{
        pub fn new() -> Self{
            Self {dbg: false,role: Role::Slaves,n_vnode:N_VNODES,replica:vec![],nodes:vec![],port:8080}
        }

        pub fn Parse_Args(&mut self){
            let args: Vec<String> = std::env::args().skip(1).collect();
           for i in &args{
                if i == "-d" || i == "--debug" || i == " --DEBUG" || i == "-D"{
                    self.dbg = true;
                } else if i == "-h" || i == "--help" || i == " --HELP" || i == "-H"{
                   Help();
                } else if i.starts_with("--role=") || i.starts_with("-r="){
                    self.role = Role::from(i[i.find("=").unwrap()+1..].to_string());
                } else if i.starts_with("--n_vnodes=") || i.starts_with("-n="){
                    self.n_vnode = i[i.find("=").unwrap()+1..].parse().expect("No of Vnodes should be an unsigned int");
                } else if i.starts_with("--port=") || i.starts_with("-p="){
                    self.port = i[i.find("=").unwrap()+1..].parse().expect("Port is a 16 bit unsigned integer");
                } else if i.starts_with("--max=") || i.starts_with("-mx="){
                    *HIGH.try_write().unwrap() = i[i.find("=").unwrap()+1..].parse().expect("Port is a 16 bit unsigned integer");
                } else if i.starts_with("--min=") || i.starts_with("-mn="){
                    *LOW.try_write().unwrap() = i[i.find("=").unwrap()+1..].parse().expect("Port is a 16 bit unsigned integer");
                } else if (i.starts_with("--replica[") && i.ends_with("]")) || (i.starts_with("--replicas[") && i.ends_with("]")) || (i.starts_with("-rep[") && i.ends_with("]")){
                   self.replica.append(&mut i[i.find("[").unwrap()+1..i.find("]").unwrap()].split(",").map(|x| x.to_string()).collect::<Vec<String>>());
                } else if (i.starts_with("--node[") && i.ends_with("]")) || (i.starts_with("--nodes[") && i.ends_with("]")){
                   self.nodes.append(&mut i[i.find("[").unwrap()+1..i.find("]").unwrap()].split(",").map(|x| x.to_string()).collect::<Vec<String>>());
                } else{
                    Help();
                }
           } 

           if LOW.try_read().unwrap().ge(&*HIGH.try_read().unwrap()){
            panic!("High has to be always greater then LOW, this is used to generate Collision free Hashes for Vnodes");
           }

        }



    }


    





}