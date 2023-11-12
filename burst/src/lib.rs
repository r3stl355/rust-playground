use std::collections::HashMap;
use std::io::Result;

pub struct Executor;

impl Executor {
    pub fn execute(&self, cmd: String) {
        println!("{}", cmd);
    }
}

type ExecutorSpec = Box<dyn Fn(Executor) -> Result<()>>;

pub struct MachineSpec {
    instance_type: String,
    ami: String,
    init: ExecutorSpec,
}

impl MachineSpec {
    pub fn new<F>(instance_type: String, ami: String, init: F) -> Self
    where
        F: Fn(Executor) -> Result<()> + 'static,
    {
        MachineSpec {
            instance_type,
            ami,
            init: Box::new(init),
        }
    }
}
pub struct BurstBuilder {
    sets: HashMap<String, (u32, MachineSpec)>,
}

impl Default for BurstBuilder {
    fn default() -> Self {
        BurstBuilder {
            sets: Default::default(),
        }
    }
}

impl BurstBuilder {
    pub fn add_set(&mut self, name: String, count: u32, spec: MachineSpec) {
        self.sets.insert(name, (count, spec));
    }
    pub fn run(&self, ex: Executor) {
        (self.sets["server"].1.init)(ex);
    }
}
