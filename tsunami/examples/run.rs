use burst::{BurstBuilder, Executor, MachineSpec};

fn main() {
    let mut b = BurstBuilder::default();
    b.add_set(
        "server".to_string(),
        1,
        MachineSpec::new("t2.micro".to_string(), "ami".to_string(), |ex: Executor| {
            ex.execute("hellow".to_string());
            Ok(())
        }),
    );
    b.run(Executor {})
}
