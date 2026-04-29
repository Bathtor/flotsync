use clap::Parser;
use flotsync_io_examples::replicated_checklist::{ReplicatedChecklistArgs, run};

fn main() {
    let args = ReplicatedChecklistArgs::parse();
    if let Err(error) = run(args) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
