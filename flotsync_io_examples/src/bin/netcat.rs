use clap::Parser;
use flotsync_io_examples::netcat::{NetcatArgs, run};

fn main() {
    let args = NetcatArgs::parse();
    if let Err(error) = run(args) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
