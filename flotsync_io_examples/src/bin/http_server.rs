use clap::Parser;
use flotsync_io_examples::http_server::{HttpServerArgs, run};

fn main() {
    let args = HttpServerArgs::parse();
    if let Err(error) = run(args) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
