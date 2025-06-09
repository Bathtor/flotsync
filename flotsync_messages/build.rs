use protobuf_codegen::Codegen;
use std::{fs, path::PathBuf};

fn main() {
    // Path to the directory containing .proto files
    let proto_dir =
        fs::canonicalize(PathBuf::from("../messages/proto/")).expect("Canonical proto_dir");

    //println!("cargo:warning=Proto dir is: {}", proto_dir.display());

    // Collect all .proto files in the directory
    let proto_files: Vec<_> = fs::read_dir(&proto_dir)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("proto") {
                let absolute_path = fs::canonicalize(path).expect("Could not canonicalize");
                // println!(
                //     "cargo:warning=Found proto file: {}",
                //     absolute_path.display()
                // );
                Some(absolute_path)
            } else {
                None
            }
        })
        .collect();

    println!("cargo:rerun-if-changed={}", proto_dir.to_string_lossy());
    for proto_file in proto_files.iter() {
        println!("cargo:rerun-if-changed={}", proto_file.to_string_lossy());
    }

    // Compile the .proto files into the output directory
    Codegen::new()
        .pure()
        .cargo_out_dir("protos")
        .inputs(&proto_files)
        .include(&proto_dir)
        .run()
        .expect("Failed to generate Rust code from .proto files");
}
