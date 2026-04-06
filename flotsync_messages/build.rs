use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let workspace_root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .expect("Canonical workspace root");
    let descriptor_set = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"))
        .join("flotsync_messages-descriptor-set.binpb");
    let proto_files = [
        "flotsync/datamodel/v1/datamodel.proto",
        "flotsync/delivery/v1/delivery.proto",
        "flotsync/discovery/v1/discovery.proto",
        "flotsync/versions/v1/versions.proto",
    ];

    emit_buf_rerun_if_changed(&workspace_root);
    build_descriptor_set(&workspace_root, &descriptor_set);

    buffa_build::Config::new()
        .files(&proto_files)
        .descriptor_set(&descriptor_set)
        .include_file("flotsync_messages.rs")
        .generate_json(false)
        .generate_text(false)
        .generate_arbitrary(false)
        .compile()
        .expect("Failed to generate Rust code from .proto files");
}

fn build_descriptor_set(workspace_root: &Path, descriptor_set: &Path) {
    let output = Command::new("buf")
        .current_dir(workspace_root)
        .arg("build")
        .arg("--as-file-descriptor-set")
        .arg("-o")
        .arg(descriptor_set)
        .arg("messages/proto")
        .output()
        .expect("Run buf build");
    if !output.status.success() {
        panic!(
            "buf build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn emit_buf_rerun_if_changed(workspace_root: &Path) {
    let buf_yaml = workspace_root.join("buf.yaml");
    println!("cargo:rerun-if-changed={}", buf_yaml.display());

    let buf_lock = workspace_root.join("buf.lock");
    if buf_lock.exists() {
        println!("cargo:rerun-if-changed={}", buf_lock.display());
    }

    let output = Command::new("buf")
        .current_dir(workspace_root)
        .arg("ls-files")
        .arg("messages/proto")
        .output()
        .expect("Run buf ls-files");
    if !output.status.success() {
        panic!(
            "buf ls-files failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    for path in String::from_utf8_lossy(&output.stdout).lines() {
        let path = path.trim();
        if !path.is_empty() {
            println!(
                "cargo:rerun-if-changed={}",
                workspace_root.join(path).display()
            );
        }
    }
}
