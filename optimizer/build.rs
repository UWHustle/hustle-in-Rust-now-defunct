extern crate cmake;

use std::{fs, env};
use std::path::PathBuf;

fn main() {
    let mut optimizer_src = cmake::Config::new("lib")
        .build_arg("-j4")
        .build();

    optimizer_src.push("build");
    optimizer_src.push("optimizer");

    let out_dir = env::var("OUT_DIR").unwrap();
    let mut optimizer_dst = PathBuf::from(out_dir);
    optimizer_dst.push("optimizer");

    fs::rename(optimizer_src, optimizer_dst).unwrap();
}