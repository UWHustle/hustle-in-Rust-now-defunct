extern crate cmake;

fn main() {
    let mut dst = cmake::build("parser");
    dst.push("build");

    println!("cargo:rustc-link-search=native={}", dst.display());
    println!("cargo:rustc-link-lib=static=hustle_parser");
}