extern crate cmake;

fn main() {
    cmake::Config::new("../optimizer")
        .build_arg("-j4")
        .build();
}
