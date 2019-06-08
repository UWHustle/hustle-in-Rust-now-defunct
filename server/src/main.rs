use std::io;
use server::server::HustleServer;

fn main() -> io::Result<()> {
    HustleServer::bind("127.0.0.1:8000")?.listen()
}
