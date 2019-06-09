use server::server::HustleServer;

fn main() {
    HustleServer::bind("127.0.0.1:8000").unwrap().listen();
}
