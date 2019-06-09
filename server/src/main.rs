use server::server::Server;

fn main() {
    Server::bind("127.0.0.1:8000").unwrap().listen();
}
