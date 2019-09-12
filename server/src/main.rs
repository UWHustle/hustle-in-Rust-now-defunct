use server::Server;

mod server;

fn main() {
    Server::bind("127.0.0.1:8000").unwrap().listen();
}
