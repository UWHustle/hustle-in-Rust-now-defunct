use std::io;
use std::net::{TcpListener, TcpStream};
use server::message::Message;

fn handle_client(mut stream: TcpStream) {
    loop {
        match Message::decode(&mut stream).unwrap() {
            Message::Execute { statement } => println!("{}", statement)
        }
    }
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8000")?;

    for stream in listener.incoming() {
        handle_client(stream?);
    }

    Ok(())
}
