extern crate rustyline;

use rustyline::Editor;
use rustyline::error::ReadlineError;
use std::net::TcpStream;
use message::Message;

const COMMAND_HISTORY_FILE_NAME: &str = "commandhistory.txt";
const PROMPT: &str = "hustle> ";

fn main() -> Result<(), String> {
    let mut stream = TcpStream::connect("127.0.0.1:8000").map_err(|e| e.to_string())?;
    let mut editor = Editor::<()>::new();

    if editor.load_history(COMMAND_HISTORY_FILE_NAME).is_err() {
        println!("No command history.");
    }

    loop {
        let readline = editor.readline(PROMPT);
        match readline {
            Ok(line) => {
                let request = Message::ExecuteSQL { sql: line };
                request.send(&mut stream)?;

                while let Ok(response) = Message::receive(&mut stream) {
                    match response {
                        Message::Success { connection_id: _ } => break,
                        Message::ReturnRow { row, connection_id: _ } => {

                        },
                        _ => panic!("Invalid message sent to client")
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("^D");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }

    editor.save_history(COMMAND_HISTORY_FILE_NAME).unwrap();

    Ok(())
}
