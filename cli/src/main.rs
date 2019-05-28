extern crate rustyline;

use rustyline::Editor;
use rustyline::error::ReadlineError;
use api::connection::Connection;

const COMMAND_HISTORY_FILE_NAME: &str = "commandhistory.txt";
const PROMPT: &str = "hustle> ";

fn main() {
    let connection = Connection::new();
    let mut editor = Editor::<()>::new();

    if editor.load_history(COMMAND_HISTORY_FILE_NAME).is_err() {
        println!("No command history.");
    }

    loop {
        let readline = editor.readline(PROMPT);
        match readline {
            Ok(line) => {
                editor.add_history_entry(line.as_str());
                let mut statement = connection.prepare(&line);
                if let Some(e) = statement.step().err() {
                    println!("Error: {}", e);
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
}
