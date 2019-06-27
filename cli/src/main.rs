extern crate rustyline;

use rustyline::Editor;
use rustyline::error::ReadlineError;
use hustle_api::HustleConnection;

const COMMAND_HISTORY_FILE_NAME: &str = "commandhistory.txt";
const PROMPT: &str = "hustle> ";

fn main() -> Result<(), String> {
    let mut connection = HustleConnection::connect("127.0.0.1:8000").map_err(|e| e.to_string())?;
    let mut editor = Editor::<()>::new();

    if editor.load_history(COMMAND_HISTORY_FILE_NAME).is_err() {
        println!("No command history.");
    }

    loop {
        let readline = editor.readline(PROMPT);
        match readline {
            Ok(line) => {
                editor.add_history_entry(line.clone());
                match connection.execute(line) {
                    Ok(result) => {
                        result.map(|r| println!("{}", r));
                    },
                    Err(reason) => println!("{}", reason)
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
