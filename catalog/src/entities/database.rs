
use entities::statistic::Statistic;
use entities::relation::Relation;

extern crate serde;
extern crate serde_json;

use serde_json::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    pub version: String,
    pub name: String,
    pub stats: Vec<Statistic>,
    pub relations: Vec<Relation>,
}

impl Database {

    pub fn new(name: String) -> Database {
        Database{
            version: "0.1".to_string(),
            name: name,
            stats: vec!(),
            relations: vec!(),
        }
    }

    pub fn delete(&self) -> Result<(),Error> {
        use std::fs;
        fs::remove_file(Database::filename(&self.name)).unwrap();
        Ok(())
    }

    pub fn add_relation(&mut self, name: String) -> () {
        self.relations.push(Relation::new(name))
    }

    pub fn get_relation(&mut self, name: String) -> &mut Relation {
        let relations = &mut self.relations;
        relations.iter_mut().find(|rel | rel.name == name).unwrap()
    }

    pub fn serialize(&mut self) -> Result<String, Error> {
        Ok(serde_json::to_string(self).unwrap())
    }

    pub fn deserialize(serialized_string: String) -> Result<Database, Error> {
        Ok(serde_json::from_str(serialized_string.as_str()).unwrap())
    }

    pub fn save(&mut self) -> Result<(),Error> {
        use std::fs::OpenOptions;
        use std::io::prelude::*;

        let mut file = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(Database::filename(&self.name))
            .unwrap();

        file.write_all(self.serialize().unwrap().as_bytes()).unwrap();

        Ok(())
    }

    pub fn load(name: String) -> Result<Database, Error> {
        use std::fs::File;
        use std::io::prelude::*;
        use std::io::ErrorKind::*;


        let mut file = match File::open(Database::filename(&name)) {
            Ok(file) => file,
            Err(e) => match e.kind() {
                NotFound => {
                    Database::new(name.clone()).save().unwrap();
                    File::open(Database::filename(&name)).unwrap()
                },
                _ => panic!("Can't read from file: {}, err {}", name, e),
            }
        };


       /* let mut file = OpenOptions::new()
            .open(Database::filename(&name))
            .unwrap();*/

        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        Database::deserialize(contents)
    }

    fn filename(name: &String) -> String{
        format!("{}.{}", name, "catalog")
    }
}