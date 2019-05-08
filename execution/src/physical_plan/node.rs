use logical_entities::relation::Relation;
use physical_operators::Operator;

use std::rc::Rc;

use storage::StorageManager;

pub struct Node {
    operator: Rc<Operator>,
    dependencies: Vec<Rc<Node>>,
}

impl Node {
    pub fn new(operator: Rc<Operator>, dependencies: Vec<Rc<Node>>) -> Self {
        Node {
            operator,
            dependencies,
        }
    }

    pub fn get_output_relation(&self) -> Relation {
        self.operator.get_target_relation()
    }

    pub fn execute(&self, storage_manager: &StorageManager) -> Relation {
        for node in &self.dependencies {
            node.execute(storage_manager);
        }
        match self.operator.execute(storage_manager) {
            Ok(val) => val,
            Err(string) => {
                println!("Error: {}", string);
                self.get_output_relation()
            }
        }
    }
}
