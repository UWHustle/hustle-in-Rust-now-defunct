use message::{Plan, Listener, Message};
use serde_json as json;
use std::sync::mpsc::{Receiver, Sender};

pub struct Resolver;

impl Resolver {
    pub fn new() -> Self {
        Resolver
    }

    pub fn resolve(&self, ast: &str) -> Result<Plan, String> {
        serde_json::from_str(ast)
            .map_err(|e| e.to_string())
            .and_then(|node| self.resolve_node(&node))
    }

    fn resolve_node(&self, node: &json::Value) -> Result<Plan, String> {
        let node_type = node["type"].as_str().unwrap();
        match node_type {
            "create_table" => self.resolve_create_table(node),
            "delete" => self.resolve_delete(node),
            "drop_table" => self.resolve_drop_table(node),
            "insert" => self.resolve_insert(node),
            "select" => self.resolve_select(node),
            "update" => self.resolve_update(node),
            _ => Err(format!("Unrecognized AST node type: {}", node_type))
        }
    }

    fn resolve_create_table(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "create_table");
        let name = node["name"].as_str().unwrap().to_owned();
        let columns = node["columns"].as_array()
            .unwrap()
            .iter()
            .map(|column| {
                Plan::Column {
                    name: column["name"].as_str().unwrap().to_owned(),
                    column_type: column["column_type"].as_str().unwrap().to_owned(),
                    table: None,
                    alias: None
                }
            })
            .collect();
        Ok(Plan::CreateTable { name, columns })
    }

    fn resolve_delete(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "delete");
        let from_table = Box::new(self.resolve_table(&node["from_table"])?);
        let filter = match node.get("filter") {
            Some(f) => Some(Box::new(self.resolve_filter(f)?)),
            None => None
        };
        Ok(Plan::Delete { from_table, filter })
    }

    fn resolve_drop_table(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "drop_table");
        let table = Box::new(self.resolve_table(&node["table"])?);
        Ok(Plan::DropTable { table })
    }

    fn resolve_insert(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "insert");
        let input = &node["input"];
        if input["type"] != "values" {
            return Err("Inserts of only literal values are supported".to_string());
        }
        let values: Result<Vec<Plan>, String> = input["values"].as_array().unwrap().iter()
            .map(|value| self.resolve_literal(value))
            .collect();
        let row = Plan::Row { values: values? };
        let into_table = Box::new(self.resolve_table(&node["into_table"])?);
        Ok(Plan::Insert { into_table, input: Box::new(row) })
    }

    fn resolve_select(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "select");
        let mut table = self.resolve_input(&node["from_table"])?;
        if let Some(filter) = node.get("filter") {
            table = Plan::Select {
                table: Box::new(table),
                filter: Box::new(self.resolve_filter(filter)?)
            };
        }
        Ok(Plan::Project {
            table: Box::new(table),
            projection: self.resolve_projection(node["projection"].as_array().unwrap())?
        })
    }

    fn resolve_input(&self, node: &json::Value) -> Result<Plan, String> {
        match node["type"].as_str().unwrap() {
            "table" => self.resolve_table(node),
            "join" => self.resolve_join(node),
            _ => Err("Input to node must be a table reference or join".to_string())
        }
    }

    fn resolve_table(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "table");
        Ok(Plan::Table { name: node["name"].as_str().unwrap().to_owned(), columns: vec![] })
    }

    fn resolve_join(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "join");
        let l_table = Box::new(self.resolve_input(&node["l_table"])?);
        let r_table = Box::new(self.resolve_input(&node["r_table"])?);
        let filter = match node.get("filter") {
            Some(f) => Some(Box::new(self.resolve_filter(f)?)),
            None => None
        };
        Ok(Plan::Join { l_table, r_table, filter })
    }

    fn resolve_filter(&self, node: &json::Value) -> Result<Plan, String> {
        let node_type = node["type"].as_str().unwrap();
        match node_type {
            "operation" => {
                let name = node["name"].as_str().unwrap().to_owned();
                let left = self.resolve_filter(&node["left"])?;
                let right = self.resolve_filter(&node["right"])?;
                match name.as_str() {
                    "and" | "or" => Ok(Plan::Connective {
                        name,
                        terms: vec![left, right],
                    }),
                    "eq" | "lt" | "le" | "gt" | "ge" => Ok(Plan::Comparison {
                        name,
                        left: Box::new(left),
                        right: Box::new(right),
                    }),
                    _ => Err(format!("Unsupported operation type {}", name))
                }
            }
            "column" => self.resolve_column(node),
            "literal" => self.resolve_literal(node),
            _ => Err(format!("Unsupported selection node type {}", node_type))
        }
    }

    fn resolve_column(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "column");
        Ok(Plan::Column {
            name: node["name"].as_str().unwrap().to_owned(),
            column_type: String::new(),
            table: None,
            alias: node.get("alias").map(|a| a.as_str().unwrap().to_owned()),
        })
    }

    fn resolve_literal(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "literal");
        Ok(Plan::Literal {
            value: node["value"].as_str().unwrap().to_owned(),
            literal_type: String::new()
        })
    }

    fn resolve_projection(&self, nodes: &Vec<json::Value>) -> Result<Vec<Plan>, String> {
        nodes.iter()
            .map(|node| {
                if node["type"] == "column" {
                    self.resolve_column(node)
                } else {
                    Err("Projection only on columns is supported".to_string())
                }
            })
            .collect()
    }

    fn resolve_update(&self, node: &json::Value) -> Result<Plan, String> {
        let table = Box::new(self.resolve_table(&node["table"])?);
        let (mut columns, mut assignments) = (vec![], vec![]);
        for assignment_node in node["assignments"].as_array().unwrap() {
            debug_assert_eq!(assignment_node["type"].as_str().unwrap(), "assignment");
            columns.push(self.resolve_column(&assignment_node["column"])?);
            assignments.push(self.resolve_literal(&assignment_node["value"])?);
        }
        let filter = match node.get("filter") {
            Some(f) => Some(Box::new(self.resolve_filter(f)?)),
            None => None
        };
        Ok(Plan::Update { table, columns, assignments, filter })
    }
}

impl Listener for Resolver {
    fn listen(&mut self, input_rx: Receiver<Vec<u8>>, output_tx: Sender<Vec<u8>>) {
        loop {
            let request = Message::deserialize(&input_rx.recv().unwrap()).unwrap();
            let response = match request {
                Message::ResolveAst { ast, connection_id } => {
                    match self.resolve(&ast) {
                        Ok(plan) => Message::ExecutePlan { plan, connection_id },
                        Err(reason) => Message::Error { reason, connection_id }
                    }
                },
                _ => request
            };
            output_tx.send(response.serialize().unwrap()).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
