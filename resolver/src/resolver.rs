use message::{Plan, Message, Column, Table};
use serde_json as json;
use std::sync::mpsc::{Receiver, Sender};
use crate::catalog::Catalog;

pub struct Resolver {
    catalog: Catalog
}

impl Resolver {
    pub fn new() -> Self {
        Resolver {
            catalog: Catalog::try_from_file().unwrap_or(Catalog::new())
        }
    }

    pub fn listen(
        &mut self,
        resolver_rx: Receiver<Vec<u8>>,
        transaction_tx: Sender<Vec<u8>>,
        completed_tx: Sender<Vec<u8>>,
    ) {
        loop {
            let buf = resolver_rx.recv().unwrap();
            let request = Message::deserialize(&buf).unwrap();
            match request {
                Message::ResolveAst { ast, connection_id } => {
                    match self.resolve(&ast) {
                        Ok(plan) => transaction_tx.send(Message::ExecutePlan {
                            plan,
                            connection_id,
                        }.serialize().unwrap()).unwrap(),
                        Err(reason) => completed_tx.send(Message::Failure {
                            reason,
                            connection_id,
                        }.serialize().unwrap()).unwrap()
                    }
                },
                _ => completed_tx.send(buf).unwrap()
            };
        }
    }

    pub fn resolve(&mut self, ast: &str) -> Result<Plan, String> {
        serde_json::from_str(ast)
            .map_err(|e| e.to_string())
            .and_then(|node| self.resolve_node(&node))
    }

    fn resolve_node(&mut self, node: &json::Value) -> Result<Plan, String> {
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

    fn resolve_create_table(&mut self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "create_table");
        let name = node["name"].as_str().unwrap().to_owned();
        if self.catalog.table_exists(&name) {
            Err(format!("Table {} already exists", &name))
        } else {
            let columns = node["columns"].as_array()
                .unwrap()
                .iter()
                .map(|column| {
                    Column::new(
                        column["name"].as_str().unwrap().to_owned(),
                    column["column_type"].as_str().unwrap().to_owned(),
                        name.clone()
                    )
                })
                .collect();
            let table = Table::new(name, columns);
            self.catalog.create_table(table.clone())?;
            Ok(Plan::CreateTable { table })
        }
    }

    fn resolve_delete(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "delete");
        let from_table = self.resolve_table(&node["from_table"])?;
        let active_columns = from_table.columns.clone();
        let multi_table = false;
        let filter = match node.get("filter") {
            Some(f) => Some(Box::new(self.resolve_filter(f, &active_columns, &multi_table)?)),
            None => None
        };
        Ok(Plan::Delete { from_table, filter })
    }

    fn resolve_drop_table(&mut self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "drop_table");
        let table = self.resolve_table(&node["table"])?;
        self.catalog.drop_table(&table.name)?;
        Ok(Plan::DropTable { table })
    }

    fn resolve_insert(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "insert");
        let input = &node["input"];
        if input["type"] != "values" {
            return Err("Inserts of only literal values are supported".to_string());
        }
        let into_table = self.resolve_table(&node["into_table"])?;
        let values: Result<Vec<Plan>, String> = input["values"].as_array().unwrap().iter()
            .zip(into_table.columns.iter())
            .map(|(value, column)|
                self.resolve_literal(value, Some(column.column_type.clone())))
            .collect();
        let row = Plan::Row { values: values? };
        Ok(Plan::Insert { into_table, input: Box::new(row) })
    }

    fn resolve_select(&self, node: &json::Value) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "select");
        let mut active_columns = vec![];
        let mut multi_table = false;
        let mut table = self.resolve_input(
            &node["from_table"],
            &mut active_columns,
            &mut multi_table
        )?;
        if let Some(filter) = node.get("filter") {
            table = Plan::Select {
                table: Box::new(table),
                filter: Box::new(self.resolve_filter(filter, &active_columns, &multi_table)?)
            };
        }
        Ok(Plan::Project {
            table: Box::new(table),
            projection: self.resolve_projection(
                node["projection"].as_array().unwrap(),
                &active_columns,
                &multi_table,
            )?
        })
    }

    fn resolve_input(
        &self,
        node: &json::Value,
        active_columns: &mut Vec<Column>,
        multi_table: &mut bool
    ) -> Result<Plan, String> {
        match node["type"].as_str().unwrap() {
            "table" => {
                let table = self.resolve_table(node)?;
                active_columns.append(&mut table.columns.clone());
                Ok(Plan::TableReference { table })
            },
            "join" => {
                *multi_table = true;
                self.resolve_join(node, active_columns, multi_table)
            },
            _ => Err("Input to node must be a table reference or join".to_string())
        }
    }

    fn resolve_table(&self, node: &json::Value) -> Result<Table, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "table");
        let name = node["name"].as_str().unwrap();
        self.catalog.get_table(name)
            .map(|table| table.clone())
            .ok_or(format!("Unrecognized table: {}", name))
    }

    fn resolve_join(
        &self,
        node: &json::Value,
        active_columns: &mut Vec<Column>,
        multi_table: &mut bool,
    ) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "join");
        let l_table = Box::new(self.resolve_input(&node["l_table"], active_columns, multi_table)?);
        let r_table = Box::new(self.resolve_input(&node["r_table"], active_columns, multi_table)?);
        let filter = match node.get("filter") {
            Some(f) => Some(Box::new(self.resolve_filter(f, active_columns, multi_table)?)),
            None => None
        };
        Ok(Plan::Join { l_table, r_table, filter })
    }

    fn resolve_filter(
        &self,
        node: &json::Value,
        active_columns: &Vec<Column>,
        multi_table: &bool,
    ) -> Result<Plan, String> {
        let node_type = node["type"].as_str().unwrap();
        match node_type {
            "operation" => {
                let name = node["name"].as_str().unwrap().to_owned();
                let left = self.resolve_filter(&node["left"], active_columns, multi_table)?;
                let right = self.resolve_filter(&node["right"], active_columns, multi_table)?;
                match name.as_str() {
                    "and" | "or" => Ok(Plan::Connective {
                        name,
                        terms: vec![left, right],
                    }),
                    "eq" | "lt" | "le" | "gt" | "ge" => Ok(Plan::Comparative {
                        name,
                        left: Box::new(left),
                        right: Box::new(right),
                    }),
                    _ => Err(format!("Unsupported operation type {}", name))
                }
            }
            "column" => Ok(Plan::ColumnReference {
                column: self.resolve_column(node, active_columns, multi_table)?
            }),
            "literal" => self.resolve_literal(node, None),
            _ => Err(format!("Unsupported selection node type {}", node_type))
        }
    }

    fn resolve_column(
        &self,
        node: &json::Value,
        active_columns: &Vec<Column>,
        multi_table: &bool,
    ) -> Result<Column, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "column");
        let column_name = node["name"].as_str().unwrap();
        if *multi_table {
            let table_name = node.get("table")
                .map(|t| t.as_str().unwrap())
                .ok_or(format!("Ambiguous column: {}", column_name))?;

            active_columns.iter()
                .find(|c| c.name == column_name && c.table == table_name)
                .map(|c| c.clone())
                .ok_or(format!("Unrecognized column: {}", column_name))

        } else {
            active_columns.iter()
                .find(|c| c.name == column_name)
                .map(|c| c.clone())
                .ok_or(format!("Unrecognized column: {}", column_name))
        }
    }

    fn resolve_literal(
        &self,
        node: &json::Value,
        literal_type: Option<String>
    ) -> Result<Plan, String> {
        debug_assert_eq!(node["type"].as_str().unwrap(), "literal");
        // TODO: Infer type if no hint is given.
        let literal_type = literal_type
            .or(node.get("literal_type").map(|t| t.as_str().unwrap().to_owned()))
            .ok_or("Could not infer literal type".to_string())?;
        Ok(Plan::Literal {
            value: node["value"].as_str().unwrap().to_owned(),
            literal_type
        })
    }

    fn resolve_projection(
        &self,
        nodes: &Vec<json::Value>,
        active_columns: &Vec<Column>,
        multi_table: &bool,
    ) -> Result<Vec<Plan>, String> {
        nodes.iter()
            .map(|node| {
                if node["type"] == "column" {
                    Ok(Plan::ColumnReference {
                        column: self.resolve_column(node, active_columns, multi_table)?
                    })
                } else {
                    Err("Projection only on columns is supported".to_string())
                }
            })
            .collect()
    }

    fn resolve_update(&self, node: &json::Value) -> Result<Plan, String> {
        let table = self.resolve_table(&node["table"])?;
        let active_columns = table.columns.clone();
        let multi_table = false;
        let (mut columns, mut assignments) = (vec![], vec![]);
        for assignment_node in node["assignments"].as_array().unwrap() {
            debug_assert_eq!(assignment_node["type"].as_str().unwrap(), "assignment");
            let column = self.resolve_column(
                &assignment_node["column"],
                &active_columns,
                &multi_table
            )?;
            let literal = self.resolve_literal(
                &assignment_node["value"],
                Some(column.column_type.clone())
            )?;
            columns.push(column);
            assignments.push(literal);
        }
        let filter = match node.get("filter") {
            Some(f) => Some(Box::new(self.resolve_filter(f, &active_columns, &multi_table)?)),
            None => None
        };
        Ok(Plan::Update { table, columns, assignments, filter })
    }
}
