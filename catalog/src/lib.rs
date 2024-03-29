#[macro_use]
extern crate serde;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;

use hustle_types::TypeVariant;

const CATALOG_FILE_NAME: &str = "catalog.json";

#[derive(Serialize, Deserialize)]
pub struct Catalog {
    tables: RwLock<HashMap<String, Table>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub fn try_from_file() -> Option<Self> {
        let file = File::open(CATALOG_FILE_NAME).ok()?;
        serde_json::from_reader(file).ok()
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.read().unwrap().contains_key(name)
    }

    pub fn get_table(&self, name: &str) -> Option<Table> {
        self.tables.read().unwrap().get(name).map(|t| t.clone())
    }

    pub fn create_table(&self, table: Table) -> Result<(), String> {
        self.tables.write().unwrap().insert(table.name.clone(), table);
        self.flush()
    }

    pub fn drop_table(&self, name: &str) -> Result<(), String> {
        self.tables.write().unwrap().remove(name);
        self.flush()
    }

    pub fn append_block_id(&self, table_name: &str, block_id: u64) -> Result<(), String> {
        self.tables.write().unwrap()
            .get_mut(table_name)
            .ok_or(format!("Table {} does not exist", table_name))?
            .block_ids.push(block_id);
        self.flush()
    }

    fn flush(&self) -> Result<(), String> {
        let buf = serde_json::to_vec(self)
            .map_err(|e| e.to_string())?;
        fs::write(CATALOG_FILE_NAME, buf)
            .map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub block_ids: Vec<u64>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self::with_block_ids(name, columns, vec![])
    }

    pub fn with_block_ids(name: String, columns: Vec<Column>, block_ids: Vec<u64>) -> Self {
        Table {
            name,
            columns,
            block_ids,
        }
    }
}

impl Hash for Table {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

impl Eq for Table {}

impl Borrow<str> for Table {
    fn borrow(&self) -> &str {
        self.name.borrow()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    name: String,
    table: String,
    type_variant: TypeVariant,
    nullable: bool,
}

impl Column {
    pub fn new(name: String, table: String, type_variant: TypeVariant, nullable: bool) -> Self {
        Column {
            name,
            table,
            type_variant,
            nullable,
        }
    }

    pub fn anon(type_variant: TypeVariant, nullable: bool) -> Self {
        Self::new(String::new(), String::new(), type_variant, nullable)
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_table(&self) -> &str {
        &self.table
    }

    pub fn get_type_variant(&self) -> &TypeVariant {
        &self.type_variant
    }

    pub fn into_type_variant(self) -> TypeVariant {
        self.type_variant
    }

    pub fn get_nullable(&self) -> bool {
        self.nullable
    }
}
