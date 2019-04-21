use logical_entities::column::Column;
use logical_entities::predicates::comparison::Comparison;
use logical_entities::predicates::Predicate;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::aggregate::*;
use physical_operators::export_csv::ExportCsv;
use physical_operators::import_csv::ImportCsv;
use physical_operators::insert::Insert;
use physical_operators::join::Join;
use physical_operators::limit::Limit;
use physical_operators::print::Print;
use physical_operators::project::Project;
use physical_operators::Operator;
use storage::storage_manager;
use type_system::operators::*;
use type_system::data_type::DataType;
use type_system::Value;

extern crate storage;

use self::storage::StorageManager;

// This is semi-temporary
static mut STORAGE_MANAGER: *mut StorageManager = 0 as *mut StorageManager;

pub struct ImmediateRelation<'a> {
    relation: Relation,
    storage_manager: &'a StorageManager,
}

impl<'a> ImmediateRelation<'a> {
    pub fn new(col_names: Vec<&str>, col_type_names: Vec<&str>) -> Result<Self, String>  {
        if col_names.len() != col_type_names.len() {
            return Err(String::from("number of types does not match number of columns"));
        }
        let mut columns: Vec<Column> = vec![];
        for i in 0..col_names.len() {
            let type_id = DataType::from_str(col_type_names[i])?;
            columns.push(Column::new(String::from(col_names[i]), type_id));
        }

        unsafe {
            if STORAGE_MANAGER == 0 as *mut StorageManager {
                STORAGE_MANAGER = Box::into_raw(Box::new(StorageManager::new()));
            }
        }

        let schema = Schema::new(columns);
        unsafe {
            let name = (*STORAGE_MANAGER).put_anon(&vec![]);
            let output = ImmediateRelation {
                relation: Relation::new(&name, schema),
                storage_manager: &*STORAGE_MANAGER,
            };
            Ok(output)
        }
    }

    pub fn get_name(&self) -> &str {
        self.relation.get_name()
    }

    pub fn get_col_names(&self) -> Vec<String> {
        let mut output: Vec<String> = vec![];
        for column in self.relation.get_schema().get_columns() {
            output.push(String::from(column.get_name()));
        }
        output
    }

    pub fn get_col_type_names(&self) -> Vec<String> {
        let mut output: Vec<String> = vec![];
        for column in self.relation.get_schema().get_columns() {
            output.push(column.get_datatype().to_string());
        }
        output
    }

    pub fn copy_slice(&self, buffer: &[u8]) {
        let mut data: Vec<u8> = vec![0; buffer.len()];
        data.clone_from_slice(buffer);
        self.storage_manager.put(self.relation.get_name(), &data);
    }

    pub fn get_data_size(&self) -> usize {
        match self.storage_manager.get(self.relation.get_name()) {
            Some(value) => (&value).len(),
            None => 0,
        }
    }

    pub fn get_data(&self) -> Option<storage_manager::Value> {
        self.storage_manager.get(self.relation.get_name())
    }

    /// Replaces current data in the relation with data from the Hustle file
    /// TODO: Pull schema from the catalog
    pub fn import_hustle(&self, name: &str) -> Result<(), String> {
        let import_relation = Relation::new(name, self.relation.get_schema().clone());
        match self.storage_manager.get(import_relation.get_name()) {
            Some(data) => {
                self.storage_manager.put(self.relation.get_name(), &data);
                Ok(())
            },
            None => Err(format!("relation {} not found in storage manager", name)),
        }
    }

    pub fn export_hustle(&self, name: &str) -> Result<(), String> {
        match self.storage_manager.get(self.relation.get_name()) {
            Some(data) => Ok(self.storage_manager.put(name, &data)),
            None => Err(format!("relation {} not found in storage manager", name)),
        }
    }

    /// Replaces current data in the relation with data from the CSV file
    pub fn import_csv(&self, filename: &str) -> Result<(), String> {
        let import_csv_op = ImportCsv::new(String::from(filename), self.relation.clone());
        import_csv_op.execute(&self.storage_manager)?;
        Ok(())
    }

    pub fn export_csv(&self, filename: &str) -> Result<(), String> {
        let export_csv_op = ExportCsv::new(String::from(filename), self.relation.clone());
        export_csv_op.execute(&self.storage_manager)?;
        Ok(())
    }

    pub fn aggregate(
        &self,
        agg_col_name: &str,
        group_by_col_names: Vec<&str>,
        agg_name: &str) -> Result<Self, String>
    {
        let agg_col = self.relation.column_from_name(agg_col_name)?;
        let group_by_cols = self.relation.columns_from_names(group_by_col_names.clone())?;

        let mut project_col_names = group_by_col_names.clone();
        project_col_names.push(agg_col_name);
        let projected = self.project(project_col_names)?;

        let agg_op = Aggregate::from_str(
            projected.relation.clone(),
            agg_col.clone(),
            group_by_cols,
            agg_col.get_datatype(),
            agg_name,
        )?;
        let output = ImmediateRelation {
            relation: agg_op.execute(&self.storage_manager)?,
            storage_manager: self.storage_manager,
        };
        Ok(output)
    }

    pub fn insert(&self, value_strings: Vec<&str>) -> Result<(), String> {
        let schema = self.relation.get_schema();
        let columns = schema.get_columns();
        let mut values: Vec<Box<Value>> = vec![];
        for i in 0..columns.len() {
            let value = columns[i].get_datatype().parse(value_strings[i])?;
            values.push(value);
        }
        let row = Row::new(schema.clone(), values);
        Insert::new(self.relation.clone(), row).execute(&self.storage_manager)?;
        Ok(())
    }

    pub fn join(&self, other: &ImmediateRelation) -> Result<Self, String> {
        let join_op = Join::new(self.relation.clone(), other.relation.clone());
        let output = ImmediateRelation {
            relation: join_op.execute(&self.storage_manager)?,
            storage_manager: self.storage_manager,
        };
        Ok(output)
    }

    pub fn limit(&self, limit: u32) -> Result<Self, String> {
        let limit_op = Limit::new(self.relation.clone(), limit);
        let output = ImmediateRelation {
            relation: limit_op.execute(self.storage_manager)?,
            storage_manager: self.storage_manager,
        };
        Ok(output)
    }

    pub fn print(&self) -> Result<(), String> {
        let print_op = Print::new(self.relation.clone());
        print_op.execute(self.storage_manager)?;
        Ok(())
    }

    pub fn project(&self, col_names: Vec<&str>) -> Result<Self, String> {
        let columns = self.relation.columns_from_names(col_names)?;
        let project_op = Project::pure_project(self.relation.clone(), columns);
        let output = ImmediateRelation {
            relation: project_op.execute(self.storage_manager)?,
            storage_manager: self.storage_manager,
        };
        Ok(output)
    }

    /// Accepts predicate strings of the form "<column> <operator> <literal>"
    pub fn select(&self, predicate: &str) -> Result<Self, String> {
        let project_op = Project::new(
            self.relation.clone(),
            self.relation.get_columns().clone(),
            self.parse_predicate(predicate)?,
        );
        let output = ImmediateRelation {
            relation: project_op.execute(self.storage_manager)?,
            storage_manager: self.storage_manager,
        };
        Ok(output)
    }

    fn parse_predicate(&self, predicate: &str) -> Result<Box<Predicate>, String> {
        let tokens: Vec<&str> = predicate.split(' ').collect();
        if tokens.len() != 3 {
            return Err(String::from("predicate expressions must have three tokens"));
        }
        let col_name = tokens[0];
        let column = self.relation.column_from_name(col_name)?;
        let comparator = Comparator::from_str(tokens[1])?;
        let value = column.get_datatype().parse(tokens[2])?;
        let predicate = Comparison::new(column, comparator, value);
        Ok(Box::new(predicate))
    }
}

impl<'a> Drop for ImmediateRelation<'a> {
    fn drop(&mut self) {
        self.storage_manager.delete(self.relation.get_name());
    }
}
