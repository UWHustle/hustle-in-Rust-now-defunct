use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::aggregate::*;
use physical_operators::export_csv::ExportCsv;
use physical_operators::import_csv::ImportCsv;
use physical_operators::insert::Insert;
use physical_operators::print::Print;
use physical_operators::project::Project;
use physical_operators::Operator;
use std::fs::remove_file;
use storage_manager::StorageManager;
use type_system::operators::*;
use type_system::type_id::TypeID;
use type_system::Value;

// TODO: Unsafe - wrap this in a mutex
static mut CURRENT_ID: u32 = 0;

struct ImmediateRelation {
    relation: Relation,
}

impl ImmediateRelation {
    /// Creates an empty in-memory relation
    pub fn new(col_names: Vec<&str>, col_type_names: Vec<&str>) -> Self {
        if col_names.len() != col_type_names.len() {
            panic!("Number of types does not match number of columns");
        }
        let mut columns: Vec<Column> = vec![];
        for i in 0..col_names.len() {
            let type_id = TypeID::from_str(col_type_names[i]);
            columns.push(Column::new(String::from(col_names[i]), type_id));
        }

        let name = unsafe {
            CURRENT_ID += 1;
            format!("${}", CURRENT_ID)
        };
        let schema = Schema::new(columns);
        ImmediateRelation {
            relation: Relation::new(name, schema),
        }
    }

    /// Replaces current data in the relation with data from the CSV file
    pub fn load_csv(&self, filename: &str) {
        let import_csv_op = ImportCsv::new(String::from(filename), self.relation.clone());
        import_csv_op.execute();
    }

    /// Replaces current data in the relation with data from the Hustle file
    ///
    /// TODO: We should have a method that creates a new ImmediateRelation from a
    /// Hustle file based on the schema from the catalog
    pub fn load_database(&self, name: &str) {
        let data_relation = Relation::new(String::from(name), self.relation.get_schema().clone());
        let data = StorageManager::get_full_data(&data_relation);
        let mut copy = StorageManager::create_relation(&self.relation, data.len());
        copy.clone_from_slice(&data);
        StorageManager::flush(&copy);
    }

    pub fn to_csv(&self, filename: &str) {
        let export_csv_op = ExportCsv::new(String::from(filename), self.relation.clone());
        export_csv_op.execute();
    }

    pub fn to_database(&self, name: &str) {
        let copy_relation = Relation::new(String::from(name), self.relation.get_schema().clone());
        let data = StorageManager::get_full_data(&self.relation);
        let mut copy = StorageManager::create_relation(&copy_relation, data.len());
        copy.clone_from_slice(&data);
        StorageManager::flush(&copy);
    }

    pub fn insert(&self, values: Vec<Box<Value>>) {
        if values.len() != self.relation.get_columns().len() {
            panic!("Incorrect number of values in row");
        }
        let row = Row::new(self.relation.get_schema().clone(), values);
        let insert_op = Insert::new(self.relation.clone(), row);
        insert_op.execute();
    }

    pub fn project(&self, col_names: Vec<&str>) -> Self {
        let columns = self.relation.columns_from_names(col_names);
        let project_op = Project::pure_project(self.relation.clone(), columns);
        ImmediateRelation {
            relation: project_op.execute(),
        }
    }

    /// Note that this function only accepts predicates of the form <column> <operator> <literal>
    pub fn select(&self, predicate: &str) -> Self {
        let tokens: Vec<&str> = predicate.split(' ').collect();
        let col_name = tokens[0];
        let column = self.relation.column_from_name(col_name);
        let comparator = Comparator::from_str(tokens[1]);
        let value = column.get_datatype().parse(tokens[2]);

        let project_op = Project::new(
            self.relation.clone(),
            self.relation.get_columns().clone(),
            col_name,
            true,
            comparator,
            &*value,
        );
        ImmediateRelation {
            relation: project_op.execute(),
        }
    }

    pub fn print(&self) {
        let print_op = Print::new(self.relation.clone());
        print_op.execute();
    }

    pub fn aggregate(
        &self,
        agg_col_name: &str,
        group_by_col_names: Vec<&str>,
        agg_name: &str,
    ) -> Self {
        let agg_col = self.relation.column_from_name(agg_col_name);
        let group_by_cols = self.relation.columns_from_names(group_by_col_names);
        let agg_op = Aggregate::from_str(
            self.relation.clone(),
            agg_col.clone(),
            group_by_cols,
            agg_col.get_datatype(),
            agg_name,
        );
        ImmediateRelation {
            relation: agg_op.execute(),
        }
    }
}

impl Drop for ImmediateRelation {
    fn drop(&mut self) {
        match remove_file(self.relation.get_filename()) {
            _ => return,
        }
    }
}
