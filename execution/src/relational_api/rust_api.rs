use logical_entities::column::Column;
use logical_entities::predicates::comparison::Comparison;
use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use physical_operators::aggregate::*;
use physical_operators::export_csv::ExportCsv;
use physical_operators::import_csv::ImportCsv;
use physical_operators::join::Join;
use physical_operators::limit::Limit;
use physical_operators::print::Print;
use physical_operators::project::Project;
use physical_operators::Operator;
use type_system::operators::*;
use type_system::data_type::DataType;

extern crate storage;
use self::storage::StorageManager;

// This is semi-temporary
static mut STORAGE_MANAGER: *mut StorageManager = 0 as *mut StorageManager;

pub struct ImmediateRelation<'a> {
    relation: Relation,
    storage_manager: &'a StorageManager,
}

impl<'a> ImmediateRelation<'a> {
    pub fn new(col_names: Vec<&str>, col_type_names: Vec<&str>) -> Self {
        if col_names.len() != col_type_names.len() {
            panic!("Number of types does not match number of columns");
        }
        let mut columns: Vec<Column> = vec![];
        for i in 0..col_names.len() {
            let type_id = DataType::from_str(col_type_names[i]);
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
            ImmediateRelation {
                relation: Relation::new(&name, schema),
                storage_manager: &*STORAGE_MANAGER,
            }
        }
    }

    pub fn get_name(&self) -> &str {
        self.relation.get_name()
    }

    /// Replaces current data in the relation with data from the Hustle file
    /// TODO: Pull schema from the catalog
    pub fn import_hustle(&self, name: &str) {
        let import_relation = Relation::new(name, self.relation.get_schema().clone());
        let data = self.storage_manager.get(import_relation.get_name()).unwrap();
        self.storage_manager.put(self.relation.get_name(), &data);
    }

    pub fn export_hustle(&self, name: &str) {
        let data = self.storage_manager.get(self.relation.get_name()).unwrap();
        self.storage_manager.put(name, &data);
    }

    /// Replaces current data in the relation with data from the CSV file
    pub fn import_csv(&self, filename: &str) {
        let import_csv_op = ImportCsv::new(String::from(filename), self.relation.clone());
        import_csv_op.execute(&self.storage_manager);
    }

    pub fn export_csv(&self, filename: &str) {
        let export_csv_op = ExportCsv::new(String::from(filename), self.relation.clone());
        export_csv_op.execute(&self.storage_manager);
    }

    pub fn aggregate(
        &self,
        agg_col_name: &str,
        group_by_col_names: Vec<&str>,
        agg_name: &str) -> Self
    {
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
            relation: agg_op.execute(&self.storage_manager),
            storage_manager: self.storage_manager,
        }
    }

    pub fn join(&self, other: &ImmediateRelation) -> Self {
        let join_op = Join::new(self.relation.clone(), other.relation.clone());
        ImmediateRelation {
            relation: join_op.execute(&self.storage_manager),
            storage_manager: self.storage_manager,
        }
    }

    pub fn limit(&self, limit: u32) -> Self {
        let limit_op = Limit::new(self.relation.clone(), limit);
        ImmediateRelation {
            relation: limit_op.execute(self.storage_manager),
            storage_manager: self.storage_manager,
        }
    }

    pub fn print(&self) {
        let print_op = Print::new(self.relation.clone());
        print_op.execute(self.storage_manager);
    }

    pub fn project(&self, col_names: Vec<&str>) -> Self {
        let columns = self.relation.columns_from_names(col_names);
        let project_op = Project::pure_project(self.relation.clone(), columns);
        ImmediateRelation {
            relation: project_op.execute(self.storage_manager),
            storage_manager: self.storage_manager,
        }
    }

    /// Accepts predicate strings of the form "<column> <operator> <literal>"
    pub fn select(&self, predicate: &str) -> Self {
        let tokens: Vec<&str> = predicate.split(' ').collect();
        let col_name = tokens[0];
        let column = self.relation.column_from_name(col_name);
        let comparator = Comparator::from_str(tokens[1]);
        let value = column.get_datatype().parse(tokens[2]);
        let predicate = Comparison::new(column, comparator, value);

        let project_op = Project::new(
            self.relation.clone(),
            self.relation.get_columns().clone(),
            Box::new(predicate),
        );
        ImmediateRelation {
            relation: project_op.execute(self.storage_manager),
            storage_manager: self.storage_manager,
        }
    }
}

impl<'a> Drop for ImmediateRelation<'a> {
    fn drop(&mut self) {
        match std::fs::remove_file(self.relation.get_filename()) {
            Ok(_) => return,
            Err(_) => println!("Unable to delete file for relation {}", self.get_name()),
        }
    }
}
