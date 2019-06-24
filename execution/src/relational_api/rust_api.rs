use logical_entities::column::Column;
use logical_entities::predicates::comparison::{Comparison, ComparisonOperand};
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
use physical_operators::select::Select;
use physical_operators::Operator;
use types::data_type::DataType;
use types::operators::*;
use types::Value;

extern crate storage;

use self::storage::StorageManager;

pub struct HustleConnection {
    storage_manager: StorageManager,
}

impl HustleConnection {
    pub fn new() -> Self {
        HustleConnection {
            storage_manager: StorageManager::new(),
        }
    }

    pub fn create_relation(
        &self,
        col_names: Vec<&str>,
        col_type_names: Vec<&str>,
    ) -> Result<ImmediateRelation, String> {
        if col_names.len() != col_type_names.len() {
            return Err(String::from(
                "number of types does not match number of columns",
            ));
        }
        let mut columns: Vec<Column> = vec![];
        for i in 0..col_names.len() {
            let data_type = DataType::from_str(col_type_names[i])?;
            columns.push(Column::new(col_names[i], data_type));
        }
        let schema = Schema::new(columns);
        let mut name = String::new();
        self.storage_manager.relational_engine().create_anon(&mut name, schema.to_size_vec());
        let output = ImmediateRelation {
            relation: Relation::new(&name, schema),
            storage_manager: &self.storage_manager,
        };
        Ok(output)
    }
}

pub struct ImmediateRelation<'a> {
    relation: Relation,
    storage_manager: &'a StorageManager,
}

impl<'a> ImmediateRelation<'a> {
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
            output.push(column.data_type().to_string());
        }
        output
    }

    pub fn copy_slice(&self, buffer: &[u8]) {
        let mut data: Vec<u8> = vec![0; buffer.len()];
        data.clone_from_slice(buffer);
        self.storage_manager
            .relational_engine()
            .get(self.relation.get_name())
            .unwrap()
            .bulk_write(&data);
    }

    pub fn get_data(&self) -> Option<Vec<u8>> {
        self.storage_manager
            .relational_engine()
            .get(self.relation.get_name())
            .map(|physical_relation| physical_relation.bulk_read())
    }

    /// Replaces current data in the relation with data from the Hustle file
    ///
    /// The schema is assumed to match that of the current ImmediateRelation
    pub fn import_hustle(&self, name: &str) -> Result<(), String> {
        self.storage_manager
            .relational_engine()
            .drop(self.relation.get_name());

        self.storage_manager
            .relational_engine()
            .copy(name, self.relation.get_name());

        Ok(())
    }

    pub fn export_hustle(&self, name: &str) -> Result<(), String> {
        self.storage_manager
            .relational_engine()
            .drop(name);

        self.storage_manager
            .relational_engine()
            .copy(self.relation.get_name(), name);

        Ok(())
    }

    /// Replaces current data in the relation with data from the CSV file
    pub fn import_csv(&self, filename: &str) -> Result<(), String> {
        let import_csv_op = ImportCsv::new(self.relation.clone(), filename);
        import_csv_op.execute(&self.storage_manager)?;
        Ok(())
    }

    pub fn export_csv(&self, filename: &str) -> Result<(), String> {
        let export_csv_op = ExportCsv::new(self.relation.clone(), filename);
        export_csv_op.execute(&self.storage_manager)?;
        Ok(())
    }

    pub fn aggregate(
        &self,
        agg_col_name: &str,
        group_by_col_names: Vec<&str>,
        agg_name: &str,
    ) -> Result<Self, String> {
        let agg_col_in = self.relation.column_from_name(agg_col_name)?;
        let agg_out_name = format!("{}({})", agg_name, agg_col_in.get_name());
        let agg_out_type = agg_col_in.data_type();
        let agg_col_out = Column::new(&agg_out_name, agg_out_type);

        let group_by_cols = self
            .relation
            .columns_from_names(group_by_col_names.clone())?;
        let mut output_col_names = vec![];
        output_col_names.push(agg_out_name.clone());
        for col in group_by_cols {
            output_col_names.push(col.get_name().to_string());
        }

        let agg_op = Aggregate::from_str(
            self.relation.clone(),
            agg_col_in,
            agg_col_out,
            output_col_names,
            agg_name,
        )?;
        let output = ImmediateRelation {
            relation: agg_op.execute(&self.storage_manager)?.unwrap(),
            storage_manager: self.storage_manager,
        };
        Ok(output)
    }

    pub fn insert(&self, value_strings: Vec<&str>) -> Result<(), String> {
        let schema = self.relation.get_schema();
        let columns = schema.get_columns();
        let mut values: Vec<Box<Value>> = vec![];
        for i in 0..columns.len() {
            let value = columns[i].data_type().parse(value_strings[i])?;
            values.push(value);
        }
        let row = Row::new(schema.clone(), values);
        Insert::new(self.relation.clone(), row).execute(&self.storage_manager)?;
        Ok(())
    }

    pub fn join(
        &self,
        other: &ImmediateRelation,
        l_col_names: Vec<&str>,
        r_col_names: Vec<&str>,
    ) -> Result<Self, String> {
        if l_col_names.len() != r_col_names.len() {
            return Err(String::from(
                "must have an equal number of left and right join columns",
            ));
        }

        let join_op = Join::new(
            self.relation.clone(),
            other.relation.clone(),
            self.relation.columns_from_names(l_col_names)?,
            other.relation.columns_from_names(r_col_names)?,
        );
        let output = ImmediateRelation {
            relation: join_op.execute(&self.storage_manager)?.unwrap(),
            storage_manager: self.storage_manager,
        };
        Ok(output)
    }

    pub fn limit(&self, limit: usize) -> Result<Self, String> {
        let limit_op = Limit::new(self.relation.clone(), limit);
        let output = ImmediateRelation {
            relation: limit_op.execute(self.storage_manager)?.unwrap(),
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
        let project_op = Project::new(self.relation.clone(), columns);
        let output = ImmediateRelation {
            relation: project_op.execute(self.storage_manager)?.unwrap(),
            storage_manager: self.storage_manager,
        };
        Ok(output)
    }

    /// Accepts predicate strings of the form "<column> <operator> <literal>"
    pub fn select(&self, predicate: &str) -> Result<Self, String> {
        let select_op = Select::new(
            self.relation.clone(),
            self.parse_predicate(predicate)?,
        );
        let output = ImmediateRelation {
            relation: select_op.execute(self.storage_manager)?.unwrap(),
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
        let value = column.data_type().parse(tokens[2])?;
        let predicate = Comparison::new(comparator, column, ComparisonOperand::Value(value));
        Ok(Box::new(predicate))
    }
}

impl<'a> Drop for ImmediateRelation<'a> {
    fn drop(&mut self) {
        self.storage_manager.relational_engine().drop(self.relation.get_name());
    }
}
