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
use type_system::data_type::DataType;
use type_system::operators::*;
use type_system::Value;

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
        let name = self.storage_manager.put_anon(&vec![]);
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
        self.storage_manager.put(self.relation.get_name(), &data);
    }

    pub fn get_data_size(&self) -> usize {
        match self.storage_manager.get(self.relation.get_name()) {
            Some(value) => (&value).len(),
            None => 0,
        }
    }

    pub fn get_data(&self) -> Option<Vec<u8>> {
        let schema = self.relation.get_schema();
        let record = self
            .storage_manager
            .get_with_schema(self.relation.get_name(), &schema.to_size_vec())?;

        let mut output: Vec<u8> = vec![];
        for block in record.blocks() {
            for row_i in 0..block.len() {
                for data in block.get_row(row_i).unwrap() {
                    output.extend_from_slice(data);
                }
            }
        }

        Some(output)
    }

    /// Replaces current data in the relation with data from the Hustle file
    ///
    /// The schema is assumed to match that of the current ImmediateRelation
    /// TODO: Pull schema from the catalog
    pub fn import_hustle(&self, name: &str) -> Result<(), String> {
        let schema = self.relation.get_schema();
        let import_record = match self
            .storage_manager
            .get_with_schema(name, &schema.to_size_vec())
        {
            Some(record) => record,
            None => return Err(format!("relation {} not found in storage manager", name)),
        };

        self.storage_manager.delete(self.relation.get_name());
        for block in import_record.blocks() {
            for row_i in 0..block.len() {
                for data in block.get_row(row_i).unwrap() {
                    self.storage_manager.append(self.relation.get_name(), data);
                }
            }
        }

        Ok(())
    }

    pub fn export_hustle(&self, name: &str) -> Result<(), String> {
        let schema = self.relation.get_schema();
        let record = self
            .storage_manager
            .get_with_schema(self.relation.get_name(), &schema.to_size_vec())
            .unwrap();

        self.storage_manager.delete(name);
        for block in record.blocks() {
            for row_i in 0..block.len() {
                for data in block.get_row(row_i).unwrap() {
                    self.storage_manager.append(name, data);
                }
            }
        }

        Ok(())
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
        agg_name: &str,
    ) -> Result<Self, String> {
        let agg_col_in = self.relation.column_from_name(agg_col_name)?;
        let agg_out_name = format!("{}({})", agg_name, agg_col_in.get_name());

        let group_by_cols = self
            .relation
            .columns_from_names(group_by_col_names.clone())?;
        let mut output_col_names = vec![];
        output_col_names.push(agg_out_name);
        for col in group_by_cols {
            output_col_names.push(col.get_name().to_string());
        }

        let agg_op = Aggregate::from_str(
            self.relation.clone(),
            agg_col_in,
            &agg_out_name,
            output_col_names,
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
            let value = columns[i].data_type().parse(value_strings[i])?;
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

    pub fn limit(&self, limit: usize) -> Result<Self, String> {
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
        let value = column.data_type().parse(tokens[2])?;
        let predicate = Comparison::new(column, comparator, value);
        Ok(Box::new(predicate))
    }
}

impl<'a> Drop for ImmediateRelation<'a> {
    fn drop(&mut self) {
        self.storage_manager.delete(self.relation.get_name());
    }
}
