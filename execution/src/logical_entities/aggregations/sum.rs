use logical_entities::column::Column;
use logical_entities::schema::Schema;
use logical_entities::relation::Relation;

use logical_entities::aggregations::AggregationTrait;

#[derive(Clone, Debug, PartialEq)]
pub struct Sum {
    input_relation: Relation,
    column : Column,
    running_total: Vec<u8>
}

impl Sum {
    pub fn new(relation: Relation, column:Column) -> Self {
        Sum {
            input_relation: relation,
            column: column,
            running_total: vec!()
        }
    }
}

impl AggregationTrait for Sum {
    fn input_relation(&self) -> Relation {
        return self.input_relation.clone();
    }

    fn group_by_columns(&self) -> Vec<Column> {
        let mut my_columns = self.input_relation.get_columns().clone();
        my_columns.retain(|col| col.get_name() != self.column.get_name() );
        return my_columns;
    }

    fn output_schema(&self) -> Schema {
        let col = Column::new("sum".to_string(), 8);

        let mut my_columns = self.group_by_columns();
        my_columns.push(col);

        Schema::new(my_columns)
    }

    fn initialize(&mut self) -> () {
        self.running_total = vec!();
    }

    fn consider_value(&mut self, data: Vec<u8>, column: Column) -> () {
        if column.get_name() == self.input_relation.get_columns().first().unwrap().get_name() {
            self.running_total = column.get_datatype().sum(&self.running_total, &data).0;
        }
    }

    fn output(&self) -> (Vec<u8>) {
        self.running_total.clone()
    }
}
