use logical_entities::column::Column;
use logical_entities::schema::Schema;
use logical_entities::relation::Relation;

use logical_entities::types::integer::IntegerType;
use logical_entities::types::DataTypeTrait;

use logical_entities::aggregations::AggregationTrait;

#[derive(Clone, Debug, PartialEq)]
pub struct Count {
    input_relation: Relation,
    column: Column,
    group_by: Vec<Column>,
    running_total: u8
}

impl Count {
    pub fn new(relation: Relation, column: Column, group_by: Vec<Column>) -> Self {
        Count {
            input_relation: relation,
            column,
            group_by,
            running_total: 0
        }
    }
}

impl AggregationTrait for Count {
    fn input_relation(&self) -> Relation {
        return self.input_relation.clone();
    }

    fn group_by_columns(&self) -> Vec<Column> {
        return self.group_by.clone();
    }

    fn output_schema(&self) -> Schema {
        let col = Column::new(format!("COUNT({})", self.column.get_name()), "Int".to_string());
        let mut my_columns = self.group_by_columns();
        my_columns.push(col);

        Schema::new(my_columns)
    }

    fn initialize(&mut self) -> () {
        self.running_total = 0;
    }

    #[allow(unused_variables)]
    fn consider_value(&mut self, data: Vec<u8>, column: Column) -> () {
        self.running_total += 1;
    }

    fn output(&self) -> (Vec<u8>) {
        let output = self.running_total.to_string();
        let (output_bits, _size) = IntegerType::parse_and_marshall(output);
        output_bits
    }
}
