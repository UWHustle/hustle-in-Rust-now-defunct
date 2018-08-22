
pub mod relation;
pub mod column;
pub mod row;
pub mod schema;
use logical_operator::relation::Relation;

pub trait Operator {
    fn output_relation(&self) -> &Relation;
}

struct Select {
    relation: Relation,
}

impl Select {
    pub fn new(relation: Relation) -> Self {
        let operator = Select {
            relation:relation,
        };

        operator
    }
}
impl Operator for Select {
    fn output_relation(&self) -> &Relation {
        &self.relation
    }
}

struct Join {
    left_relation: Relation,
    right_relation: Relation,
    output_relation: Relation,
}

impl Join {
    pub fn new(left_relation: Relation, right_relation: Relation) -> Self {
        let mut left_columns = left_relation.get_columns().clone();
        let right_columns = right_relation.get_columns().clone();
        left_columns.extend(right_columns.iter().cloned());

        let output_relation = Relation::new(
            format!("{}_join_{}", left_relation.get_name(), right_relation.get_name()),
            left_columns,
        );

        Join {
            left_relation,
            right_relation,
            output_relation,
        }
    }
}
impl Operator for Join {
    fn output_relation(&self) -> &Relation {
        &self.output_relation
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn select_create() {
        use logical_operator::Select;
        use logical_operator::relation::Relation;
        use logical_operator::Operator;
        use logical_operator::column::Column;

        let s = Select::new(
            Relation::new(
                String::from("TableName"),
                vec![Column::new(String::from("a"),8)])
        );
        assert_eq!(s.output_relation().get_name(), &"TableName");
        assert_eq!(s.output_relation().get_columns()[0].get_name(), "a");
    }

    #[test]
    fn join_create() {
        use logical_operator::Join;
        use logical_operator::relation::Relation;
        use logical_operator::Operator;
        use logical_operator::column::Column;

        let s = Join::new(
            Relation::new(
                String::from("Left"),
                vec![Column::new(String::from("a"),8),Column::new(String::from("b"),8)]),
            Relation::new(
            String::from("Right"),
            vec![Column::new(String::from("c"),8),Column::new(String::from("d"),8)])
        );
        assert_eq!(s.output_relation().get_name(), &"Left_join_Right");
        assert_eq!(s.output_relation().get_columns()[0].get_name(), "a");
        assert_eq!(s.output_relation().get_columns()[1].get_name(), "b");
        assert_eq!(s.output_relation().get_columns()[2].get_name(), "c");
        assert_eq!(s.output_relation().get_columns()[3].get_name(), "d");

    }
}