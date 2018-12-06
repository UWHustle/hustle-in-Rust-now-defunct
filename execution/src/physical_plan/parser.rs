
use physical_plan::node::Node;

use logical_entities::relation::Relation;
use logical_entities::schema::Schema;
use logical_entities::column::Column;

use logical_entities::types::DataTypeTrait;
use logical_entities::types::integer::IntegerType;

use physical_operators::Operator;
use physical_operators::print::Print;
use physical_operators::project::Project;

use std::rc::Rc;

extern crate regex;

pub fn parse(string_plan:&str) -> Node {
    let input_relation_regex = regex::Regex::new(r"\| \+\-input=TableReference\[relation=(.*),alias=(.*)\][\r\n]((\| \| (.*)[\r\n])+)").unwrap();
    let output_relation_regex = regex::Regex::new(r"\| \+\-project_expressions=[\r\n]((\|   \+\-(.*)+[\r\n])+)").unwrap();
    let input_relation_column_regex = regex::Regex::new(r"\| \| \+\-AttributeReference\[id=([0-9]+),name=(.*),relation=(.*),type=(.*)\]").unwrap();
    let output_relation_column_regex = regex::Regex::new(r"\|   \+\-AttributeReference\[id=([0-9]+),name=(.*),relation=(.*),type=(.*)\]").unwrap();
    let mut input_relations: Vec<Relation> = vec!();

    for rel_cap in input_relation_regex.captures_iter(string_plan) {

        let relation_name = &rel_cap[1];
        let relation_column_string = &rel_cap[3];

        let mut columns:Vec<Column> = vec!();


        for col_cap in input_relation_column_regex.captures_iter(relation_column_string) {
            let column_name: &str = &col_cap[2];
            let _column_type: &str = &col_cap[4];

        let column = Column::new(column_name.to_string(), _column_type.to_string());
            columns.push(column);
        }

        let schema = Schema::new(columns);
        let relation = Relation::new(relation_name.to_string(),schema);

        input_relations.push(relation);
    }


    let mut output_relation= Relation::null();

    for rel_cap in output_relation_regex.captures_iter(string_plan) {

        let relation_name = "output_relation";
        let relation_column_string = &rel_cap[1];

        let mut columns:Vec<Column> = vec!();


        for col_cap in output_relation_column_regex.captures_iter(relation_column_string) {
            let column_name: &str = &col_cap[2];
            let _column_type: &str = &col_cap[4];

            let column = Column::new(column_name.to_string(), _column_type.to_string());
            columns.push(column);
        }

        let schema = Schema::new(columns);
        output_relation = Relation::new(relation_name.to_string(),schema);
    }


    let project_operator = Project::new(input_relations.first().unwrap().clone(), output_relation.get_columns().clone());
    let print_operator = Print::new(project_operator.get_target_relation());

    let project_node = Node::new(Rc::new(project_operator), vec!());

    return Node::new(Rc::new(print_operator),vec!(Rc::new(project_node)));
}

pub fn type_string_to_type(_type_string:&str) -> impl DataTypeTrait {
    return IntegerType;
}
