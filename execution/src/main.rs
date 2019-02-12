pub mod logical_entities;
pub mod physical_operators;
pub mod storage_manager;
pub mod physical_plan;
pub mod type_system;
pub mod test_helpers;

use physical_plan::parser::parse;

fn main() {
    run_select_sum_a_comma_b_from_t_group_by_b();
}

fn run_select_sum_a_comma_b_from_t_group_by_b() {
    let plan_str = r#"
        {
          "json_name": "TopLevelPlan",
          "plan": {
            "json_name": "Selection",
            "has_repartition": "false",
            "input": {
              "json_name": "TableReference",
              "relation": "T",
              "alias": "t",
              "array": [
                {
                  "json_name": "AttributeReference",
                  "id": "0",
                  "name": "a",
                  "relation": "t",
                  "type": "Int NULL"
                },
                {
                  "json_name": "AttributeReference",
                  "id": "1",
                  "name": "b",
                  "relation": "t",
                  "type": "Int"
                }
              ]
            },
            "filter_predicate": {
              "json_name": "Equal",
              "attribute_reference": {
                "json_name": "AttributeReference",
                "id": "0",
                "name": "a",
                "relation": "t",
                "type": "Int NULL"
              },
              "literal": {
                "json_name": "Literal",
                "value": "5",
                "type": "Int"
              }
            },
            "project_expressions": [
              {
                "json_name": "AttributeReference",
                "id": "0",
                "name": "a",
                "relation": "t",
                "type": "Int NULL"
              }
            ]
          },
          "output_attributes": [
            {
              "json_name": "AttributeReference",
              "id": "0",
              "name": "a",
              "relation": "t",
              "type": "Int NULL"
            }
          ]
        }"#;

    let node = parse(plan_str);
    node.execute();
}