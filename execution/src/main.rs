pub mod logical_entities;
pub mod physical_operators;
pub mod storage_manager;
pub mod physical_plan;
pub mod test_helpers;

use std::os::raw::c_char;

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
                "json_name": "Aggregate",
                "has_repartition": "false",
                "input": {
                  "json_name": "TableReference",
                  "relation": "T",
                  "alias": "t",
                  "": [
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
                "grouping_expressions": [
                  {
                    "json_name": "AttributeReference",
                    "id": "1",
                    "name": "b",
                    "relation": "t",
                    "type": "Int"
                  }
                ],
                "aggregate_expressions": [
                  {
                    "json_name": "Alias",
                    "id": "2",
                    "name": "",
                    "alias": "$aggregate0",
                    "relation": "$aggregate",
                    "type": "Long NULL",
                    "": {
                      "json_name": "AggregateFunction",
                      "function": "SUM",
                      "": [
                        {
                          "json_name": "AttributeReference",
                          "id": "0",
                          "name": "a",
                          "relation": "t",
                          "type": "Int NULL"
                        }
                      ]
                    }
                  }
                ]
              },
              "project_expressions": [
                {
                  "json_name": "Alias",
                  "id": "2",
                  "name": "",
                  "alias": "SUM(a)",
                  "relation": "",
                  "type": "Long NULL",
                  "": {
                    "json_name": "AttributeReference",
                    "id": "2",
                    "name": "",
                    "alias": "$aggregate0",
                    "relation": "$aggregate",
                    "type": "Long NULL"
                  }
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
            "output_attributes": [
              {
                "json_name": "AttributeReference",
                "id": "2",
                "name": "",
                "alias": "SUM(a)",
                "relation": "",
                "type": "Long NULL"
              },
              {
                "json_name": "AttributeReference",
                "id": "1",
                "name": "b",
                "relation": "t",
                "type": "Int"
              }
            ]
        }"#;

    let node = parse(plan_str);
    node.execute();
}