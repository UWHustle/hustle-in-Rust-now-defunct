extern crate execution;

use execution::test_helpers::data_gen::generate_relation_into_hustle_and_sqlite3;

use execution::physical_plan::parser;

const RECORD_COUNT: usize = 32;


//#[test]
fn test_parser() {

    generate_relation_into_hustle_and_sqlite3(RECORD_COUNT, true);

    let root_node = parser::parse(
"
{
  \"json_name\": \"TopLevelPlan\",
  \"plan\": {
    \"json_name\": \"Selection\",
    \"has_repartition\": \"false\",
    \"input\": {
      \"json_name\": \"TableReference\",
      \"relation\": \"T\",
      \"alias\": \"t\",
      \"\": [
        {
          \"json_name\": \"AttributeReference\",
          \"id\": \"0\",
          \"name\": \"a\",
          \"relation\": \"t\",
          \"type\": \"Int NULL\"
        },
        {
          \"json_name\": \"AttributeReference\",
          \"id\": \"1\",
          \"name\": \"b\",
          \"relation\": \"t\",
          \"type\": \"Int\"
        }
      ]
    },
    \"project_expressions\": [
      {
        \"json_name\": \"AttributeReference\",
        \"id\": \"0\",
        \"name\": \"a\",
        \"relation\": \"t\",
        \"type\": \"Int NULL\"
      },
      {
        \"json_name\": \"AttributeReference\",
        \"id\": \"1\",
        \"name\": \"b\",
        \"relation\": \"t\",
        \"type\": \"Int\"
      }
    ]
  },
  \"output_attributes\": [
    {
      \"json_name\": \"AttributeReference\",
      \"id\": \"0\",
      \"name\": \"a\",
      \"relation\": \"t\",
      \"type\": \"Int NULL\"
    },
    {
      \"json_name\": \"AttributeReference\",
      \"id\": \"1\",
      \"name\": \"b\",
      \"relation\": \"t\",
      \"type\": \"Int\"
    }
  ]
}
");
    root_node.execute();

}
