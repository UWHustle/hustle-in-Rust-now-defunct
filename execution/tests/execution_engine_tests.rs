#[cfg(test)]
mod execution_engine_tests {
    use hustle_execution::ExecutionEngine;
    use std::sync::Arc;
    use hustle_catalog::{Catalog, Table, Column};
    use hustle_common::plan::{Plan, Literal, Expression, QueryOperator, Query};
    use hustle_types::{TypeVariant, Int8, Char};

    #[test]
    fn create_table() {
        let catalog = Catalog::new();
        let engine = ExecutionEngine::new(Arc::new(catalog));

        let result = engine.execute_plan(Plan::CreateTable {
            table: Table::new(
                "create_table".to_owned(),
                vec![Column::new("col_a".to_owned(), TypeVariant::Int8(Int8), false)],
                vec![],
            )
        });

        assert_eq!(result, Ok(None));
    }

    #[test]
    fn insert() {
        let catalog = catalog_with_table_name("insert".to_owned());
        let table = catalog.get_table("insert").unwrap();
        let engine = ExecutionEngine::new(Arc::new(catalog));
        let result = insert_into_table(&engine, table);
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn project() {
        let catalog = catalog_with_table_name("insert".to_owned());
        let table = catalog.get_table("insert").unwrap();
        let engine = ExecutionEngine::new(Arc::new(catalog));
        insert_into_table(&engine, table.clone()).unwrap();

        let result = engine.execute_plan(Plan::Query {
            query: Query {
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        operator: QueryOperator::TableReference {
                            table: table.clone()
                        },
                        output: table.columns.clone()
                    }),
                    cols: vec![1]
                },
                output: vec![table.columns[1].clone()]
            }
        });
    }

    fn insert_into_table(engine: &ExecutionEngine, table: Table) -> Result<Option<Table>, String> {
        engine.execute_plan(Plan::Insert {
            into_table: table,
            values: vec![
                Expression::Literal { literal: Literal::Int(1) },
                Expression::Literal { literal: Literal::String("a".to_owned()) },
            ]
        })
    }

    fn catalog_with_table_name(name: String) -> Catalog {
        let catalog = Catalog::new();
        catalog.create_table(Table::new(
            name,
            vec![
                Column::new("col_a".to_owned(), TypeVariant::Int8(Int8), false),
                Column::new("col_b".to_owned(), TypeVariant::Char(Char::new(1)), false),
            ],
            vec![]
        )).unwrap();
        catalog
    }
}