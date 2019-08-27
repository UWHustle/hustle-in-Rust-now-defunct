// cargo test -- --test-threads=1

#[cfg(test)]
mod resolver_tests {
    use hustle_resolver_new::Resolver;
    use sqlparser::parser::Parser;
    use sqlparser::dialect::GenericDialect;
//    use hustle_common::{Plan, Query, QueryOperator, Expression, ComparativeVariant, ConnectiveVariant, AggregateFunction, AggregateFunctionVariant, Column, Table};
    use hustle_common::*;

    #[test]
    fn select1() {
        setup();
        let sql = "SELECT a, b FROM T;";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let column_a = table.columns.iter().find(|c| c.name.eq("a")).unwrap().clone();
        let column_b = table.columns.iter().find(|c| c.name.eq("b")).unwrap().clone();

        let expected = Plan::Query {
            query: Query {
                output: vec![column_a, column_b],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: table.columns.clone(),
                        operator: QueryOperator::TableReference { table },
                    })
                },
            }
        };
        assert_eq!(plan, expected);
        teardown();
    }

    #[test]
    fn select2() {
        setup();
        let sql = "SELECT T.a, T.b FROM T;";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let column_a = table.columns.iter().find(|c| c.name.eq("a")).unwrap().clone();
        let column_b = table.columns.iter().find(|c| c.name.eq("b")).unwrap().clone();

        let expected = Plan::Query {
            query: Query {
                output: vec![column_a, column_b],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: table.columns.clone(),
                        operator: QueryOperator::TableReference { table },
                    })
                },
            }
        };
        assert_eq!(plan, expected);
        teardown();
    }

    /// select and where
    #[test]
    fn select3() {
        setup();
        let sql = "SELECT a, b FROM T WHERE a > 1 AND a < 3 AND b >= 2 AND b <= 4;";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let columns = table.columns.clone();
//        let column_a_idx = columns.iter().position(|c| c.name.eq("a")).unwrap();
//        let column_b_idx = columns.iter().position(|c| c.name.eq("b")).unwrap();
        let expected = Plan::Query {
            query: Query {
                output: vec![
                    columns[0].clone(),
                    columns[1].clone()
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: columns.clone(),
                        operator: QueryOperator::Select {
                            filter: Box::new(Expression::Connective {
                                variant: ConnectiveVariant::And,
                                terms: vec![
                                    Expression::Connective {
                                        variant: ConnectiveVariant::And,
                                        terms: vec![
                                            Expression::Connective {
                                                variant: ConnectiveVariant::And,
                                                terms: vec![
                                                    Expression::Comparative {
                                                        variant: ComparativeVariant::Gt,
                                                        left: Box::new(Expression::ColumnReference {
                                                            table: 0,
                                                            column: 0,
                                                        }),
                                                        right: Box::new(Expression::Literal {
                                                            value: "1".to_string(),
                                                            literal_type: "Long".to_string()
                                                        })
                                                    },
                                                    Expression::Comparative {
                                                        variant: ComparativeVariant::Lt,
                                                        left: Box::new(Expression::ColumnReference {
                                                            table: 0,
                                                            column: 0,
                                                        }),
                                                        right: Box::new(Expression::Literal {
                                                            value: "3".to_string(),
                                                            literal_type: "Long".to_string()
                                                        })
                                                    },
                                                ]
                                            },
                                            Expression::Comparative {
                                                variant: ComparativeVariant::Ge,
                                                left: Box::new(Expression::ColumnReference {
                                                    table: 0,
                                                    column: 1,
                                                }),
                                                right: Box::new(Expression::Literal {
                                                    value: "2".to_string(),
                                                    literal_type: "Long".to_string()
                                                })
                                            }
                                        ]
                                    },
                                    Expression::Comparative {
                                        variant: ComparativeVariant::Le,
                                        left: Box::new(Expression::ColumnReference {
                                            table: 0,
                                            column: 1,
                                        }),
                                        right: Box::new(Expression::Literal {
                                            value: "4".to_string(),
                                            literal_type: "Long".to_string()
                                        })
                                    }
                                ]
                            }),
                            input: Box::new(Query {
                                output: columns,
                                operator: QueryOperator::TableReference { table },
                            })
                        }
                    }),
                },
            }
        };
        assert_eq!(plan, expected);
        teardown();
    }

    #[test]
    fn select4() {
        setup();
        let sql = "SELECT T.a, T.b FROM T WHERE T.a > 1 AND a < 3 AND T.b >= 2 AND b <= 4;";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let columns = table.columns.clone();
//        let column_a_idx = columns.iter().position(|c| c.name.eq("a")).unwrap();
//        let column_b_idx = columns.iter().position(|c| c.name.eq("b")).unwrap();
        let expected = Plan::Query {
            query: Query {
                output: vec![
                    columns[0].clone(),
                    columns[1].clone()
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: columns.clone(),
                        operator: QueryOperator::Select {
                            filter: Box::new(Expression::Connective {
                                variant: ConnectiveVariant::And,
                                terms: vec![
                                    Expression::Connective {
                                        variant: ConnectiveVariant::And,
                                        terms: vec![
                                            Expression::Connective {
                                                variant: ConnectiveVariant::And,
                                                terms: vec![
                                                    Expression::Comparative {
                                                        variant: ComparativeVariant::Gt,
                                                        left: Box::new(Expression::ColumnReference {
                                                            table: 0,
                                                            column: 0,
                                                        }),
                                                        right: Box::new(Expression::Literal {
                                                            value: "1".to_string(),
                                                            literal_type: "Long".to_string()
                                                        })
                                                    },
                                                    Expression::Comparative {
                                                        variant: ComparativeVariant::Lt,
                                                        left: Box::new(Expression::ColumnReference {
                                                            table: 0,
                                                            column: 0,
                                                        }),
                                                        right: Box::new(Expression::Literal {
                                                            value: "3".to_string(),
                                                            literal_type: "Long".to_string()
                                                        })
                                                    },
                                                ]
                                            },
                                            Expression::Comparative {
                                                variant: ComparativeVariant::Ge,
                                                left: Box::new(Expression::ColumnReference {
                                                    table: 0,
                                                    column: 1,
                                                }),
                                                right: Box::new(Expression::Literal {
                                                    value: "2".to_string(),
                                                    literal_type: "Long".to_string()
                                                })
                                            }
                                        ]
                                    },
                                    Expression::Comparative {
                                        variant: ComparativeVariant::Le,
                                        left: Box::new(Expression::ColumnReference {
                                            table: 0,
                                            column: 1,
                                        }),
                                        right: Box::new(Expression::Literal {
                                            value: "4".to_string(),
                                            literal_type: "Long".to_string()
                                        })
                                    }
                                ]
                            }),
                            input: Box::new(Query {
                                output: columns,
                                operator: QueryOperator::TableReference { table },
                            })
                        }
                    }),
                },
            }
        };
        assert_eq!(plan, expected);
        teardown();
    }

    /// project on cartesian product
    #[test]
    fn select5() {
        setup();
        let sql = "SELECT T.a, R.e FROM T, R;";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table_t = resolver.catalog.get_table("T").unwrap().clone();
        let table_r = resolver.catalog.get_table("R").unwrap().clone();

        let expected = Plan::Query {
            query: Query {
                output: vec![
                    Column::new("a".to_string(), "INT1".to_string(), "T".to_string()),
                    Column::new("e".to_string(), "FLOAT4".to_string(), "R".to_string()),
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: [&table_t.columns[..], &table_r.columns[..]].concat(),
                        operator: QueryOperator::Join {
                            input: vec![
                                Query {
                                    output: table_t.columns.clone(),
                                    operator: QueryOperator::TableReference {table: table_t},
                                },
                                Query {
                                    output: table_r.columns.clone(),
                                    operator: QueryOperator::TableReference {table: table_r},
                                }
                            ],
                            filter: None
                        }
                    })
                }
            }
        };
        assert_eq!(plan, expected, "plan:\n{:#?}\n\nexpected:\n{:#?}", plan, expected);
        teardown();
    }

    /// project on cartesian product
    #[test]
    fn select6() {
        setup();
        let sql = "SELECT a, e FROM T, R;";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table_t = resolver.catalog.get_table("T").unwrap().clone();
        let table_r = resolver.catalog.get_table("R").unwrap().clone();

        let expected = Plan::Query {
            query: Query {
                output: vec![
                    Column::new("a".to_string(), "INT1".to_string(), "T".to_string()),
                    Column::new("e".to_string(), "FLOAT4".to_string(), "R".to_string()),
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: [&table_t.columns[..], &table_r.columns[..]].concat(),
                        operator: QueryOperator::Join {
                            input: vec![
                                Query {
                                    output: table_t.columns.clone(),
                                    operator: QueryOperator::TableReference {table: table_t},
                                },
                                Query {
                                    output: table_r.columns.clone(),
                                    operator: QueryOperator::TableReference {table: table_r},
                                }
                            ],
                            filter: None
                        }
                    })
                }
            }
        };
        assert_eq!(plan, expected, "plan:\n{:#?}\n\nexpected:\n{:#?}", plan, expected);
        teardown();
    }

    /// join
    #[test]
    fn select7() {
        setup();
        let sql = "SELECT T.a, R.e FROM T, R WHERE T.b = R.d;";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table_t = resolver.catalog.get_table("T").unwrap().clone();
        let table_r = resolver.catalog.get_table("R").unwrap().clone();

        let expected = Plan::Query {
            query: Query {
                output: vec![
                    Column::new("a".to_string(), "INT1".to_string(), "T".to_string()),
                    Column::new("e".to_string(), "FLOAT4".to_string(), "R".to_string()),
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: [&table_t.columns[..], &table_r.columns[..]].concat(),
                        operator: QueryOperator::Select {
                            input: Box::new(Query {
                                output: [&table_t.columns[..], &table_r.columns[..]].concat(),
                                operator: QueryOperator::Join {
                                    input: vec![
                                        Query {
                                            output: table_t.columns.clone(),
                                            operator: QueryOperator::TableReference { table: table_t },
                                        },
                                        Query {
                                            output: table_r.columns.clone(),
                                            operator: QueryOperator::TableReference { table: table_r },
                                        }
                                    ],
                                    filter: None
                                }
                            }),
                            filter: Box::new(Expression::Comparative {
                                variant: ComparativeVariant::Eq,
                                left: Box::new(Expression::ColumnReference {
                                    table: 0,
                                    column: 1
                                }),
                                right: Box::new(Expression::ColumnReference {
                                    table: 0,
                                    column: 3
                                })
                            })
                        }
                    })
                }
            }

        };
        assert_eq!(plan, expected);
        teardown();
    }

    #[test]
    fn insert() {
        setup();
        let sql = "INSERT INTO T VALUES (1, 2.3, '172.16.254.1')";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let expected = Plan::Insert {
            into_table: table,
            values: vec![
                Expression::Literal { value: "1".to_string(), literal_type: "Long".to_string() },
                Expression::Literal { value: "2.3".to_string(), literal_type: "Double".to_string() },
                Expression::Literal { value: "172.16.254.1".to_string(), literal_type: "SingleQuotedString".to_string() },
            ]
        };
        assert_eq!(plan, expected);
        teardown();
    }

    #[test]
    fn update_columns() {
        setup();
        let sql = "UPDATE T SET a = 2, b = 3";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let expected = Plan::Update {
            columns: vec![
                table.columns.iter().find(|c| c.name.eq("a")).unwrap().clone(),
                table.columns.iter().find(|c| c.name.eq("b")).unwrap().clone(),
            ],
            table,
            assignments: vec![
                Expression::Literal { value: "2".to_string(), literal_type: "Long".to_string() },
                Expression::Literal { value: "3".to_string(), literal_type: "Long".to_string() }
            ],
            filter: None
        };
        assert_eq!(plan, expected);
        teardown();
    }

    #[test]
    fn update_rows() {
        setup();
        let sql = "UPDATE T SET a = 2 WHERE a = 1";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let column_idx = table.columns.iter().position(|c| c.name.eq("a")).unwrap();
        let column = table.columns[column_idx].clone();
        assert_eq!(column_idx, 0);

        let expected = Plan::Update {
            table,
            columns: vec![column],
            assignments: vec![Expression::Literal {
                value: "2".to_string(),
                literal_type: "Long".to_string()
            }],
            filter: Some(Box::new(
                Expression::Comparative {
                    variant: ComparativeVariant::Eq,
                    left: Box::new(Expression::ColumnReference { table: 0, column: column_idx }),
                    right: Box::new(Expression::Literal {
                        value: "1".to_string(),
                        literal_type: "Long".to_string()
                    })
                }
            ))
        };
        assert_eq!(plan, expected);
        teardown();
    }

    #[test]
    fn delete_column() {
        setup();
        let sql = "DELETE FROM T WHERE a";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let column_idx = table.columns.iter().position(|c| c.name.eq("a")).unwrap();
        assert_eq!(column_idx, 0);

        let expected = Plan::Delete {
            from_table: table,
            filter: Some(Box::new(Expression::ColumnReference { table: 0, column: column_idx }))
        };
        assert_eq!(plan, expected);
        teardown();
    }

    #[test]
    fn delete_rows() {
        setup();
        let sql = "DELETE FROM T WHERE a = 1";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };

        let table = resolver.catalog.get_table("T").unwrap().clone();
        let column_idx = table.columns.iter().position(|c| c.name.eq("a")).unwrap();
        assert_eq!(column_idx, 0);

        let expected = Plan::Delete {
            from_table: table,
            filter: Some(Box::new(Expression::Comparative {
                variant: ComparativeVariant::Eq,
                left: Box::new(Expression::ColumnReference { table: 0, column: column_idx }),
                right: Box::new(Expression::Literal {
                    value: "1".to_string(),
                    literal_type: "Long".to_string()
                })
            }))
        };
        assert_eq!(plan, expected);
        teardown();
    }

    fn setup() {
        let mut resolver = Resolver::new();
        let table_t = Table::new(
            "T".to_string(),
            vec![
                Column::new("a".to_string(), "INT1".to_string(), "T".to_string()),
                Column::new("b".to_string(), "FLOAT4".to_string(), "T".to_string()),
                Column::new("c".to_string(), "IPv4".to_string(), "T".to_string()),
            ]
        );
        let table_r = Table::new(
            "R".to_string(),
            vec![
                Column::new("d".to_string(), "INT1".to_string(), "R".to_string()),
                Column::new("e".to_string(), "FLOAT4".to_string(), "R".to_string()),
                Column::new("f".to_string(), "IPv4".to_string(), "R".to_string()),
            ]
        );
        assert!(resolver.catalog.create_table(table_t).is_ok());
        assert!(resolver.catalog.create_table(table_r).is_ok());
    }

    fn teardown() {
        let mut resolver = Resolver::new();
        assert!(resolver.catalog.drop_table("T").is_ok());
        assert!(resolver.catalog.drop_table("R").is_ok());
        assert_eq!(resolver.catalog.table_exists("T"), false);
        assert_eq!(resolver.catalog.table_exists("R"), false);
    }

    #[test]
    fn create_table() {
        let sql_create = "CREATE TABLE T (a INT1, b FLOAT4, c IPv4);";
        let mut resolver = Resolver::new();
        assert!(resolver.catalog.drop_table("T").is_ok());
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql_create.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };
        let expected = Plan::CreateTable {
            table: Table::new(
                "T".to_string(),
                vec![
                    Column::new("a".to_string(), "INT1".to_string(), "T".to_string()),
                    Column::new("b".to_string(), "FLOAT4".to_string(), "T".to_string()),
                    Column::new("c".to_string(), "IPv4".to_string(), "T".to_string()),
                ]
            )
        };
        assert_eq!(plan, expected);

        let sql_drop = "DROP TABLE T;";
        let table = resolver.catalog.get_table("T").unwrap().clone();
        let ast = match Parser::parse_sql(&dialect, sql_drop.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };
        let expected = Plan::DropTable { table };
        assert_eq!(plan, expected);
    }

    #[test]
    fn drop_not_exist() {
        let sql = "DROP TABLE A";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let res = resolver.resolve(ast);
        assert!(res.is_err());
    }

    #[test]
    fn start_transaction() {
        let sql = "BEGIN";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };
        let expected = Plan::BeginTransaction;
        assert_eq!(plan, expected);
    }

    #[test]
    fn commit() {
        let sql = "COMMIT;";
        let mut resolver = Resolver::new();
        let dialect = GenericDialect {};

        let ast = match Parser::parse_sql(&dialect, sql.to_string()) {
            Ok(t) => t[0].clone(),
            Err(e) => panic!("{}", e)
        };
        let plan = match resolver.resolve(ast) {
            Ok(t) => t,
            Err(e) => panic!("{}", e)
        };
        let expected = Plan::CommitTransaction;
        assert_eq!(plan, expected);
    }
}