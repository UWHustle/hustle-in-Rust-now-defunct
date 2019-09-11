#[cfg(test)]
mod resolver_tests {
    use std::sync::Arc;

    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use hustle_catalog::{Catalog, Column, Table};
    use hustle_common::plan::{Expression, Plan, Query, QueryOperator};
    use hustle_resolver::Resolver;
    use hustle_types::{Bool, Char, Int64, TypeVariant, ComparativeVariant};

    #[test]
    fn select_1() {
        let sql = "SELECT a, b FROM T;";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();
        let column_a = table.columns.iter()
            .find(|c| c.get_name() == "a")
            .unwrap().clone();
        let column_b = table.columns.iter()
            .find(|c| c.get_name() == "b")
            .unwrap().clone();

        let expected = Plan::Query(Query {
                output: vec![column_a, column_b],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: table.columns.clone(),
                        operator: QueryOperator::TableReference(table),
                    }),
                    cols: vec![0, 1],
                },
        });

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    #[test]
    fn select_2() {
        let sql = "SELECT T.a, T.b FROM T;";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();
        let column_a = table.columns.iter()
            .find(|c| c.get_name() == "a")
            .unwrap().clone();
        let column_b = table.columns.iter()
            .find(|c| c.get_name() == "b")
            .unwrap().clone();

        let expected = Plan::Query(Query {
                output: vec![column_a, column_b],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: table.columns.clone(),
                        operator: QueryOperator::TableReference(table),
                    }),
                    cols: vec![0, 1],
                },
        });

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    /// select and where
    #[test]
    fn select_3() {
        let sql = "SELECT a, b FROM T WHERE a > 1 AND a < 3 AND b >= 2 AND b <= 4;";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();
        let columns = table.columns.clone();

        let expected = Plan::Query(Query {
                output: vec![
                    columns[0].clone(),
                    columns[1].clone()
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: columns.clone(),
                        operator: QueryOperator::Select {
                            filter: Box::new(Expression::Conjunctive {
                                terms: vec![
                                    Expression::Conjunctive {
                                        terms: vec![
                                            Expression::Conjunctive {
                                                terms: vec![
                                                    Expression::Comparative {
                                                        variant: ComparativeVariant::Gt,
                                                        left: Box::new(Expression::ColumnReference(0)),
                                                        right: Box::new(Expression::Literal {
                                                            type_variant: TypeVariant::Int64(Int64),
                                                            buf: Int64.new_buf(1),
                                                        })
                                                    },
                                                    Expression::Comparative {
                                                        variant: ComparativeVariant::Lt,
                                                        left: Box::new(Expression::ColumnReference(0)),
                                                        right: Box::new(Expression::Literal {
                                                            type_variant: TypeVariant::Int64(Int64),
                                                            buf: Int64.new_buf(3),
                                                        })
                                                    },
                                                ]
                                            },
                                            Expression::Comparative {
                                                variant: ComparativeVariant::Ge,
                                                left: Box::new(Expression::ColumnReference(1)),
                                                right: Box::new(Expression::Literal {
                                                    type_variant: TypeVariant::Int64(Int64),
                                                    buf: Int64.new_buf(2),
                                                })
                                            }
                                        ]
                                    },
                                    Expression::Comparative {
                                        variant: ComparativeVariant::Le,
                                        left: Box::new(Expression::ColumnReference(1)),
                                        right: Box::new(Expression::Literal {
                                            type_variant: TypeVariant::Int64(Int64),
                                            buf: Int64.new_buf(4),
                                        })
                                    }
                                ]
                            }),
                            input: Box::new(Query {
                                output: columns,
                                operator: QueryOperator::TableReference(table),
                            })
                        }
                    }),
                    cols: vec![0, 1],
                },
        });

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    #[test]
    fn select_4() {
        let sql = "SELECT T.a, T.b FROM T WHERE T.a > 1 AND a < 3 AND T.b >= 2 AND b <= 4;";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();
        let columns = table.columns.clone();

        let expected = Plan::Query(Query {
                output: vec![
                    columns[0].clone(),
                    columns[1].clone()
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: columns.clone(),
                        operator: QueryOperator::Select {
                            filter: Box::new(Expression::Conjunctive {
                                terms: vec![
                                    Expression::Conjunctive {
                                        terms: vec![
                                            Expression::Conjunctive {
                                                terms: vec![
                                                    Expression::Comparative {
                                                        variant: ComparativeVariant::Gt,
                                                        left: Box::new(Expression::ColumnReference(0)),
                                                        right: Box::new(Expression::Literal {
                                                            type_variant: TypeVariant::Int64(Int64),
                                                            buf: Int64.new_buf(1),
                                                        })
                                                    },
                                                    Expression::Comparative {
                                                        variant: ComparativeVariant::Lt,
                                                        left: Box::new(Expression::ColumnReference(0)),
                                                        right: Box::new(Expression::Literal {
                                                            type_variant: TypeVariant::Int64(Int64),
                                                            buf: Int64.new_buf(3),
                                                        })
                                                    },
                                                ]
                                            },
                                            Expression::Comparative {
                                                variant: ComparativeVariant::Ge,
                                                left: Box::new(Expression::ColumnReference(1)),
                                                right: Box::new(Expression::Literal {
                                                    type_variant: TypeVariant::Int64(Int64),
                                                    buf: Int64.new_buf(2),
                                                })
                                            }
                                        ]
                                    },
                                    Expression::Comparative {
                                        variant: ComparativeVariant::Le,
                                        left: Box::new(Expression::ColumnReference(1)),
                                        right: Box::new(Expression::Literal {
                                            type_variant: TypeVariant::Int64(Int64),
                                            buf: Int64.new_buf(4),
                                        })
                                    }
                                ]
                            }),
                            input: Box::new(Query {
                                output: columns,
                                operator: QueryOperator::TableReference(table),
                            })
                        }
                    }),
                    cols: vec![0, 1],
                },
        });

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    /// project on cartesian product
    #[test]
    fn select_5() {
        let sql = "SELECT T.a, R.e FROM T, R;";
        let (plan, catalog) = resolve(sql);

        let table_t = catalog.get_table("T").unwrap().clone();
        let table_r = catalog.get_table("R").unwrap().clone();

        let expected = Plan::Query(Query {
                output: vec![
                    Column::new("a".to_string(), "T".to_string(), TypeVariant::Bool(Bool), false),
                    Column::new("e".to_string(), "R".to_string(), TypeVariant::Int64(Int64), false),
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: [&table_t.columns[..], &table_r.columns[..]].concat(),
                        operator: QueryOperator::Cartesian {
                            inputs: vec![
                                Query {
                                    output: table_t.columns.clone(),
                                    operator: QueryOperator::TableReference(table_t),
                                },
                                Query {
                                    output: table_r.columns.clone(),
                                    operator: QueryOperator::TableReference(table_r),
                                }
                            ],
                        }
                    }),
                    cols: vec![0, 4],
                }
        });

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    /// project on cartesian product
    #[test]
    fn select_6() {
        let sql = "SELECT a, e FROM T, R;";
        let (plan, catalog) = resolve(sql);

        let table_t = catalog.get_table("T").unwrap().clone();
        let table_r = catalog.get_table("R").unwrap().clone();

        let expected = Plan::Query(Query {
                output: vec![
                    Column::new("a".to_string(), "T".to_string(), TypeVariant::Bool(Bool), false),
                    Column::new("e".to_string(), "R".to_string(), TypeVariant::Int64(Int64), false),
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: [&table_t.columns[..], &table_r.columns[..]].concat(),
                        operator: QueryOperator::Cartesian {
                            inputs: vec![
                                Query {
                                    output: table_t.columns.clone(),
                                    operator: QueryOperator::TableReference(table_t),
                                },
                                Query {
                                    output: table_r.columns.clone(),
                                    operator: QueryOperator::TableReference(table_r),
                                }
                            ],
                        }
                    }),
                    cols: vec![0, 4],
                }
        });

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    /// join
    #[test]
    fn select_7() {
        let sql = "SELECT T.a, R.e FROM T, R WHERE T.b = R.d;";
        let (plan, catalog) = resolve(sql);

        let table_t = catalog.get_table("T").unwrap().clone();
        let table_r = catalog.get_table("R").unwrap().clone();

        let expected = Plan::Query(Query {
                output: vec![
                    Column::new("a".to_string(), "T".to_string(), TypeVariant::Bool(Bool), false),
                    Column::new("e".to_string(), "R".to_string(), TypeVariant::Int64(Int64), false),
                ],
                operator: QueryOperator::Project {
                    input: Box::new(Query {
                        output: [&table_t.columns[..], &table_r.columns[..]].concat(),
                        operator: QueryOperator::Select {
                            input: Box::new(Query {
                                output: [&table_t.columns[..], &table_r.columns[..]].concat(),
                                operator: QueryOperator::Cartesian {
                                    inputs: vec![
                                        Query {
                                            output: table_t.columns.clone(),
                                            operator: QueryOperator::TableReference(table_t),
                                        },
                                        Query {
                                            output: table_r.columns.clone(),
                                            operator: QueryOperator::TableReference(table_r),
                                        }
                                    ],
                                }
                            }),
                            filter: Box::new(Expression::Comparative {
                                variant: ComparativeVariant::Eq,
                                left: Box::new(Expression::ColumnReference(1)),
                                right: Box::new(Expression::ColumnReference(3))
                            })
                        }
                    }),
                    cols: vec![0, 4],
                }
        });

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    #[test]
    fn insert() {
        let sql = "INSERT INTO T VALUES (true, 1, '172.16.254.1')";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();

        let expected = Plan::Insert {
            into_table: table,
            bufs: vec![
                Bool.new_buf(true),
                Int64.new_buf(1),
                Char::new(12).new_buf("172.16.254.1"),
            ]
        };

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    #[test]
    fn update() {
        let sql = "UPDATE T SET a = 2, b = 3";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();

        let expected = Plan::Update {
            table,
            assignments: vec![
                (0, Int64.new_buf(2)),
                (1, Int64.new_buf(3)),
            ],
            filter: None
        };

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    #[test]
    fn update_filter() {
        let sql = "UPDATE T SET a = 2 WHERE a = 1";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();

        let expected = Plan::Update {
            table,
            assignments: vec![(0, Int64.new_buf(2))],
            filter: Some(Box::new(
                Expression::Comparative {
                    variant: ComparativeVariant::Eq,
                    left: Box::new(Expression::ColumnReference(0)),
                    right: Box::new(Expression::Literal {
                        type_variant: TypeVariant::Int64(Int64),
                        buf: Int64.new_buf(1),
                    })
                }
            ))
        };

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    #[test]
    fn delete() {
        let sql = "DELETE FROM T";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();

        let expected = Plan::Delete {
            from_table: table,
            filter: None,
        };

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    #[test]
    fn delete_filter() {
        let sql = "DELETE FROM T WHERE a = 1";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();

        let expected = Plan::Delete {
            from_table: table,
            filter: Some(Box::new(Expression::Comparative {
                variant: ComparativeVariant::Eq,
                left: Box::new(Expression::ColumnReference(0)),
                right: Box::new(Expression::Literal {
                    type_variant: TypeVariant::Int64(Int64),
                    buf: Int64.new_buf(1),
                })
            }))
        };

        assert_eq!(plan, expected);

        teardown(&catalog);
    }

    #[test]
    fn create_table() {
        let sql = "CREATE TABLE U (a BOOLEAN, b INT, c CHAR(8));";
        let (plan, _catalog) = resolve(sql);

        let expected = Plan::CreateTable(Table::new(
                "U".to_string(),
                vec![
                    Column::new("a".to_string(), "T".to_string(), TypeVariant::Bool(Bool), false),
                    Column::new("b".to_string(), "T".to_string(), TypeVariant::Int64(Int64), false),
                    Column::new("c".to_string(), "T".to_string(), TypeVariant::Char(Char::new(8)), false),
                ]
        ));

        assert_eq!(plan, expected);
    }

    #[test]
    fn drop_table() {
        let sql = "DROP TABLE T;";
        let (plan, catalog) = resolve(sql);

        let table = catalog.get_table("T").unwrap().clone();

        let expected = Plan::DropTable(table);

        assert_eq!(plan, expected);
    }

    #[test]
    fn start_transaction() {
        let sql = "BEGIN";
        let (plan, _catalog) = resolve(sql);

        let expected = Plan::BeginTransaction;

        assert_eq!(plan, expected);
    }

    #[test]
    fn commit() {
        let sql = "COMMIT;";
        let (plan, _catalog) = resolve(sql);

        let expected = Plan::CommitTransaction;

        assert_eq!(plan, expected);
    }

    fn resolve(sql: &str) -> (Plan, Arc<Catalog>) {
        let table_t = Table::new(
            "T".to_string(),
            vec![
                Column::new("a".to_string(), "T".to_owned(), TypeVariant::Bool(Bool), false),
                Column::new("b".to_string(), "T".to_owned(), TypeVariant::Int64(Int64), false),
                Column::new("c".to_string(), "T".to_owned(), TypeVariant::Char(Char::new(8)), false),
            ]
        );

        let table_r = Table::new(
            "R".to_string(),
            vec![
                Column::new("d".to_string(), "R".to_owned(), TypeVariant::Bool(Bool), false),
                Column::new("e".to_string(), "R".to_owned(), TypeVariant::Int64(Int64), false),
                Column::new("f".to_string(), "R".to_owned(), TypeVariant::Char(Char::new(8)), false),
            ]
        );

        let catalog = Arc::new(Catalog::new());

        assert!(catalog.create_table(table_t).is_ok());
        assert!(catalog.create_table(table_r).is_ok());

        let mut resolver = Resolver::new(catalog.clone());
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap()[0].clone();
        let plan = resolver.resolve(&[ast]).unwrap();

        (plan, catalog)
    }

    fn teardown(catalog: &Catalog) {
        assert!(catalog.drop_table("T").is_ok());
        assert!(catalog.drop_table("R").is_ok());
        assert!(!catalog.table_exists("T"));
        assert!(!catalog.table_exists("R"));
    }
}
