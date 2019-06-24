#![recursion_limit = "128"]

extern crate hustle_parser;

#[cfg(test)]
mod tests {
    use hustle_parser::parse;
    use serde_json::{json, Value};

    #[test]
    fn create_table() {
        let ast_string = parse("CREATE TABLE t (a INT1, b FLOAT4, c IPv4);").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "create_table",
            "name": "t",
            "columns": [
                {
                    "type": "column",
                    "name": "a",
                    "column_type": "INT1"
                },
                {
                    "type": "column",
                    "name": "b",
                    "column_type": "FLOAT4"
                },
                {
                    "type": "column",
                    "name": "c",
                    "column_type": "IPv4"
                }
            ]
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn insert() {
        let ast_string = parse("INSERT INTO t VALUES (1, 2.3, '172.16.254.1');").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "insert",
            "into_table": {
                "type": "table",
                "name": "t"
            },
            "input": {
                "type": "values",
                "values": [
                    {
                        "type": "literal",
                        "value": "1",
                        "literal_type": "int"
                    },
                    {
                        "type": "literal",
                        "value": "2.3",
                        "literal_type": "float"
                    },
                    {
                        "type": "literal",
                        "value": "'172.16.254.1'",
                        "literal_type": "string"
                    }
                ]
            }
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn select() {
        let ast_string = parse("SELECT a FROM t;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "select",
            "from_table": {
                "type": "table",
                "name": "t"
            },
            "projection": [
                {
                    "type": "column",
                    "name": "a"
                }
            ]
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn select_where() {
        let ast_string = parse(
            "SELECT a, b \
            FROM t \
            WHERE a < 3 AND a > 1 AND b >= 2 AND b <= 4;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "select",
            "from_table": {
                "type": "table",
                "name": "t"
            },
            "filter": {
                "type": "operation",
                "name": "and",
                "left": {
                    "type": "operation",
                    "name": "and",
                    "left": {
                        "type": "operation",
                        "name": "and",
                        "left": {
                            "type": "operation",
                            "name": "lt",
                            "left": {
                                "type": "column",
                                "name": "a"
                            },
                            "right": {
                                "type": "literal",
                                "value": "3",
                                "literal_type": "int"
                            }
                        },
                        "right": {
                            "type": "operation",
                            "name": "gt",
                            "left": {
                                "type": "column",
                                "name": "a"
                            },
                            "right": {
                                "type": "literal",
                                "value": "1",
                                "literal_type": "int"
                            }
                        }
                    },
                    "right": {
                        "type": "operation",
                        "name": "ge",
                        "left": {
                            "type": "column",
                            "name": "b"
                        },
                        "right": {
                            "type": "literal",
                            "value": "2",
                            "literal_type": "int"
                        }
                    }
                },
                "right": {
                    "type": "operation",
                    "name": "le",
                    "left": {
                        "type": "column",
                        "name": "b"
                    },
                    "right": {
                        "type": "literal",
                        "value": "4",
                        "literal_type": "int"
                    }
                }
            },
            "projection": [
                {
                    "type": "column",
                    "name": "a"
                },
                {
                    "type": "column",
                    "name": "b"
                }
            ]
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn cartesian() {
        let ast_string = parse("SELECT t.a, u.b FROM t, u;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "select",
            "from_table": {
                "type": "join",
                "l_table": {
                    "type": "table",
                    "name": "t"
                },
                "r_table": {
                    "type": "table",
                    "name": "u"
                }
            },
            "projection": [
                {
                    "type": "column",
                    "table": "t",
                    "name": "a"
                },
                {
                    "type": "column",
                    "table": "u",
                    "name": "b"
                }
            ]
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn comma_join() {
        let ast_string = parse("SELECT t.a, u.b FROM t, u WHERE t.a = u.b;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "select",
            "from_table": {
                "type": "join",
                "l_table": {
                    "type": "table",
                    "name": "t"
                },
                "r_table": {
                    "type": "table",
                    "name": "u"
                }
            },
            "filter": {
                "type": "operation",
                "name": "eq",
                "left": {
                    "type": "column",
                    "table": "t",
                    "name": "a"
                },
                "right": {
                    "type": "column",
                    "table": "u",
                    "name": "b"
                }
            },
            "projection": [
                {
                    "type": "column",
                    "table": "t",
                    "name": "a"
                },
                {
                    "type": "column",
                    "table": "u",
                    "name": "b"
                }
            ]
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn keyword_join() {
        let ast_string = parse("SELECT t.a FROM t JOIN u ON t.a = u.b;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "select",
            "from_table": {
                "type": "join",
                "l_table": {
                    "type": "table",
                    "name": "t"
                },
                "r_table": {
                    "type": "table",
                    "name": "u"
                },
                "filter": {
                    "type": "operation",
                    "name": "eq",
                    "left": {
                        "type": "column",
                        "table": "t",
                        "name": "a"
                    },
                    "right": {
                        "type": "column",
                        "table": "u",
                        "name": "b"
                    }
                }
            },
            "projection": [
                {
                    "type": "column",
                    "table": "t",
                    "name": "a"
                }
            ]
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn update() {
        let ast_string = parse("UPDATE t SET a = 2, b = 3;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "update",
            "table": {
                "type": "table",
                "name": "t"
            },
            "assignments": [
                {
                    "type": "assignment",
                    "column": {
                        "type": "column",
                        "name": "a"
                    },
                    "value": {
                        "type": "literal",
                        "value": "2",
                        "literal_type": "int"
                    }
                },
                {
                    "type": "assignment",
                    "column": {
                        "type": "column",
                        "name": "b"
                    },
                    "value": {
                        "type": "literal",
                        "value": "3",
                        "literal_type": "int"
                    }
                }
            ]
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn update_where() {
        let ast_string = parse("UPDATE t SET a = 2 WHERE a = 1;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "update",
            "table": {
                "type": "table",
                "name": "t"
            },
            "filter": {
                "type": "operation",
                "name": "eq",
                "left": {
                    "type": "column",
                    "name": "a"
                },
                "right": {
                    "type": "literal",
                    "value": "1",
                    "literal_type": "int"
                }
            },
            "assignments": [
                {
                    "type": "assignment",
                    "column": {
                        "type": "column",
                        "name": "a"
                    },
                    "value": {
                        "type": "literal",
                        "value": "2",
                        "literal_type": "int"
                    }
                }
            ]
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn delete() {
        let ast_string = parse("DELETE FROM t WHERE a = 1;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "delete",
            "from_table": {
                "type": "table",
                "name": "t"
            },
            "filter": {
                "type": "operation",
                "name": "eq",
                "left": {
                    "type": "column",
                    "name": "a"
                },
                "right": {
                    "type": "literal",
                    "value": "1",
                    "literal_type": "int"
                }
            }
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn drop_table() {
        let ast_string = parse("DROP TABLE t;").unwrap();
        let ast: Value = serde_json::from_str(&ast_string).unwrap();
        let expected = json!({
            "type": "drop_table",
            "table": {
                "type": "table",
                "name": "t"
            }
        });
        assert_eq!(ast, expected);
    }
}
