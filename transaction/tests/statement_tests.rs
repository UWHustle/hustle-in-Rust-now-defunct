#[cfg(test)]
mod statement_tests {
    use hustle_transaction::policy::ColumnManager;
    use hustle_transaction::statement::StatementDomain;
    use hustle_transaction_test_util as test_util;

    // DELETE tests.

    #[test]
    fn delete_conflict_a() {
        let stmts = generate_statements(&[
            "DELETE FROM T;",
            "SELECT a FROM T;",
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn delete_conflict_b() {
        let stmts = generate_statements(&[
            "DELETE FROM T WHERE a = 1;",
            "SELECT b FROM T WHERE b = 1;",
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn delete_conflict_c() {
        let stmts = generate_statements(&[
            "DELETE FROM T;",
            "UPDATE T SET a = 1;",
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn delete_no_conflict_a() {
        let stmts = generate_statements(&[
            "DELETE FROM T WHERE a = 1;",
            "SELECT b FROM T WHERE a = 2;",
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn delete_no_conflict_b() {
        let stmts = generate_statements(&[
            "DELETE FROM T WHERE a = 1;",
            "INSERT INTO T VALUES (2, 3);",
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn delete_no_conflict_c() {
        let stmts = generate_statements(&[
            "DELETE FROM T;",
            "SELECT c FROM U;",
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    // INSERT tests.

    #[test]
    fn insert_conflict_a() {
        let stmts = generate_statements(&[
            "INSERT INTO T VALUES (1, 2);",
            "SELECT a FROM T;",
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn insert_conflict_b() {
        let stmts = generate_statements(&[
            "INSERT INTO T VALUES (1, 2);",
            "UPDATE T SET a = 1 WHERE b = 2;",
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn insert_no_conflict_a() {
        let stmts = generate_statements(&[
            "INSERT INTO T VALUES (1, 2);",
            "SELECT a FROM T WHERE a = 2;",
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn insert_no_conflict_b() {
        let stmts = generate_statements(&[
            "INSERT INTO T VALUES (1, 2);",
            "UPDATE T SET a = 1 WHERE b = 1;",
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn insert_no_conflict_c() {
        let stmts = generate_statements(&[
            "INSERT INTO T VALUES (1, 2);",
            "SELECT c FROM U;",
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    // SELECT tests.

    #[test]
    fn select_no_conflict_a() {
        let stmts = generate_statements(&[
            "SELECT a FROM T;",
            "SELECT a FROM T;",
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn select_no_conflict_b() {
        let stmts = generate_statements(&[
            "SELECT T.a, U.c FROM T, U WHERE T.b = U.c AND T.b = 1;",
            "UPDATE T SET a = 1 WHERE b = 2;",
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn select_conflict_a() {
        let stmts = generate_statements(&[
            "SELECT T.a, U.c FROM T, U WHERE T.b = U.c;",
            "UPDATE T SET b = 1;",
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn select_conflict_b() {
        let stmts = generate_statements(&[
            "SELECT T.a, U.c FROM T, U WHERE T.b = U.c AND U.c = 1;",
            "DELETE FROM U WHERE U.c = 1;",
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    // UPDATE tests.

    #[test]
    fn update_conflict_a() {
        let stmts = generate_statements(&[
            "UPDATE T SET a = 1 WHERE b = 1;",
            "SELECT a FROM T;"
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn update_conflict_b() {
        let stmts = generate_statements(&[
            "UPDATE T SET a = 1 WHERE b = 1;",
            "UPDATE T SET b = 2;"
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn update_conflict_c() {
        let stmts = generate_statements(&[
            "UPDATE T SET a = 1 WHERE b = 1;",
            "UPDATE T SET b = 2 WHERE a = 2;"
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn update_conflict_d() {
        let stmts = generate_statements(&[
            "UPDATE T SET a = 1 WHERE a = 2;",
            "SELECT a FROM T WHERE a = 1;"
        ]);
        assert!(stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn update_no_conflict_a() {
        let stmts = generate_statements(&[
            "UPDATE T SET a = 1;",
            "SELECT b FROM T;"
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    #[test]
    fn update_no_conflict_b() {
        let stmts = generate_statements(&[
            "UPDATE T SET a = 1 WHERE a = 2;",
            "SELECT a FROM T WHERE a = 3;"
        ]);
        assert!(!stmts[0].conflicts(&stmts[1]));
    }

    pub fn generate_statements(sqls: &[&str]) -> Vec<StatementDomain> {
        let mut column_manager = ColumnManager::new();
        test_util::generate_plans(sqls)
            .into_iter()
            .map(|plan| StatementDomain::new(0, 0, 0,  plan, &mut column_manager))
            .collect()
    }
}
