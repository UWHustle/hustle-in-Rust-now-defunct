#[cfg(test)]
mod tests {
    use hustle_api::Hustle;
    use crossbeam_utils::thread as crossbeam_thread;
    use std::{time, thread};


    #[test]
    fn phantom_read() {
        let hustle = Hustle::new();
        let conn = hustle.connect();
        let _ = conn.prepare_statement("DROP TABLE T;").execute();
        conn.prepare_statement("CREATE TABLE T (a INT);").execute().unwrap();
        conn.prepare_statement("INSERT INTO T VALUES (1);").execute().unwrap();

        crossbeam_thread::scope(|s| {
            // Transaction 1.
            s.spawn(|_| {
                let mut conn = hustle.connect();

                conn.begin_transaction().unwrap();

                let select_count = conn.prepare_statement("SELECT COUNT(a) FROM T;");
                let result_a = select_count.execute().unwrap().unwrap().get_i64(0);

                // First count should be 1.
                assert_eq!(result_a, Some(1));

                thread::sleep(time::Duration::from_millis(200));

                // Transaction 2 tries to insert a row at this point in time.

                let result_b = select_count.execute().unwrap().unwrap().get_i64(0);

                conn.commit_transaction().unwrap();

                // Second count should also be 1.
                assert_eq!(result_b, Some(1));
            });

            // Transaction 2.
            s.spawn(|_| {
                let mut conn = hustle.connect();
                thread::sleep(time::Duration::from_millis(100));
                conn.begin_transaction().unwrap();
                conn.prepare_statement("INSERT INTO T values (2);").execute().unwrap();
                conn.commit_transaction().unwrap();
            });
        }).unwrap();
    }
}