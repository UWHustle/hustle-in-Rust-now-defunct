sqlite3 -cmd "DROP TABLE IF EXISTS T;" test-data/sqlite.data
sqlite3 -cmd "CREATE TABLE T (a INTEGER, b INTEGER);" test-data/sqlite.data
sqlite3 -cmd ".mode csv" -cmd ".import test-data/data.csv T" test-data/sqlite.data