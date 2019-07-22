#[macro_use]
extern crate lazy_static;

use std::sync::Mutex;

use hustle_common::{Column, Plan, Table};
use hustle_resolver::{Catalog, Resolver};

lazy_static! {
    static ref RESOLVER: Mutex<Resolver> = {
        let column_a = Column::new("a".to_owned(), "int".to_owned(), "T".to_owned());
        let column_b = Column::new("b".to_owned(), "int".to_owned(), "T".to_owned());
        let column_c = Column::new("c".to_owned(), "int".to_owned(), "T".to_owned());
        let table_t = Table::new("T".to_owned(), vec![column_a, column_b]);
        let table_u = Table::new("U".to_owned(), vec![column_c]);

        let mut catalog = Catalog::new();
        catalog.create_table(table_t).unwrap();
        catalog.create_table(table_u).unwrap();

        Mutex::new(Resolver::with_catalog(catalog))
    };
}

pub fn generate_plan(sql: &str) -> Plan {
    RESOLVER.lock().unwrap().resolve(&hustle_parser::parse(sql).unwrap()).unwrap()
}

pub fn generate_plans(sqls: &[&str]) -> Vec<Plan> {
    sqls.iter()
        .map(|sql| {
            generate_plan(sql)
        })
        .collect()
}
