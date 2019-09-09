use hustle_common::plan::Plan;
use hustle_catalog::{Column, Table, Catalog};
use hustle_resolver::Resolver;
use std::sync::Arc;
use hustle_types::{TypeVariant, Int8};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn generate_plan(sql: &str) -> Plan {
    let column_a = Column::new("a".to_owned(), "T".to_owned(), TypeVariant::Int8(Int8), false);
    let column_b = Column::new("b".to_owned(), "T".to_owned(), TypeVariant::Int8(Int8), false);
    let column_c = Column::new("c".to_owned(), "T".to_owned(), TypeVariant::Int8(Int8), false);
    let table_t = Table::new("T".to_owned(), vec![column_a, column_b]);
    let table_u = Table::new("U".to_owned(), vec![column_c]);

    let catalog = Catalog::new();
    catalog.create_table(table_t).unwrap();
    catalog.create_table(table_u).unwrap();

    let mut resolver = Resolver::new(Arc::new(catalog));

    let mut stmt = Parser::parse_sql(&GenericDialect {}, sql.to_owned()).unwrap();
    resolver.resolve(stmt.pop().unwrap()).unwrap()
}

pub fn generate_plans(sqls: &[&str]) -> Vec<Plan> {
    sqls.iter()
        .map(|sql| {
            generate_plan(sql)
        })
        .collect()
}
