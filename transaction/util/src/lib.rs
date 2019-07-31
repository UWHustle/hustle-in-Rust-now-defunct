use hustle_common::{Column, Plan, Table};
use hustle_resolver::{Catalog, Resolver};

pub fn generate_plan(sql: &str) -> Plan {
    let column_a = Column::new("a", "int", "T", None);
    let column_b = Column::new("b", "int", "T", None);
    let column_c = Column::new("c", "int", "U", None);
    let table_t = Table::new("T", vec![column_a, column_b]);
    let table_u = Table::new("U", vec![column_c]);

    let mut catalog = Catalog::new();
    catalog.create_table(table_t).unwrap();
    catalog.create_table(table_u).unwrap();

    let mut resolver = Resolver::with_catalog(catalog);

    resolver.resolve(&hustle_parser::parse(sql).unwrap()).unwrap()
}

pub fn generate_plans(sqls: &[&str]) -> Vec<Plan> {
    sqls.iter()
        .map(|sql| {
            generate_plan(sql)
        })
        .collect()
}
