use super::sqlite3::FILENAME;
use logical_entities::column::Column;
use logical_entities::relation::Relation;
use logical_entities::row::Row;
use logical_entities::schema::Schema;
use physical_operators::create_table::CreateTable;
use physical_operators::insert::Insert;
use physical_operators::Operator;
use storage::StorageManager;
use type_system::data_type::*;
use type_system::integer::Int4;
use type_system::*;

extern crate rand;
use self::rand::{random, Rng, SeedableRng, StdRng};

pub fn generate_int4_relation_hustle_and_sqlite3(
    storage_manager: &StorageManager,
    name: &str,
    col_names: Vec<&str>,
    record_count: usize,
    seed: bool,
) -> Relation {
    let mut columns = vec![];
    for name in col_names {
        columns.push(Column::new(name, DataType::new(Variant::Int4, true)));
    }
    let relation = Relation::new(name, Schema::new(columns));
    CreateTable::new(relation.clone())
        .execute(storage_manager)
        .unwrap();

    let connection = sqlite::open(FILENAME).unwrap();
    connection
        .execute(format!("DROP TABLE IF EXISTS {};", name))
        .unwrap();
    connection
        .execute(format!(
            "CREATE TABLE {} ({});",
            name,
            relation.get_schema().to_string()
        ))
        .unwrap();

    let mut rng: StdRng = match seed {
        true => SeedableRng::seed_from_u64(1),
        false => SeedableRng::seed_from_u64(random::<u64>()),
    };
    for _row_i in 0..record_count {
        let mut values: Vec<Box<Value>> = vec![];
        for _col_i in 0..relation.get_schema().get_columns().len() {
            values.push(Box::new(Int4::from(rng.gen::<i32>())));
        }
        let row = Row::new(relation.get_schema().clone(), values);
        Insert::new(relation.clone(), row.clone())
            .execute(storage_manager)
            .unwrap();
        connection
            .execute(format!("INSERT INTO {} VALUES({})", name, row.to_string()))
            .unwrap();
    }

    relation
}

pub fn generate_t_hustle_and_sqlite(
    storage_manager: &StorageManager,
    record_count: usize,
    seed: bool,
) -> Relation {
    generate_int4_relation_hustle_and_sqlite3(
        storage_manager,
        "t",
        vec!["a", "b"],
        record_count,
        seed,
    )
}
