use logical_entities::relation::Relation;
use storage::StorageManager;
use type_system::borrowed_buffer::BorrowedBuffer;
use type_system::integer::Int8;
use type_system::{cast_numeric, force_numeric, Buffer, Numeric};

pub fn sum_column_hustle(
    storage_manager: &StorageManager,
    relation: Relation,
    column_name: &str,
) -> i64 {
    let schema = relation.get_schema();
    let schema_sizes = schema.to_size_vec();
    let physical_relation = storage_manager
        .relational_engine()
        .get(relation.get_name())
        .unwrap();

    // Index of the specified column
    let column = relation.column_from_name(column_name).unwrap();
    let col_i = schema
        .get_columns()
        .iter()
        .position(|x| x == &column)
        .unwrap();

    let mut sum: Box<Numeric> = Box::new(Int8::from(0));
    for block in physical_relation.blocks() {
        for row_i in 0..block.get_n_rows() {
            let data = block.get_row_col(row_i, col_i).unwrap();
            let data_type = column.data_type();
            let value = BorrowedBuffer::new(data, data_type, false).marshall();
            sum = sum.add(&*force_numeric(&*value));
        }
    }

    cast_numeric::<Int8>(&*sum).value()
}
