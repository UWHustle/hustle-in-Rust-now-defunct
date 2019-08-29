use hustle_catalog::{Table, Column};
use hustle_types::{TypeVariant, Bool, Int64, Char, HustleType};
use hustle_storage::block::BlockReference;
use hustle_storage::StorageManager;

pub fn example_table() -> Table {
    Table::new(
        "insert".to_owned(),
        vec![
            Column::new("col_bool".to_owned(), TypeVariant::Bool(Bool), false),
            Column::new("col_int64".to_owned(), TypeVariant::Int64(Int64), false),
            Column::new("col_char".to_owned(), TypeVariant::Char(Char::new(1)), false),
        ],
        vec![],
    )
}

pub fn example_block(storage_manager: &StorageManager) -> BlockReference {
    let table = example_table();

    let col_sizes = table.columns.into_iter()
        .map(|c| c.into_type_variant().into_type().byte_len())
        .collect();

    let block = storage_manager.create_block(col_sizes, 0);

    let bool_type = Bool;
    let int64_type = Int64;
    let char_type = Char::new(1);

    let mut bufs = vec![
        vec![0; bool_type.byte_len()],
        vec![0; int64_type.byte_len()],
        vec![0; char_type.byte_len()],
    ];

    bool_type.set(false, &mut bufs[0]);
    int64_type.set(1, &mut bufs[1]);
    char_type.set("a", &mut bufs[2]);

    block.insert_row(bufs.iter().map(|buf| buf.as_slice()));

    bool_type.set(true, &mut bufs[0]);
    int64_type.set(-1, &mut bufs[1]);
    char_type.set("b", &mut bufs[2]);

    block.insert_row(bufs.iter().map(|buf| buf.as_slice()));

    block
}
