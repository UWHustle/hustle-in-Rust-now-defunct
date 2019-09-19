use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Insert {
    table_name: String,
    bufs: Vec<Vec<u8>>,
    router: BlockPoolDestinationRouter,
}

impl Insert {
    pub fn new(table_name: String, bufs: Vec<Vec<u8>>, router: BlockPoolDestinationRouter) -> Self {
        Insert {
            table_name,
            bufs,
            router,
        }
    }
}

impl Operator for Insert {
    fn execute(self: Box<Self>, storage_manager: &StorageManager, catalog: &Catalog) {
        let output_block = self.router.get_block(storage_manager);
        output_block.tentative_insert_row(
                self.bufs.iter().map(|buf| buf.as_slice()),
                |_| (), // TODO: Write the row ID to storage.
        );

        for block_id in self.router.get_created_block_ids() {
            catalog.append_block_id(&self.table_name, block_id).unwrap();
        }
    }
}

#[cfg(test)]
mod insert_tests {
    use hustle_execution_test_util as test_util;
    use hustle_types::{Bool, Char, HustleType, Int64};

    use crate::operator::Insert;

    use super::*;

    #[test]
    fn insert() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let catalog = Catalog::new();
        let table = test_util::example_table();
        catalog.create_table(table.clone()).unwrap();
        let router = BlockPoolDestinationRouter::new(table.columns);

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

        let insert = Box::new(Insert::new("insert".to_owned(), bufs.clone(), router));
        insert.execute(&storage_manager, &catalog);

        storage_manager.clear();
    }
}
