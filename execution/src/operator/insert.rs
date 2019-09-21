use hustle_catalog::Catalog;
use hustle_storage::{LogManager, StorageManager};

use crate::engine::FinalizeRowIds;
use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Insert {
    table_name: String,
    bufs: Vec<Vec<u8>>,
    router: BlockPoolDestinationRouter,
    transaction_id: u64,
    finalize_row_ids: FinalizeRowIds,
}

impl Insert {
    pub fn new(
        table_name: String,
        bufs: Vec<Vec<u8>>,
        router: BlockPoolDestinationRouter,
        transaction_id: u64,
        finalize_row_ids: FinalizeRowIds,
    ) -> Self {
        Insert {
            table_name,
            bufs,
            router,
            transaction_id,
            finalize_row_ids,
        }
    }
}

impl Operator for Insert {
    fn execute(
        self: Box<Self>,
        storage_manager: &StorageManager,
        log_manager: &LogManager,
        catalog: &Catalog
    ) {
        let output_block = self.router.get_block(storage_manager);
        output_block.tentative_insert_row(
                self.bufs.iter().map(|buf| buf.as_slice()),
                |row_id| {
                    log_manager.log_insert(
                        self.transaction_id,
                        output_block.id,
                        row_id as u64
                    );

                    self.finalize_row_ids.lock().unwrap()
                        .entry(self.transaction_id)
                        .or_default()
                        .entry(output_block.id)
                        .or_default()
                        .push(row_id as u64);
                }
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
        let log_manager = LogManager::with_unique_log_directory();
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

        let insert = Box::new(Insert::new("insert".to_owned(), bufs.clone(), router, 0));
        insert.execute(&storage_manager, &log_manager, &catalog);

        storage_manager.clear();
        log_manager.clear();
    }
}
