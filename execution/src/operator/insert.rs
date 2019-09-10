use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Insert {
    bufs: Vec<Vec<u8>>,
    router: BlockPoolDestinationRouter,
}

impl Insert {
    pub fn new(bufs: Vec<Vec<u8>>, router: BlockPoolDestinationRouter) -> Self {
        Insert {
            bufs,
            router,
        }
    }
}

impl Operator for Insert {
    fn execute(self: Box<Self>, storage_manager: &StorageManager, _catalog: &Catalog) {
        let output_block = self.router.get_block(storage_manager);
        output_block.insert_row(self.bufs.iter().map(|buf| buf.as_slice()));
        self.router.return_block(output_block);
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

        let insert = Insert::new(bufs, router);
        insert.execute(&storage_manager, &catalog);

        let output_block_ids = insert.router.get_block_ids();
        assert!(!output_block_ids.is_empty());

        let output_block = storage_manager.get_block(*output_block_ids.first().unwrap()).unwrap();
        let cols = (0..output_block.n_cols()).collect::<Vec<usize>>();
        let mut rows = output_block.project(&cols);

        for (actual_buf, expected_buf) in rows.next().unwrap().zip(insert.bufs) {
            assert_eq!(actual_buf, expected_buf.as_slice());
        }

        assert!(rows.next().is_none());

        storage_manager.clear();
    }
}
