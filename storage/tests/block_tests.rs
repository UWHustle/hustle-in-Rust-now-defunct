extern crate hustle_storage;

#[macro_use]
extern crate lazy_static;

#[cfg(test)]
mod block_tests {
    use hustle_storage::StorageManager;
    use hustle_storage::block::BlockReference;

    lazy_static! {
        static ref STORAGE_MANAGER: StorageManager = {
            StorageManager::new()
        };
    }

    #[test]
    fn col_sizes() {
        let schema = vec![1, 2, 3];
        let block: BlockReference = STORAGE_MANAGER.create_block(&schema);

        assert_eq!(block.get_col_sizes(), schema.as_slice());

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn get_row_col() {
        let schema = vec![1, 2, 3];
        let block: BlockReference = STORAGE_MANAGER.create_block(&schema);

        block.get_row_col(0, 0).copy_from_slice(b"a");
        block.get_row_col(0, 1).copy_from_slice(b"bb");
        block.get_row_col(0, 2).copy_from_slice(b"ccc");

        assert_eq!(block.get_row_col(0, 0), b"a");
        assert_eq!(block.get_row_col(0, 1), b"bb");
        assert_eq!(block.get_row_col(0, 2), b"ccc");
    }

    #[test]
    fn get_row() {
        let schema = vec![1, 2, 3];
        let block: BlockReference = STORAGE_MANAGER.create_block(&schema);

        let mut row = block.get_row_internal(0);
        row.next().unwrap().copy_from_slice(b"a");
        row.next().unwrap().copy_from_slice(b"bb");
        row.next().unwrap().copy_from_slice(b"ccc");

        let mut row = block.get_row_internal(0);
        assert_eq!(row.next().unwrap(), b"a");
        assert_eq!(row.next().unwrap(), b"bb");
        assert_eq!(row.next().unwrap(), b"ccc");
        assert_eq!(row.next(), None);
    }

    #[test]
    fn get_col() {
        let schema = vec![b"repetition".len(), b"duplication".len()];
        let block: BlockReference = STORAGE_MANAGER.create_block(&schema);

        for (c0, c1) in block.get_col(0).zip(block.get_col(1)) {
            c0.copy_from_slice(b"repetition");
            c1.copy_from_slice(b"duplication");
        }

        for (c0, c1) in block.get_col(0).zip(block.get_col(1)) {
            assert_eq!(c0, b"repetition");
            assert_eq!(c1, b"duplication");
        }
    }

    #[test]
    fn get_rows() {
        let schema = vec![1, 2, 3];
        let block: BlockReference = STORAGE_MANAGER.create_block(&schema);

        let expected = [
            [b"a" as &[u8], b"bb", b"ccc"],
            [b"d", b"ee", b"ddd"],
        ];

        for (row_i, row) in expected.iter().enumerate() {
            for (col_i, val) in row.iter().enumerate() {
                block.get_row_col(row_i, col_i).copy_from_slice(val);
            }
        }

        for (row, expected_row) in block.get_rows().zip(expected.iter()) {
            for (val, &expected_val) in row.zip(expected_row.iter()) {
                assert_eq!(val, expected_val);
            }
        }
    }

    #[test]
    fn get_cols() {
        let schema = vec![1, 2, 3];
        let block: BlockReference = STORAGE_MANAGER.create_block(&schema);

        let expected = [
            [b"a" as &[u8], b"d"],
            [b"bb", b"ee"],
            [b"ccc", b"ddd"],
        ];

        for (col_i, col) in expected.iter().enumerate() {
            for (row_i, val) in col.iter().enumerate() {
                block.get_row_col(row_i, col_i).copy_from_slice(val);
            }
        }

        for (col, expected_col) in block.get_cols().zip(expected.iter()) {
            for (val, &expected_val) in col.zip(expected_col.iter()) {
                assert_eq!(val, expected_val);
            }
        }
    }
}
