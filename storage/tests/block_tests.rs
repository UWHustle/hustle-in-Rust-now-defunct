extern crate hustle_storage;
#[macro_use]
extern crate lazy_static;

#[cfg(test)]
mod block_tests {
    use hustle_storage::block::BlockReference;
    use hustle_storage::StorageManager;

    lazy_static! {
        static ref STORAGE_MANAGER: StorageManager = {
            StorageManager::new()
        };
    }

    #[test]
    fn get_rows() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1, 2], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a", b"bb"],
            vec![b"c", b"dd"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        for (row, expected_row) in block.get_rows().zip(&rows) {
            for (val, &expected_val) in row.zip(expected_row.iter()) {
                assert_eq!(val, expected_val);
            }
        }
    }

    #[test]
    fn get_rows_with_mask() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        let mask = block.filter_col(0, |buf| buf == b"b");
        let mut rows = block.get_rows_with_mask(mask);

        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());
    }

    #[test]
    fn delete_rows() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1], 0);

        insert_row(&[b"a"], &block);
        block.delete_rows();

        assert!(block.get_rows().next().is_none());
    }

    #[test]
    fn delete_rows_with_mask() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        let mask = block.filter_col(0, |buf| buf == b"a");
        block.delete_rows_with_mask(mask);
        let mut rows = block.get_rows();

        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());
    }

    #[test]
    fn update_col() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1, 2], 0);

        insert_row(&[b"a", b"bb"], &block);
        block.update_col(0, b"c");

        let mut row = block.get_rows().next().unwrap();
        assert_eq!(row.next().unwrap(), b"c");
        assert_eq!(row.next().unwrap(), b"bb");

        block.update_col(1, b"dd");

        let mut row = block.get_rows().next().unwrap();
        assert_eq!(row.next().unwrap(), b"c");
        assert_eq!(row.next().unwrap(), b"dd");
    }

    #[test]
    fn update_col_with_mask() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        let mask = block.filter_col(0, |buf| buf == b"a");
        block.update_col_with_mask(0, b"c", mask);
        let mut rows = block.get_rows();

        assert_eq!(rows.next().unwrap().next().unwrap(), b"c");
        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());
    }

    #[test]
    fn insert_rows() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1], 0);

        for row in block.get_insert_guard().into_insert_rows() {
            for buf in row {
                buf.copy_from_slice(b"a");
            }
        }
    }

    fn insert_row(row: &[&[u8]], block: &BlockReference) {
        for (insert_buf, buf) in block.get_insert_guard().insert_row().zip(row.iter()) {
            insert_buf.copy_from_slice(buf);
        }
    }
}
