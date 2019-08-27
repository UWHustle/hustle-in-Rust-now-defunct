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
    fn project() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1, 2], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a", b"bb"],
            vec![b"c", b"dd"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        for (row, expected_row) in block.project(&[0, 1]).zip(&rows) {
            for (val, &expected_val) in row.zip(expected_row.iter()) {
                assert_eq!(val, expected_val);
            }
        }

        let mut projection = block.project(&[1]);
        let mut row = projection.next().unwrap();
        assert_eq!(row.next().unwrap(), b"bb");

        row = projection.next().unwrap();
        assert_eq!(row.next().unwrap(), b"dd");

        assert!(projection.next().is_none());

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn project_with_mask() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        let mask = block.filter_col(0, |buf| buf == b"b");
        let mut rows = block.project_with_mask(&[0], mask);

        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn delete_rows() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1], 0);

        insert_row(&[b"a"], &block);
        block.delete_rows();

        assert!(block.project(&[0]).next().is_none());

        STORAGE_MANAGER.delete_block(block.id);
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
        let mut rows = block.project(&[0]);

        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn update_col() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1, 2], 0);

        insert_row(&[b"a", b"bb"], &block);
        block.update_col(0, b"c");

        let mut row = block.project(&[0, 1]).next().unwrap();
        assert_eq!(row.next().unwrap(), b"c");
        assert_eq!(row.next().unwrap(), b"bb");

        block.update_col(1, b"dd");

        let mut row = block.project(&[0, 1]).next().unwrap();
        assert_eq!(row.next().unwrap(), b"c");
        assert_eq!(row.next().unwrap(), b"dd");

        STORAGE_MANAGER.delete_block(block.id);
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
        let mut rows = block.project(&[0]);

        assert_eq!(rows.next().unwrap().next().unwrap(), b"c");
        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn insert_rows() {
        let block: BlockReference = STORAGE_MANAGER.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
            vec![b"c"],
        ];

        block.insert_rows(&mut rows.iter().map(|row| row.iter().map(|&buf| buf)));

        STORAGE_MANAGER.delete_block(block.id);
    }

    fn insert_row(row: &[&[u8]], block: &BlockReference) {
        block.insert_row(row.iter().map(|&buf| buf));
    }
}
