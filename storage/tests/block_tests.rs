extern crate hustle_storage;

#[cfg(test)]
mod block_tests {
    use hustle_storage::block::BlockReference;
    use hustle_storage::StorageManager;

    #[test]
    fn get_row() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1], 0);

        insert_row(&[b"a"], &block);

        let mut row = block.get_row(0).unwrap();
        assert_eq!(row.next().unwrap(), b"a");
        assert!(row.next().is_none());

        assert!(block.get_row(1).is_none());

        storage_manager.clear();
    }

    #[test]
    fn get_col() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1], 0);

        insert_row(&[b"a"], &block);

        let mut col = block.get_col(0).unwrap();
        assert_eq!(col.next().unwrap(), (0, b"a" as &[u8]));
        assert!(col.next().is_none());

        assert!(block.get_col(1).is_none());

        storage_manager.clear();
    }

    #[test]
    fn rows() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1, 2], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a", b"bb"],
            vec![b"c", b"dd"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        for (row, expected_row) in block.rows().zip(&rows) {
            for (val, &expected_val) in row.zip(expected_row.iter()) {
                assert_eq!(val, expected_val);
            }
        }

        storage_manager.clear();
    }

    #[test]
    fn project() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1, 2], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a", b"bb"],
            vec![b"c", b"dd"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        let mut projection = block.project(&[1]);
        let mut row = projection.next().unwrap();
        assert_eq!(row.next().unwrap(), b"bb");

        row = projection.next().unwrap();
        assert_eq!(row.next().unwrap(), b"dd");

        assert!(projection.next().is_none());

        storage_manager.clear();
    }

    #[test]
    fn project_with_mask() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        let mask = block.filter_col(0, |buf| buf == b"b");
        let mut rows = block.project_with_mask(&[0], &mask);

        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());

        storage_manager.clear();
    }

    #[test]
    fn tentative_delete_rows() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1], 0);

        insert_row(&[b"a"], &block);
        block.tentative_delete_rows(|row_i| assert_eq!(row_i, 0));

        assert!(block.project(&[0]).next().is_none());

        storage_manager.clear();
    }

    #[test]
    fn tentative_delete_rows_with_mask() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        let mask = block.filter_col(0, |buf| buf == b"a");
        block.tentative_delete_rows_with_mask(&mask, |row_i| assert_eq!(row_i, 0));
        let mut rows = block.project(&[0]);

        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());

        storage_manager.clear();
    }

    #[test]
    fn update_col() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1, 2], 0);

        insert_row(&[b"a", b"bb"], &block);
        block.update_col(0, b"c");

        let mut row = block.project(&[0, 1]).next().unwrap();
        assert_eq!(row.next().unwrap(), b"c");
        assert_eq!(row.next().unwrap(), b"bb");

        block.update_col(1, b"dd");

        let mut row = block.project(&[0, 1]).next().unwrap();
        assert_eq!(row.next().unwrap(), b"c");
        assert_eq!(row.next().unwrap(), b"dd");

        storage_manager.clear();
    }

    #[test]
    fn update_col_with_mask() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
        ];

        for row in &rows {
            insert_row(row, &block);
        }

        let mask = block.filter_col(0, |buf| buf == b"a");
        block.update_col_with_mask(0, b"c", &mask);
        let mut rows = block.project(&[0]);

        assert_eq!(rows.next().unwrap().next().unwrap(), b"c");
        assert_eq!(rows.next().unwrap().next().unwrap(), b"b");
        assert!(rows.next().is_none());

        storage_manager.clear();
    }

    #[test]
    fn tentative_insert_row() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1], 0);

        let mut inserted_row_i = None;
        let row: &[&[u8]] = &[b"a"];

        block.tentative_insert_row(
            row.iter().map(|&buf| buf),
            |row_i| inserted_row_i = Some(row_i),
        );

        assert!(block.rows().next().is_none());

        block.finalize_row(inserted_row_i.unwrap());

        assert_eq!(block.rows().next().unwrap().next().unwrap(), b"a");

        storage_manager.clear();
    }

    #[test]
    fn insert_rows() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block = storage_manager.create_block(vec![1], 0);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a"],
            vec![b"b"],
            vec![b"c"],
        ];

        block.insert_rows(&mut rows.iter().map(|row| row.iter().map(|&buf| buf)));

        storage_manager.clear();
    }

    fn insert_row(row: &[&[u8]], block: &BlockReference) {
        block.insert_row(row.iter().map(|&buf| buf));
    }
}
