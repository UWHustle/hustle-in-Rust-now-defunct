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
    fn metadata() {
        let schema = vec![1, 2, 3];
        let block: BlockReference = STORAGE_MANAGER.create_block(&schema);

        assert_eq!(block.n_cols(), schema.len());
        assert_eq!(block.row_size(), schema.iter().sum::<usize>());
        assert_eq!(block.schema(), schema);
        assert!(!block.is_full());

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn insert() {
        let (block, expected, _) = block_with_data();

        let actual = block.rows()
            .map(|row| row.collect::<Vec<&[u8]>>())
            .collect::<Vec<Vec<&[u8]>>>();

        compare_rows(actual, expected);

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn extend() {
        let (block_a, expected, schema) = block_with_data();
        let block_b: BlockReference = STORAGE_MANAGER.create_block(&schema);

        block_b.extend(&mut block_a.rows());

        let actual = block_b.rows()
            .map(|row| row.collect::<Vec<&[u8]>>())
            .collect::<Vec<Vec<&[u8]>>>();

        compare_rows(actual, expected);

        STORAGE_MANAGER.delete_block(block_a.id);
        STORAGE_MANAGER.delete_block(block_b.id);
    }

    #[test]
    fn project() {
        let (block, rows, _) = block_with_data();
        let cols = vec![0, 2];

        let actual = block.project(&cols)
            .map(|row| row.collect::<Vec<&[u8]>>())
            .collect::<Vec<Vec<&[u8]>>>();

        let expected = rows.into_iter()
            .map(|row| cols.iter().map(|&col| row[col]).collect::<Vec<&[u8]>>())
            .collect::<Vec<Vec<&[u8]>>>();

        compare_rows(actual, expected);

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn select() {
        let (block, rows, _) = block_with_data();
        let filter = |row: &[&[u8]]| row[0] == b"d";

        let actual = block.select(filter)
            .map(|row| row.collect::<Vec<&[u8]>>())
            .collect::<Vec<Vec<&[u8]>>>();

        let expected = rows.into_iter()
            .filter(|row| filter(row))
            .collect::<Vec<Vec<&[u8]>>>();

        compare_rows(actual, expected);

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn delete() {
        let (block, rows, _) = block_with_data();
        let filter = |row: &[&[u8]]| row[0] == b"d";

        block.delete(filter);

        let actual = block.rows()
            .map(|row| row.collect::<Vec<&[u8]>>())
            .collect::<Vec<Vec<&[u8]>>>();

        let expected = rows.into_iter()
            .filter(|row| !filter(row))
            .collect::<Vec<Vec<&[u8]>>>();

        compare_rows(actual, expected);

        STORAGE_MANAGER.delete_block(block.id);
    }

    #[test]
    fn clear() {
        let (block, _, _) = block_with_data();

        block.clear();

        let actual = block.rows()
            .map(|row| row.collect::<Vec<&[u8]>>())
            .collect::<Vec<Vec<&[u8]>>>();

        compare_rows(actual, vec![]);

        STORAGE_MANAGER.delete_block(block.id);
    }

    fn block_with_data() -> (BlockReference, Vec<Vec<&'static [u8]>>, Vec<usize>) {
        let schema = vec![1, 2, 3];
        let block: BlockReference = STORAGE_MANAGER.create_block(&schema);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"a", b"bb", b"ccc"],
            vec![b"d", b"ee", b"fff"],
        ];

        for row in &rows {
            block.insert(&row);
        }

        (block, rows, schema)
    }

    fn compare_rows<'a>(
        actual: Vec<Vec<&[u8]>>,
        expected: Vec<Vec<&[u8]>>,
    ) {
        assert_eq!(actual.len(), expected.len());
        for (actual_row, expected_row) in actual.iter().zip(expected) {
            assert_eq!(actual_row.len(), expected_row.len());
            for (&actual_value, &expected_value) in actual_row.iter().zip(expected_row.iter()) {
                assert_eq!(actual_value, expected_value);
            }
        }
    }
}
