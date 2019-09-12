extern crate hustle_storage;

#[cfg(test)]
mod storage_manager_tests {
    use hustle_storage::StorageManager;

    #[test]
    fn create_block() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block_id = storage_manager.create_block(vec![1], 0).id;
        assert!(storage_manager.get_block(block_id).is_some());
        storage_manager.clear();
    }

    #[test]
    fn delete_block() {
        let storage_manager = StorageManager::with_unique_data_directory();
        let block_id = storage_manager.create_block(vec![1], 0).id;
        storage_manager.delete_block(block_id);
        assert!(storage_manager.get_block(block_id).is_none());
        storage_manager.clear();
    }
}
