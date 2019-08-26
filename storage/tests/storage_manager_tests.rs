extern crate hustle_storage;

#[macro_use]
extern crate lazy_static;

#[cfg(test)]
mod storage_manager_tests {
    use hustle_storage::StorageManager;

    lazy_static! {
        static ref STORAGE_MANAGER: StorageManager = {
            StorageManager::new()
        };
    }

    #[test]
    fn create_block() {
        let block_id = STORAGE_MANAGER.create_block(vec![1], 0).id;
        assert!(STORAGE_MANAGER.get_block(block_id).is_some());
        STORAGE_MANAGER.delete_block(block_id);
    }

    #[test]
    fn delete_block() {
        let block_id = STORAGE_MANAGER.create_block(vec![1], 0).id;
        STORAGE_MANAGER.delete_block(block_id);
        assert!(STORAGE_MANAGER.get_block(block_id).is_none());
    }
}
