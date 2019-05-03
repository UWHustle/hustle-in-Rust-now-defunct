use storage::StorageManager;

static mut STORAGE_MANAGER: *const StorageManager = 0 as *const StorageManager;

// TODO: This has the potential to cause concurrency issues (although this is unlikely)
// Namely, if one thread calls get() while another thread is initializing the singleton
pub fn get() -> &'static StorageManager {
    unsafe {
        if STORAGE_MANAGER == 0 as *const StorageManager {
            STORAGE_MANAGER = Box::into_raw(Box::new(StorageManager::new()));
        }
        &(*STORAGE_MANAGER)
    }
}
