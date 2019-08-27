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
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        let output_block = self.router.get_block(storage_manager);
        output_block.insert_row(self.bufs.iter().map(|buf| buf.as_slice()))
    }
}
