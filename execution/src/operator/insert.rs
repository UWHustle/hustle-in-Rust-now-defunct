use hustle_catalog::Catalog;
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Insert {
    values: Vec<Vec<u8>>,
    router: BlockPoolDestinationRouter,
}

impl Insert {
    pub fn new(values: Vec<Vec<u8>>, router: BlockPoolDestinationRouter) -> Self {
        Insert {
            values,
            router,
        }
    }
}

impl Operator for Insert {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        let block = self.router.get_block(storage_manager);
        block.insert(self.values.iter().map(|v| &v[..]));
    }
}
