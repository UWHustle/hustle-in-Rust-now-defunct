use hustle_catalog::Catalog;
use hustle_common::plan::Literal;
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Insert {
    values: Vec<Literal>,
    router: BlockPoolDestinationRouter,
}

impl Insert {
    pub fn new(values: Vec<Literal>, router: BlockPoolDestinationRouter) -> Self {
        Insert {
            values,
            router,
        }
    }
}

impl Operator for Insert {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        unimplemented!()
    }
}
