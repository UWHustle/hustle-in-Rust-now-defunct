use hustle_catalog::Catalog;
use hustle_common::plan::Literal;
use hustle_storage::StorageManager;

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;
use hustle_types::{Int64, Char};

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
        let block = self.router.get_block(storage_manager);
        let mut insert_guard = block.lock_insert();
        for (literal, buf) in self.values.iter().zip(insert_guard.insert_row()) {
            match literal {
                Literal::Int(i) => {
                    Int64.set(*i, buf);
                },
                Literal::String(s) => {
                    Char::new(s.len()).set(s, buf);
                },
            }
        }
    }
}
