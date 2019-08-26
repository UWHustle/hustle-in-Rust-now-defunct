use hustle_catalog::{Catalog, Column};
use hustle_common::plan::Literal;
use hustle_storage::StorageManager;
use hustle_types::{Char, Int64, TypeVariant, HustleType};

use crate::operator::Operator;
use crate::router::BlockPoolDestinationRouter;

pub struct Insert {
    columns: Vec<Column>,
    literals: Vec<Literal>,
    router: BlockPoolDestinationRouter,
}

impl Insert {
    pub fn new(columns: Vec<Column>, literals: Vec<Literal>, router: BlockPoolDestinationRouter) -> Self {
        Insert {
            columns,
            literals,
            router,
        }
    }
}

impl Operator for Insert {
    fn execute(&self, storage_manager: &StorageManager, _catalog: &Catalog) {
        let block = self.router.get_block(storage_manager);
        let mut insert_guard = block.lock_insert();
        for ((column, literal), buf) in self.columns.iter().zip(&self.literals).zip(insert_guard.insert_row()) {
            let type_variant = column.get_type_variant();
            match literal {
                Literal::Int(i) => {
                    match type_variant {
                        TypeVariant::Int8(t) => {
                            t.set(*i as i8, buf);
                        },
                        TypeVariant::Int16(t) => {
                            t.set(*i as i16, buf);
                        },
                        TypeVariant::Int32(t) => {
                            t.set(*i as i32, buf);
                        },
                        TypeVariant::Int64(t) => {
                            t.set(*i as i64, buf);
                        }
                        _ => panic!("Cannot assign integer to column of type {:?}", type_variant),
                    }
                },
                Literal::String(s) => {
                    match type_variant {
                        TypeVariant::Char(t) => {
                            assert_eq!(s.len(), t.byte_len(), "Incorrect length string");
                            t.set(s, buf);
                        },
                        _ => panic!("Cannot assign string to column of type {:?}", type_variant),
                    }
                },
            }
        }
    }
}
