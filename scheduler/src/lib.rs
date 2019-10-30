mod operator;
mod collect;
mod router;
mod worker;

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Sender};
use std::thread;

use hustle_storage::StorageManager;
use hustle_catalog::{Catalog, Table, Column};
use hustle_common::plan::{Expression, Plan, Query, QueryOperator};
use hustle_storage::block::{BlockReference, RowMask};

//use crate::operator::{Operator, CreateTable, Delete, DropTable, Insert, Project, Select, TableReference, Update};
use crate::operator::*;
use crate::collect::Collect;
use crate::router::BlockPoolDestinationRouter;
use crate::worker::Worker;



pub struct Scheduler {
    storage_manager: Arc<StorageManager>,
    catalog: Arc<Mutex<Catalog>>,
    //work_orders: Arc<Mutex<VecDeque<Box<dyn WorkOrder>>>>,
    worker_txs: Vec<Sender<Box<dyn WorkOrder>>>,
    worker_states: Arc<Mutex<Vec<bool>>>,
    curr_worker_i: usize,
}

impl Scheduler {
    pub fn new(storage_manager: StorageManager, catalog: Catalog) -> Self {
        let worker_states = Arc::new(Mutex::new(vec![false; core_affinity::get_core_ids().unwrap().len()]));
        let worker_txs = core_affinity::get_core_ids().unwrap()
            .into_iter()
            .enumerate()
            .map(|(id, core_id)| {
                let (tx, rx) = mpsc::channel();
                Worker::new(id, core_id, worker_states.clone(), rx);
                tx
            }).collect();

        Scheduler {
            storage_manager: Arc::new(storage_manager),
            catalog: Arc::new(Mutex::new(catalog)),
            //work_orders: Arc::new(Mutex::new(VecDeque::new())),
            worker_txs,
            worker_states,
            curr_worker_i: 0,
        }
    }

    pub fn execute(&mut self, plan: Plan) -> Result<Option<Table>, String> {
        let (work_order_tx, work_order_rx) = mpsc::channel();
        match plan {
            Plan::Query(query) => {
                let cols = query.output.clone();
                let (block_tx, block_rx) = mpsc::channel();
                let collect = Collect::new(cols, block_rx);

                let mut operators = Vec::new();
                self.compile_query(query, block_tx, work_order_tx, &mut operators);

                thread::spawn(move || {
                    let mut i = 1;
                    for operator in operators.into_iter() {
                        operator.push_work_orders();
                        println!("push_work_orders {} finished", i);
                        i += 1;
                    }
                });

                let mut i = 1;
                for work_order in work_order_rx {
                    self.send(work_order);
                    println!("work_order sent: {}", i);
                    i += 1;
                }
                Ok(Some(collect.get_result()))
            }
            _ => {
                let operator = self.compile_other(plan, work_order_tx);
                operator.push_work_orders();

                for work_order in work_order_rx.recv() {
                    self.send(work_order);
                }

                while self.worker_states.lock().unwrap().iter().filter(|&&s| s).count() != 0 {}
                Ok(None)
            }
        }
    }

    fn send(&mut self, work_order: Box<dyn WorkOrder>) {
        while *self.worker_states.lock().unwrap().get(self.curr_worker_i).unwrap() {
            self.curr_worker_i = (self.curr_worker_i + 1) % self.worker_txs.len();
        }
        let curr_worker_i = self.curr_worker_i;
        self.curr_worker_i = (self.curr_worker_i + 1) % self.worker_txs.len();

        self.worker_states.lock().unwrap()[curr_worker_i] = true;
        self.worker_txs.get(curr_worker_i).unwrap().send(work_order).unwrap();
    }

    fn compile_other(&self,
                     plan: Plan,
                     work_order_tx: Sender<Box<dyn WorkOrder>>
    ) -> Box<dyn Operator> {
        match plan {
            Plan::CreateTable(table) => Box::new(CreateTable::new(
                self.catalog.clone(),
                table,
                work_order_tx
            )),
            Plan::DropTable(table) => Box::new(DropTable::new(
                self.storage_manager.clone(),
                self.catalog.clone(),
                table,
                work_order_tx
            )),
            Plan::Insert { into_table, bufs } => {
                let table_name = into_table.name.clone();
                let router = BlockPoolDestinationRouter::with_block_ids(
                    into_table.block_ids,
                    into_table.columns,
                    None
                );
                Box::new(Insert::new(
                    self.storage_manager.clone(),
                    self.catalog.clone(),
                    table_name,
                    bufs,
                    router,
                    work_order_tx
                ))
            },
            Plan::Update { table, assignments, filter } => {
                let filter = filter.map(|f| self.compile_filter(*f, &table.columns));
                Box::new(Update::new(
                    self.storage_manager.clone(),
                    assignments,
                    filter,
                    table.block_ids,
                    work_order_tx
                ))
            },
            Plan::Delete { from_table, filter } => {
                let filter = filter.map(|f| self.compile_filter(*f, &from_table.columns));
                Box::new(Delete::new(
                    self.storage_manager.clone(),
                    filter,
                    from_table.block_ids,
                    work_order_tx
                ))
            },
            _ => panic!("Unsupported plan: {:?}", plan)
        }
    }

    fn compile_query(&self,
                     query: Query,
                     block_tx: Sender<u64>,
                     work_order_tx: Sender<Box<dyn WorkOrder>>,
                     operators: &mut Vec<Box<dyn Operator>>
    ) {
        let router = Arc::new(BlockPoolDestinationRouter::new(query.output, Some(block_tx.clone())));
        match query.operator {
            QueryOperator::TableReference(table) => {
                let table_reference = TableReference::new(
                    table,
                    block_tx,
                    work_order_tx
                );
                operators.push(Box::new(table_reference));
            },
            QueryOperator::Project { input, cols } => {
                //let router = Arc::new(BlockPoolDestinationRouter::new(query.output));
                let (child_block_tx, block_rx) = mpsc::channel();
                self.compile_query(*input, child_block_tx, work_order_tx.clone(), operators);
                let project = Project::new(
                    self.storage_manager.clone(),
                    cols,
                    router,
                    block_rx,
                    block_tx,
                    work_order_tx
                );
                operators.push(Box::new(project));
            },
            QueryOperator::Select { input, filter } => {
                //let router = Arc::new(BlockPoolDestinationRouter::new(query.output));
                let filter = self.compile_filter(*filter, &input.output);
                let (child_block_tx, block_rx) = mpsc::channel();
                self.compile_query(*input, child_block_tx, work_order_tx.clone(), operators);

                let select = Select::new(
                    self.storage_manager.clone(),
                    filter,
                    router,
                    block_rx,
                    block_tx,
                    work_order_tx
                );
                operators.push(Box::new(select));
            },
            QueryOperator::Cartesian { inputs: _ } => {
//                let block_rxs = inputs.into_iter()
//                    .map(|input| {
//                        let (child_block_tx, block_rx) = mpsc::channel();
//                        Self::compile_query(input, child_block_tx, operators);
//                        block_rx
//                    })
//                    .collect::<Vec<_>>();
//
//                let cartesian = Cartesian::new(router, block_rxs, block_tx);
//                operators.push(Box::new(cartesian));
            },
        }
    }

    fn compile_filter(
        &self,
        filter: Expression,
        columns: &[Column],
    ) -> Box<dyn Fn(&BlockReference) -> RowMask> {
        match filter {
            Expression::Comparative { variant: comparative_variant, left, right } => {
                let l_col_i = match *left {
                    Expression::ColumnReference(column) => column,
                    _ => panic!("Only column references are allowed on left side of comparisons"),
                };
                let l_type_variant = columns[l_col_i].get_type_variant().clone();

                match *right {
                    Expression::Literal { buf: r_buf, type_variant: r_type_variant } => {
                        Box::new(move |block|
                            block.filter_col(l_col_i, |l_buf|
                                hustle_types::compare(
                                    comparative_variant,
                                    &l_type_variant,
                                    &r_type_variant,
                                    l_buf,
                                    &r_buf,
                                ).unwrap()
                            )
                        )
                    },
                    Expression::ColumnReference(r_col_i) => {
                        let r_type_variant = columns[r_col_i].get_type_variant().clone();
                        Box::new(move |block|
                            block.filter_cols(l_col_i, r_col_i, |l_buf, r_buf|
                                hustle_types::compare(
                                    comparative_variant,
                                    &l_type_variant,
                                    &r_type_variant,
                                    l_buf,
                                    r_buf,
                                ).unwrap()
                            )
                        )
                    },
                    _ => panic!("")
                }
            },
            Expression::Conjunctive { terms } => {
                let compiled_terms = terms.into_iter()
                    .map(|term| self.compile_filter(term, columns))
                    .collect::<Vec<_>>();

                Box::new(move |block| {
                    let mut compiled_terms_iter = compiled_terms.iter();
                    let mut mask = (compiled_terms_iter.next().unwrap())(block);
                    for compiled_term in compiled_terms_iter {
                        mask.intersect(&(compiled_term)(block));
                    }
                    mask
                })
            },
            Expression::Disjunctive { terms } => {
                let compiled_terms = terms.into_iter()
                    .map(|term| self.compile_filter(term, columns))
                    .collect::<Vec<_>>();

                Box::new(move |block| {
                    let mut compiled_terms_iter = compiled_terms.iter();
                    let mut mask = (compiled_terms_iter.next().unwrap())(block);
                    for compiled_term in compiled_terms_iter {
                        mask.union(&(compiled_term)(block));
                    }
                    mask
                })
            }
            _ => panic!("Unsupported expression node type"),
        }
    }

}



#[cfg(test)]
mod scheduler_tests {
    use hustle_types::{TypeVariant, Bool, Char, HustleType, Int64, ComparativeVariant};
    use super::*;

    #[test]
    fn test_query() {
        let mut scheduler = Scheduler::new(
            StorageManager::with_unique_data_directory(),
            Catalog::new()
        );
        create_table(&mut scheduler);
        insert(&mut scheduler);

        println!("----- test_query -----");
        let table = scheduler.catalog.lock().unwrap().get_table("table").unwrap();
        let col_bool = table.columns.iter()
            .find(|c| c.get_name() == "col_bool")
            .unwrap().clone();
        let col_int64 = table.columns.iter()
            .find(|c| c.get_name() == "col_int64")
            .unwrap().clone();
        let col_char = table.columns.iter()
            .find(|c| c.get_name() == "col_char")
            .unwrap().clone();

        let plan = Plan::Query(Query {
            output: vec![col_bool.clone(), col_int64.clone()],
            operator: QueryOperator::Project {
                input: Box::new(Query {
                    output: vec![col_bool, col_int64, col_char],
                    operator: QueryOperator::Select {
                        input: Box::new(Query {
                            output: table.columns.clone(),
                            operator: QueryOperator::TableReference(table),
                        }),
                        filter: Box::new(Expression::Comparative {
                            variant: ComparativeVariant::Eq,
                            left: Box::new(Expression::ColumnReference(0)),
                            right: Box::new(Expression::Literal {
                                type_variant: TypeVariant::Bool(Bool),
                                buf: Bool.new_buf(true)
                            })
                        })
                    }
                }),
                cols: vec![0, 1],
            },
        });

        let table = scheduler.execute(plan).unwrap().unwrap();
        println!("{:#?}", table);
//        println!("{}", table.columns);

        scheduler.storage_manager.clear();
    }


    #[test]
    fn test_other() {
        let mut scheduler = Scheduler::new(
            StorageManager::with_unique_data_directory(),
            Catalog::new()
        );

        create_table(&mut scheduler);
        insert(&mut scheduler);
        update(&mut scheduler);
        delete(&mut scheduler);
        drop_table(&mut scheduler);
        scheduler.storage_manager.clear()
    }


    fn update(scheduler: &mut Scheduler) {
        let table = scheduler.catalog.lock().unwrap().get_table("table").unwrap();
        let plan = Plan::Update {
            table,
            assignments: vec![(0, Bool.new_buf(true)), (1, Int64.new_buf(3))],
            filter: None
        };
        let res = scheduler.execute(plan).unwrap();
        assert_eq!(res, None);
    }

    fn drop_table(scheduler: &mut Scheduler) {
        let table = scheduler.catalog.lock().unwrap().get_table("table").unwrap();
        let res = scheduler.execute(Plan::DropTable(table)).unwrap();

        assert_eq!(res, None);
        assert!(!scheduler.catalog.lock().unwrap().table_exists("table"));
    }

    fn delete(scheduler: &mut Scheduler) {
        let table = scheduler.catalog.lock().unwrap().get_table("table").unwrap();
        let plan = Plan::Delete {
            from_table: table,
            filter: Some(Box::new(
                Expression::Comparative {
                    variant: ComparativeVariant::Eq,
                    left: Box::new(Expression::ColumnReference(0)),
                    right: Box::new(Expression::Literal {
                        type_variant: TypeVariant::Bool(Bool),
                        buf: Bool.new_buf(true)
                    })
                }
            ))
        };
        let res = scheduler.execute(plan).unwrap();
        assert_eq!(res, None);
        //assert!(scheduler.catalog.lock().unwrap().table_exists("table"));
    }

    fn insert(scheduler: &mut Scheduler) {
        let table = scheduler.catalog.lock().unwrap().get_table("table").unwrap();

        let bool_type = Bool;
        let int64_type = Int64;
        let char_type = Char::new(1);
        let mut bufs = vec![
            vec![0; bool_type.byte_len()],
            vec![0; int64_type.byte_len()],
            vec![0; char_type.byte_len()],
        ];
        bool_type.set(true, &mut bufs[0]);
        int64_type.set(1, &mut bufs[1]);
        char_type.set("a", &mut bufs[2]);
        scheduler.execute(Plan::Insert{ into_table: table.clone(), bufs: bufs.clone()}).unwrap();

        bool_type.set(false, &mut bufs[0]);
        int64_type.set(2, &mut bufs[1]);
        char_type.set("b", &mut bufs[2]);
        scheduler.execute(Plan::Insert{ into_table: table, bufs}).unwrap();
    }


    fn create_table(scheduler: &mut Scheduler) {
        let table = Table::new(
            "table".to_owned(),
            vec![
                Column::new(
                    "col_bool".to_owned(),
                    "table".to_owned(),
                    TypeVariant::Bool(Bool),
                    false,
                ),
                Column::new(
                    "col_int64".to_owned(),
                    "table".to_owned(),
                    TypeVariant::Int64(Int64),
                    false,
                ),
                Column::new(
                    "col_char".to_owned(),
                    "table".to_owned(),
                    TypeVariant::Char(Char::new(1)),
                    false,
                ),
            ],
        );
        let plan = Plan::CreateTable(table);
        let res = scheduler.execute(plan).unwrap();

        assert_eq!(res, None);
        assert!(scheduler.catalog.lock().unwrap().table_exists("table"));
    }

}




