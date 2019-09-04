use std::sync::{Arc, mpsc};
use std::sync::mpsc::{Receiver, Sender};

use hustle_catalog::{Catalog, Column, Table};
use hustle_common::message::Message;
use hustle_common::plan::{ComparativeVariant, Expression, Plan, Query, QueryOperator};
use hustle_storage::block::{BlockReference, RowMask};
use hustle_storage::StorageManager;
use hustle_types::{CompareEq, CompareOrd, TypeVariant};

use crate::operator::{Cartesian, Collect, CreateTable, Delete, DropTable, Insert, Operator, Project, Select, TableReference, Update};
use crate::router::BlockPoolDestinationRouter;

macro_rules! match_types_and_compile_comparative {
    (
        $comparison:ident,
        $left_type_variant:path,
        $left_col_i:path,
        $right_expr:path,
        $columns:path,
        $(($left:ident, $right:ident),)+
    ) => {
        match $right_expr {
            Expression::Literal { buf: right_buf, type_variant: right_type_variant } => {
                 match ($left_type_variant, right_type_variant) {
                    $((TypeVariant::$left(left_type), TypeVariant::$right(right_type)) =>
                        Box::new(move |block: &BlockReference|
                            block.filter_col(
                                $left_col_i,
                                |left_buf| left_type.$comparison(&right_type, left_buf, &right_buf),
                            )
                        ),)+
                    _ => panic!("Invalid comparison"),
                }
            },
            Expression::ColumnReference(right_col_i) => {
                let right_type_variant = $columns[right_col_i].get_type_variant().clone();
                match ($left_type_variant, right_type_variant) {
                    $((TypeVariant::$left(left_type), TypeVariant::$right(right_type)) =>
                        Box::new(move |block: &BlockReference|
                            block.filter_cols(
                                $left_col_i,
                                right_col_i,
                                |left_buf, right_buf| left_type.$comparison(
                                    &right_type,
                                    left_buf,
                                    right_buf
                                ),
                            )
                        ),)+
                    _ => panic!("Invalid comparison"),
                }
            }
            _ => panic!(""),
        }
    };
}

macro_rules! fn_compile_comparative {
    ([$(($left_eq:ident, $right_eq:ident),)+], [$(($left_ord:ident, $right_ord:ident),)+],) => {
        fn compile_comparative(
            comparative_variant: ComparativeVariant,
            left_type_variant: TypeVariant,
            left_col_i: usize,
            right_expr: Expression,
            columns: &[Column],
        ) -> Box<dyn Fn(&BlockReference) -> RowMask> {
            match comparative_variant {
                ComparativeVariant::Eq => {
                    match_types_and_compile_comparative!(
                        eq,
                        left_type_variant,
                        left_col_i,
                        right_expr,
                        columns,
                        $(($left_eq, $right_eq),)+
                    )
                },
                ComparativeVariant::Lt => {
                    match_types_and_compile_comparative!(
                        lt,
                        left_type_variant,
                        left_col_i,
                        right_expr,
                        columns,
                        $(($left_ord, $right_ord),)+
                    )
                },
                ComparativeVariant::Le => {
                    match_types_and_compile_comparative!(
                        le,
                        left_type_variant,
                        left_col_i,
                        right_expr,
                        columns,
                        $(($left_ord, $right_ord),)+
                    )
                },
                ComparativeVariant::Gt => {
                    match_types_and_compile_comparative!(
                        gt,
                        left_type_variant,
                        left_col_i,
                        right_expr,
                        columns,
                        $(($left_ord, $right_ord),)+
                    )
                },
                ComparativeVariant::Ge => {
                    match_types_and_compile_comparative!(
                        ge,
                        left_type_variant,
                        left_col_i,
                        right_expr,
                        columns,
                        $(($left_ord, $right_ord),)+
                    )
                },
            }
        }
    };
}

fn_compile_comparative!(
    [
        // Match arms are generated for each of these type pairs for equal comparisons.
        (Bool, Bool),
        (Int8, Int8),
        (Int8, Int16),
        (Int8, Int32),
        (Int8, Int64),
        (Int16, Int8),
        (Int16, Int16),
        (Int16, Int32),
        (Int16, Int64),
        (Int32, Int8),
        (Int32, Int16),
        (Int32, Int32),
        (Int32, Int64),
        (Int64, Int8),
        (Int64, Int16),
        (Int64, Int32),
        (Int64, Int64),
        (Char, Char),
        (Bits, Bits),
    ],
    [
        // Match arms are generated for each of these type pairs for ordered comparisons.
        (Bool, Bool),
        (Int8, Int8),
        (Int8, Int16),
        (Int8, Int32),
        (Int8, Int64),
        (Int16, Int8),
        (Int16, Int16),
        (Int16, Int32),
        (Int16, Int64),
        (Int32, Int8),
        (Int32, Int16),
        (Int32, Int32),
        (Int32, Int64),
        (Int64, Int8),
        (Int64, Int16),
        (Int64, Int32),
        (Int64, Int64),
        (Char, Char),
    ],
);


pub struct ExecutionEngine {
    storage_manager: StorageManager,
    catalog: Arc<Catalog>,
}

impl ExecutionEngine {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        ExecutionEngine {
            storage_manager: StorageManager::default(),
            catalog
        }
    }

    pub fn execute_plan(&self, plan: Plan) -> Result<Option<Table>, String> {
        let operator = Self::compile_plan(plan);
        operator.execute(&self.storage_manager, &self.catalog);
        let table = operator.downcast_ref::<Collect>().map(|collect| collect.get_table());
        Ok(table)
    }

    pub fn listen(
        &mut self,
        execution_rx: Receiver<Message>,
        transaction_tx: Sender<Message>,
        completed_tx: Sender<Message>,
    ) {
        for message in execution_rx {
            if let Message::ExecutePlan { plan, statement_id, connection_id } = message {
                match self.execute_plan(plan) {
                    Ok(relation) => {

                        // The execution may have produced a result relation. If so, we send the
                        // rows back to the user.
                        if let Some(relation) = relation {

                            // Send a message with the result schema.
                            completed_tx.send(Message::Schema {
                                schema: relation.columns.clone(),
                                connection_id
                            }).unwrap();

                            // Send each row of the result.
                            for &block_id in &relation.block_ids {
                                let block = self.storage_manager.get_block(block_id).unwrap();
                                for row in block.rows() {
                                    completed_tx.send(Message::ReturnRow {
                                        row: row.map(|value| value.to_vec()).collect(),
                                        connection_id,
                                    }).unwrap();
                                }
                            }
                        }

                        // Send a success message to indicate completion.
                        completed_tx.send(Message::Success { connection_id }).unwrap();
                    },

                    Err(reason) => {
                        completed_tx.send(Message::Failure {
                            reason,
                            connection_id,
                        }).unwrap();
                    },
                };

                // Notify the transaction manager that the plan execution has completed.
                transaction_tx.send(Message::CompletePlan {
                    statement_id,
                    connection_id,
                }).unwrap();
            }
        }
    }

    fn compile_plan(plan: Plan) -> Box<dyn Operator> {
        match plan {
            Plan::CreateTable { table } => Box::new(CreateTable::new(table)),
            Plan::DropTable { table } => Box::new(DropTable::new(table)),
            Plan::Insert { into_table, bufs } => {
                let router = BlockPoolDestinationRouter::with_block_ids(
                    into_table.block_ids,
                    into_table.columns,
                );
                Box::new(Insert::new(bufs, router))
            },
            Plan::Update { table, assignments, filter } => {
                let filter = filter.map(|f| Self::compile_filter(*f, &table.columns));
                Box::new(Update::new(assignments, filter, table.block_ids))
            },
            Plan::Delete { from_table, filter } => {
                let filter = filter.map(|f| Self::compile_filter(*f, &from_table.columns));
                Box::new(Delete::new(filter, from_table.block_ids))
            },
            Plan::Query { query } => {
                let cols = query.output.clone();
                let (block_tx, block_rx) = mpsc::channel();
                let mut operators = Vec::new();
                Self::compile_query(query, block_tx, &mut operators);
                Box::new(Collect::new(operators, cols, block_rx))
            },
            _ => panic!("Unsupported plan: {:?}", plan),
        }
    }

    fn compile_query(query: Query, block_tx: Sender<u64>, operators: &mut Vec<Box<dyn Operator>>) {
        let router = BlockPoolDestinationRouter::new(query.output);
        match query.operator {
            QueryOperator::TableReference { table } => {
                operators.push(Box::new(TableReference::new(table, block_tx)));
            },
            QueryOperator::Project { input, cols } => {
                let (child_block_tx, block_rx) = mpsc::channel();
                Self::compile_query(*input, child_block_tx, operators);

                let project = Project::new(cols, router, block_rx, block_tx);
                operators.push(Box::new(project));
            },
            QueryOperator::Select { input, filter } => {
                let filter = Self::compile_filter(*filter, &input.output);
                let (child_block_tx, block_rx) = mpsc::channel();
                Self::compile_query(*input, child_block_tx, operators);

                let select = Select::new(filter, router, block_rx, block_tx);
                operators.push(Box::new(select));
            },
            QueryOperator::Cartesian { inputs } => {
                let block_rxs = inputs.into_iter()
                    .map(|input| {
                        let (child_block_tx, block_rx) = mpsc::channel();
                        Self::compile_query(input, child_block_tx, operators);
                        block_rx
                    })
                    .collect::<Vec<_>>();

                let cartesian = Cartesian::new(router, block_rxs, block_tx);
                operators.push(Box::new(cartesian));
            },
        }
    }

    fn compile_filter(
        filter: Expression,
        columns: &[Column],
    ) -> Box<dyn Fn(&BlockReference) -> RowMask> {
        match filter {
            Expression::Comparative { variant: comparative_variant, left, right: right_expr } => {
                let left_col_i = match *left {
                    Expression::ColumnReference(column) => column,
                    _ => panic!("Only column references are allowed on left side of comparisons"),
                };
                let left_type_variant = columns[left_col_i].get_type_variant().clone();

                compile_comparative(
                    comparative_variant,
                    left_type_variant,
                    left_col_i,
                    *right_expr,
                    columns
                )
            },
            _ => panic!(""),
        }
    }
}
