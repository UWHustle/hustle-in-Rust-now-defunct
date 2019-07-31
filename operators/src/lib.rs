pub mod aggregate;
pub mod build_hash;
pub mod query_plan;
pub mod sort;
pub mod star_join_aggregate;
pub mod work_orders_container;

pub use aggregate::Aggregate;
pub use build_hash::BuildHash;
pub use query_plan::{QueryPlan, QueryPlanDag};
pub use sort::Sort;
pub use star_join_aggregate::StarJoinAggregate;
pub use work_orders_container::WorkOrdersContainer;

use std::cmp::Reverse;

use hustle_common::{
    BinaryOperation, ColumnType, Comparison, Literal, Predicate, Scalar, UnaryOperation,
};
use hustle_storage::relational_block::RelationalBlock;
use std::collections::HashMap;

pub enum InputFeed {
    Block(usize, String),
}

pub trait Operator /*: std::fmt::Debug*/ {
    fn get_all_work_orders(&mut self, work_orders_container: &mut WorkOrdersContainer) -> bool;

    fn feed_input(&mut self, input_feed: Option<InputFeed>);

    fn done_feeding_input(&mut self, relation_name: Option<String>);
}

pub trait WorkOrder {
    fn execute(&mut self, storage_manager: std::sync::Arc<hustle_storage::StorageManager>);
}

fn evaluate_scalar(scalar: &Scalar, block: &RelationalBlock, rid: usize) -> Literal {
    match scalar {
        Scalar::ScalarLiteral(l) => l.clone(),
        Scalar::ScalarAttribute(c) => {
            let cid = c.id();
            let bytes = block.get_row_col(rid, cid).unwrap();
            match c.column__type {
                ColumnType::I32 => Literal::Int32(unsafe { *(bytes.as_ptr() as *const i32) }),
                ColumnType::I64 => Literal::Int64(unsafe { *(bytes.as_ptr() as *const i64) }),
                ColumnType::Char(_) => {
                    Literal::Char(std::str::from_utf8(bytes).unwrap().to_owned())
                }
                _ => Literal::Int32(0),
            }
        }
        Scalar::UnaryExpression { operation, operand } => {
            let operand_literal = evaluate_scalar(&*operand, block, rid);
            match operation {
                UnaryOperation::Negate => match operand_literal {
                    Literal::Int32(i) => Literal::Int32(-i),
                    Literal::Int32Reverse(Reverse(i)) => Literal::Int32Reverse(Reverse(-i)),
                    Literal::Int64(i) => Literal::Int64(-i),
                    Literal::Int64Reverse(Reverse(i)) => Literal::Int64Reverse(Reverse(-i)),
                    Literal::Char(_) | Literal::CharReverse(_) => operand_literal,
                },
            }
        }
        Scalar::BinaryExpression {
            operation,
            left,
            right,
        } => {
            let left_literal = evaluate_scalar(&*left, block, rid);
            let right_literal = evaluate_scalar(&*right, block, rid);
            match operation {
                BinaryOperation::Add => panic!("TODO add"),
                BinaryOperation::Substract => match left_literal {
                    Literal::Int32(l) => match right_literal {
                        Literal::Int32(r) => Literal::Int32(l - r),
                        _ => panic!("TODO"),
                    },
                    _ => panic!("TODO"),
                },
                BinaryOperation::Multiply => match left_literal {
                    Literal::Int32(l) => match right_literal {
                        Literal::Int32(r) => Literal::Int32(l * r),
                        _ => panic!("TODO"),
                    },
                    _ => panic!("TODO"),
                },
                BinaryOperation::Divide => match left_literal {
                    Literal::Int32(l) => match right_literal {
                        Literal::Int32(r) => Literal::Int32(l / r),
                        _ => panic!("TODO"),
                    },
                    _ => panic!("TODO"),
                },
                BinaryOperation::Modulo => panic!("TODO modulo"),
            }
        }
    }
}

fn evaluate_predicate(
    predicate: &Predicate,
    block: &RelationalBlock,
    rid: usize,
    common_sub_exprs: &mut Option<&mut Literal>,
) -> bool {
    match predicate {
        Predicate::True => true,
        Predicate::False => false,
        Predicate::Between {
            operand,
            begin,
            end,
        } => {
            let operand_literal = evaluate_scalar(operand, block, rid);
            if let Scalar::ScalarLiteral(l) = &**begin {
                if operand_literal < *l {
                    return false;
                }
            }

            if let Scalar::ScalarLiteral(r) = &**end {
                if operand_literal > *r {
                    return false;
                }
            }

            true
        }
        Predicate::Comparison {
            comparison,
            left,
            right,
        } => {
            let left_literal = evaluate_scalar(left, block, rid);
            if let Some(sub_exprs) = common_sub_exprs {
                **sub_exprs = left_literal.clone();
            }
            let right_literal = evaluate_scalar(right, block, rid);
            match comparison {
                Comparison::Equal => left_literal == right_literal,
                Comparison::NotEqual => left_literal != right_literal,
                Comparison::Less => left_literal < right_literal,
                Comparison::LessEqual => left_literal <= right_literal,
                Comparison::Greater => left_literal > right_literal,
                Comparison::GreaterEqual => left_literal >= right_literal,
            }
        }
        Predicate::Negation { operand } => {
            !evaluate_predicate(operand, block, rid, common_sub_exprs)
        }
        Predicate::Conjunction {
            static_operand_list,
            dynamic_operand_list,
        } => {
            for static_predicate in static_operand_list {
                match static_predicate {
                    Predicate::True => (),
                    Predicate::False => return false,
                    _ => panic!("Unreachable"),
                };
            }

            for dynamic_predicate in dynamic_operand_list {
                if !evaluate_predicate(dynamic_predicate, block, rid, common_sub_exprs) {
                    return false;
                }
            }

            true
        }
        Predicate::Disjunction {
            static_operand_list,
            dynamic_operand_list,
        } => {
            for static_predicate in static_operand_list {
                match static_predicate {
                    Predicate::True => return true,
                    Predicate::False => (),
                    _ => panic!("Unreachable"),
                };
            }

            for dynamic_predicate in dynamic_operand_list {
                if evaluate_predicate(dynamic_predicate, block, rid, common_sub_exprs) {
                    return true;
                }
            }

            false
        }
    }
}
