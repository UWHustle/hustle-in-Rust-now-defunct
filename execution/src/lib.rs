
pub mod logical_entities;
pub mod physical_operators;
pub mod storage_manager;

use logical_entities::column::ExtColumn;
use logical_entities::relation::ExtRelation;

use physical_operators::select_sum::SelectSum;

#[no_mangle]
pub extern fn sum_column(c_relation: ExtRelation, c_column: ExtColumn) -> String{
    let column = c_column.to_column();

    let relation = c_relation.to_relation();

    let sum_operator = SelectSum::new(relation,column);
    let result = sum_operator.execute();

    println!("Summing {}", result);
    result
}

use physical_operators::join::Join;
use physical_operators::print::Print;
use physical_operators::Operator;

#[no_mangle]
pub extern fn join(c_relation_left: ExtRelation, c_relation_right: ExtRelation) -> bool{
    let relation_l = c_relation_right.to_relation();
    let relation_r = c_relation_left.to_relation();

    let join_operator = Join::new(relation_l, relation_r);

    let result_relation = join_operator.execute();

    let print_operator = Print::new(result_relation);
    print_operator.execute();
    true
}
