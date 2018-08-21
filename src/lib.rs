mod physical_operator;
mod logical_operator;




use logical_operator::column::cColumn;
use logical_operator::logical_relation::cRelation;

use physical_operator::select_sum::SelectSum;

#[no_mangle]
pub extern fn sum_column(c_relation: cRelation, c_column: cColumn) -> u64{
    let column = c_column.to_column();

    let relation = c_relation.to_relation();

    let sum_operator = SelectSum::new(relation,column);
    let result = sum_operator.execute();

    println!("Summing {}", result as u64);
    result as u64
}

use logical_operator::row::cRow;
use physical_operator::insert::Insert;

#[no_mangle]
pub extern fn insert(c_relation: cRelation, c_row: cRow) -> u64{
    let row = c_row.to_row();
    let relation = c_relation.to_relation();

    let insert_operator = Insert::new(relation, row);
    println!("Start execute");
    insert_operator.execute();
    1
}
