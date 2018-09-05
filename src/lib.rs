
pub mod logical_entities;
pub mod physical_operators;

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

use logical_entities::row::ExtRow;
use physical_operators::insert::Insert;

#[no_mangle]
pub extern fn insert(c_relation: ExtRelation, c_row: ExtRow) -> u64{
    println!("Start");
    let row = c_row.to_row();
    println!("row");
    let relation = c_relation.to_relation();
    println!("Relation");

    let insert_operator = Insert::new(relation, row);
    println!("Start execute");
    insert_operator.execute();
    1
}

use physical_operators::join::Join;
use physical_operators::select_output::SelectOutput;

#[no_mangle]
pub extern fn join(c_relation_left: ExtRelation, c_relation_right: ExtRelation) -> bool{
    let relation_l = c_relation_right.to_relation();
    let relation_r = c_relation_left.to_relation();

    let join_operator = Join::new(relation_l, relation_r);

    let result_relation = join_operator.execute();

    let mut select_operator = SelectOutput::new(result_relation);
    select_operator.execute();
    select_operator.print();
    true

    //select_operator.get_result().iter().map(|row:&Row|{cRow::from_row(row.clone())}).collect()
}
