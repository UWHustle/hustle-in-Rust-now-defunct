use super::sum_column::sum_column_hustle;
use logical_entities::column::Column;
use logical_entities::predicates::Predicate;
use logical_entities::relation::Relation;
use physical_operators::aggregate::Aggregate;
use physical_operators::project::Project;
use physical_operators::*;
use storage::StorageManager;
use type_system::data_type::DataType;
use type_system::Value;
use physical_operators::update::Update;

pub fn hustle_agg(
    storage_manager: &StorageManager,
    relation: Relation,
    agg_in_name: &str,
    agg_out_type: DataType,
    agg_name: &str,
) -> i64 {
    let agg_col_in = relation.column_from_name(agg_in_name).unwrap();
    let agg_out_name = format!("{}({})", agg_name, agg_col_in.get_name());
    let agg_col_out = Column::new(&agg_out_name, agg_out_type);
    let agg_op = Aggregate::from_str(
        relation,
        agg_col_in,
        agg_col_out,
        vec![agg_out_name.clone()],
        agg_name,
    )
    .unwrap();
    sum_column_hustle(
        storage_manager,
        agg_op.execute(storage_manager).unwrap(),
        &agg_out_name,
    )
}

pub fn hustle_predicate(
    storage_manager: &StorageManager,
    relation: Relation,
    col_name: &str,
    predicate: Box<Predicate>,
) -> i64 {
    let column = relation.column_from_name(col_name).unwrap();
    let project_op = Project::new(relation, vec![column.clone()], predicate);
    sum_column_hustle(
        storage_manager,
        project_op.execute(storage_manager).unwrap(),
        column.get_name(),
    )
}

pub fn hustle_update(
    storage_manager: &StorageManager,
    relation: Relation,
    predicate: Box<Predicate>,
    col_name: &str,
    assignment: Box<Value>
) -> i64 {
    let column = relation.column_from_name(col_name).unwrap();
    let update_op = Update::new(
        relation,
        Some(predicate),
        vec![column.clone()],
        vec![assignment]);
    let relation = update_op.execute(storage_manager).unwrap();
    sum_column_hustle(
        storage_manager,
        relation,
        column.get_name()
    )
}