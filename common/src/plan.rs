use hustle_catalog::{Table, Column};
use hustle_types::TypeVariant;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    BeginTransaction,
    CommitTransaction,
    Query {
        query: Query,
    },
    CreateTable {
        table: Table,
    },
    DropTable {
        table: Table,
    },
    Insert {
        into_table: Table,
        values: Vec<Expression>,
    },
    Delete {
        from_table: Table,
        filter: Option<Box<Expression>>,
    },
    Update {
        table: Table,
        columns: Vec<Column>,
        assignments: Vec<Expression>,
        filter: Option<Box<Expression>>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Query {
    pub operator: QueryOperator,
    pub output_types: Vec<TypeVariant>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum QueryOperator {
    Aggregate {
        input: Box<Query>,
        aggregates: Vec<AggregateFunction>,
        groups: Vec<Expression>,
    },
    Join {
        input: Vec<Query>,
        filter: Option<Box<Expression>>,
    },
    Limit {
        input: Box<Query>,
        limit: usize,
    },
    Project {
        input: Box<Query>,
        cols: Vec<usize>,
    },
    Select {
        input: Box<Query>,
        filter: Box<Expression>,
    },
    TableReference {
        table: Table,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Comparative {
        variant: ComparativeVariant,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Conjunctive {
        terms: Vec<Expression>,
    },
    Disjunctive {
        terms: Vec<Expression>,
    },
    Literal {
        literal: Literal,
    },
    ColumnReference {
        column: Column,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComparativeVariant {
    Eq, Lt, Le, Gt, Ge
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConnectiveVariant {
    And, Or
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateFunction {
    variant: AggregateFunctionVariant,
    column: Column
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunctionVariant {
    Avg, Count, Max, Min, Sum
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Int8(i8),
}
