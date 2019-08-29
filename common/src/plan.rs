use hustle_catalog::{Table, Column};

#[derive(Clone, Debug, Serialize, Deserialize)]
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
        bufs: Vec<Vec<u8>>,
    },
    Delete {
        from_table: Table,
        filter: Option<Box<Expression>>,
    },
    Update {
        table: Table,
        columns: Vec<usize>,
        bufs: Vec<Vec<u8>>,
        filter: Option<Box<Expression>>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Query {
    pub operator: QueryOperator,
    pub output: Vec<Column>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ComparativeVariant {
    Eq, Lt, Le, Gt, Ge
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConnectiveVariant {
    And, Or
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateFunction {
    variant: AggregateFunctionVariant,
    column: Column
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggregateFunctionVariant {
    Avg, Count, Max, Min, Sum
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Literal {
    Int(i64),
    String(String),
}
