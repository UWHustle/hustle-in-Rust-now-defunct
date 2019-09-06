use hustle_catalog::{Table, Column};
use hustle_types::TypeVariant;

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
        assignments: Vec<(usize, Vec<u8>)>,
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
    Cartesian {
        inputs: Vec<Query>,
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
        type_variant: TypeVariant,
        buf: Vec<u8>,
    },
    ColumnReference(usize),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ComparativeVariant {
    Eq, Lt, Le, Gt, Ge
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
