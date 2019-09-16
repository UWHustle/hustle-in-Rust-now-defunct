use hustle_catalog::{Column, Table};
use hustle_types::{ComparativeVariant, TypeVariant};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Statement {
    pub id: u64,
    pub transaction_id: u64,
    pub plan: Plan,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    BeginTransaction,
    CommitTransaction,
    Query(Query),
    CreateTable(Table),
    DropTable(Table),
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Query {
    pub operator: QueryOperator,
    pub output: Vec<Column>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    TableReference(Table),
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
        type_variant: TypeVariant,
        buf: Vec<u8>,
    },
    ColumnReference(usize),
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
