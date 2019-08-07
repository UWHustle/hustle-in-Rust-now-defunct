use hustle_catalog::{Table, Column};

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
    operator: QueryOperator,
    output: Vec<Column>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum QueryOperator {
    Aggregate {
        input: Box<Query>,
        aggregates: Vec<AggregateFunction>,
        groups: Vec<Expression>,
    },
    Join {
        l_input: Box<Query>,
        r_input: Box<Query>,
        filter: Option<Box<Expression>>,
    },
    Limit {
        input: Box<Query>,
        limit: usize,
    },
    Project {
        input: Box<Query>,
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
    Connective {
        variant: ConnectiveVariant,
        terms: Vec<Expression>,
    },
    Function {
        name: String,
        arguments: Vec<Expression>,
        output_type: String,
    },
    Literal {
        value: String,
        literal_type: String,
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
