use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

use hustle_types::data_type::DataType;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    CloseConnection { connection_id: u64 },
    ExecuteSql { sql: String },
    ParseSql { sql: String, connection_id: u64 },
    ResolveAst { ast: String, connection_id: u64 },
    OptimizePlan { plan: Plan, connection_id: u64 },
    TransactPlan { plan: Plan, connection_id: u64 },
    ExecutePlan { plan: Plan, statement_id: u64, connection_id: u64 },
    CompletePlan { statement_id: u64, connection_id: u64 },
    Schema { schema: Vec<(String, DataType)>, connection_id: u64 },
    ReturnRow { row: Vec<Vec<u8>>, connection_id: u64 },
    Success { connection_id: u64 },
    Failure { reason: String, connection_id: u64 },
}

impl Message {
    pub fn send<W>(&self, buf: &mut W) -> Result<(), String> where W: WriteBytesExt {
        let payload = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        buf.write_u32::<BigEndian>(payload.len() as u32).map_err(|e| e.to_string())?;
        buf.write_all(&payload).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn receive<R>(buf: &mut R) -> Result<Self, String> where R: ReadBytesExt {
        let payload_len = buf.read_u32::<BigEndian>().map_err(|e| e.to_string())?;
        let mut payload = vec![0; payload_len as usize];
        buf.read_exact(&mut payload).map_err(|e| e.to_string())?;
        serde_json::from_slice(&payload).map_err(|e| e.to_string())
    }

    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        let mut buf = vec![];
        self.send(&mut buf)?;
        Ok(buf)
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(buf);
        Self::receive(&mut cursor)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Ast {
    BeginTransaction,
    CommitTransaction,
    CreateTable {
        table: String,
        columns: Vec<AstColumnDefinition>,
    },
    Delete {
        from_table: String,
        filter: Option<AstExpression>,
    },
    DropTable {
        table: String,
    },
    Insert {
        into_table: String,
        input: Vec<AstLiteral>,
    },
    Select {
        from_table: String,
        filter: Option<AstExpression>,
        projection: Vec<AstColumnReference>
    },
    Update {
        table: String,
        assignments: Vec<AstAssignment>,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AstExpression {
    Comparative {
        name: String,
        left: Box<AstExpression>,
        right: Box<AstExpression>,
    },
    Connective {
        name: String,
        left: Box<AstExpression>,
        right: Box<AstExpression>,
    },
    ColumnReference {
        column_reference: AstColumnReference,
    },
    Literal {
        literal: AstLiteral,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AstAssignment {
    column: AstColumnReference,
    value: AstLiteral,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AstColumnDefinition {
    name: String,
    column_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AstColumnReference {
    name: String,
    table: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AstLiteral {
    value: String,
    literal_type: String,
}

//#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
//pub enum Plan {
//    Aggregate {
//        table: Box<Plan>,
//        aggregates: Vec<Plan>,
//        groups: Vec<Plan>,
//    },
//    BeginTransaction,
//    ColumnReference {
//        column: Column,
//    },
//    CommitTransaction,
//    Comparative {
//        name: String,
//        left: Box<Plan>,
//        right: Box<Plan>,
//    },
//    Connective {
//        name: String,
//        terms: Vec<Plan>,
//    },
//    CreateTable {
//        table: Table,
//    },
//    Delete {
//        from_table: Table,
//        filter: Option<Box<Plan>>,
//    },
//    DropTable {
//        table: Table,
//    },
//    Function {
//        name: String,
//        arguments: Vec<Plan>,
//        output_type: String
//    },
//    Insert {
//        into_table: Table,
//        input: Box<Plan>,
//    },
//    Join {
//        l_table: Box<Plan>,
//        r_table: Box<Plan>,
//        filter: Option<Box<Plan>>,
//    },
//    Limit {
//        table: Box<Plan>,
//        limit: usize,
//    },
//    Literal {
//        value: String,
//        literal_type: String,
//    },
//    Project {
//        table: Box<Plan>,
//        projection: Vec<Plan>,
//    },
//    Row {
//        values: Vec<Plan>,
//    },
//    Select {
//        table: Box<Plan>,
//        filter: Box<Plan>,
//    },
//    TableReference {
//        table: Table,
//    },
//    Update {
//        table: Table,
//        columns: Vec<Column>,
//        assignments: Vec<Plan>,
//        filter: Option<Box<Plan>>,
//    },
//}

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
//    pub output_type: Vec<String>,
//    pub output_name: Vec<String>,
    pub output: Vec<Column>
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
//    Function {
//        name: String,
//        arguments: Vec<Expression>,
//        output_type: String,
//    },
    Literal {
        value: String,
        literal_type: String,
    },
    ColumnReference {
        table: usize,
        column: usize,
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
    pub variant: AggregateFunctionVariant,
    pub column: Column
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunctionVariant {
    Avg, Count, Max, Min, Sum
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub from_table: String,
}

impl Column {
    pub fn new(name: String, column_type: String, from_table: String) -> Self {
        Column {
            name,
            column_type,
            from_table,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Table {
            name,
            columns,
        }
    }
}

impl Hash for Table {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

impl Eq for Table {}

impl Borrow<str> for Table {
    fn borrow(&self) -> &str {
        self.name.borrow()
    }
}
