extern crate dashmap;

use std::borrow::Borrow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use hustle_types::data_type::DataType;

pub const CUSTOMER_FILENAME: &str = "customer.tbl";
pub const DDATE_FILENAME: &str = "date.tbl";
pub const LINEORDER_FILENAME: &str = "lineorder.tbl";
pub const PART_FILENAME: &str = "part.tbl";
pub const SUPPLIER_FILENAME: &str = "supplier.tbl";

pub const C_RELATION_NAME: &str = "customer";
pub const D_RELATION_NAME: &str = "ddate";
pub const LO_RELATION_NAME: &str = "lineorder";
pub const P_RELATION_NAME: &str = "part";
pub const S_RELATION_NAME: &str = "supplier";

const NUM_CUSTOMER_BASE: usize = 30000;
const NUM_LINEORDER_BASE: usize = 6000000;
pub const NUM_PART_BASE: usize = 200000;
const NUM_SUPPLIER_BASE: usize = 2000;

pub const NUM_CUSTOMER: usize = NUM_CUSTOMER_BASE * SCALE_FACTOR;
pub const NUM_DDATE: usize = 2556;
pub const NUM_LINEORDER: usize = NUM_LINEORDER_BASE * SCALE_FACTOR;
pub const NUM_SUPPLIER: usize = NUM_SUPPLIER_BASE * SCALE_FACTOR;
pub const SCALE_FACTOR: usize = 1;
pub const AGGREGATE_OPT_HASH_TABLE: bool = true;

pub fn get_relation_size(relation_name: &str) -> usize {
    if relation_name == C_RELATION_NAME {
        NUM_CUSTOMER
    } else if relation_name == D_RELATION_NAME {
        NUM_DDATE
    } else if relation_name == LO_RELATION_NAME {
        NUM_LINEORDER
    } else if relation_name == P_RELATION_NAME {
        let part_factor = (SCALE_FACTOR as f64).log2() as usize;
        NUM_PART_BASE * (1 + part_factor)
    } else if relation_name == S_RELATION_NAME {
        NUM_SUPPLIER
    } else {
        unreachable!()
    }
}

pub enum SSB_LINEORDER {
    LO_ORDERKEY,
    LO_LINENUMBER,
    LO_CUSTKEY,
    LO_PARTKEY,
    LO_SUPPKEY,
    LO_ORDERDATE,
    LO_ORDERPRIORITY,
    LO_SHIPPRIORITY,
    LO_QUANTITY,
    LO_EXTENDEDPRICE,
    LO_ORDTOTALPRICE,
    LO_DISCOUNT,
    LO_REVENUE,
    LO_SUPPLYCOST,
    LO_TAX,
    LO_COMMITDATE,
    LO_SHIPMODE,
}

pub enum SSB_PART {
    P_PARTKEY,
    P_NAME,
    P_MFGR,
    P_CATEGORY,
    P_BRAND1,
    P_COLOR,
    P_TYPE,
    P_SIZE,
    P_CONTAINER,
}

pub enum SSB_SUPPLIER {
    S_SUPPKEY,
    S_NAME,
    S_ADDRESS,
    S_CITY,
    S_NATION,
    S_REGION,
    S_PHONE,
}

pub enum SSB_CUSTOMER {
    C_CUSTKEY,
    C_NAME,
    C_ADDRESS,
    C_CITY,
    C_NATION,
    C_REGION,
    C_PHONE,
    C_MKTSEGMENT,
}

pub enum SSB_DDATE {
    D_DATEKEY,
    D_DATE,
    D_DAYOFWEEK,
    D_MONTH,
    D_YEAR,
    D_YEARMONTHNUM,
    D_YEARMONTH,
    D_DAYNUMINWEEK,
    D_DAYNUMINMONTH,
    D_DAYNUMINYEAR,
    D_MONTHNUMINYEAR,
    D_WEEKNUMINYEAR,
    D_SELLINGSEASON,
    D_LASTDAYINWEEKFL,
    D_LASTDAYINMONTHFL,
    D_HOLIDAYFL,
    D_WEEKDAYFL,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    CloseConnection {
        connection_id: u64,
    },
    ExecuteSql {
        sql: String,
    },
    ParseSql {
        sql: String,
        connection_id: u64,
    },
    ResolveAst {
        ast: String,
        connection_id: u64,
    },
    OptimizePlan {
        plan: Plan,
        connection_id: u64,
    },
    TransactPlan {
        plan: Plan,
        connection_id: u64,
    },
    WorkOrder {
        query_id: usize,
        op_index: usize,
        work_order: usize, /* *mut WorkOrder */
        is_normal_work_order: bool,
    },
    WorkOrderCompletion {
        query_id: usize,
        op_index: usize,
        is_normal_work_order: bool,
        worker_id: usize,
    },
    ExecutePlan {
        plan: Plan,
        statement_id: u64,
        connection_id: u64,
    },
    CompletePlan {
        statement_id: u64,
        connection_id: u64,
    },
    Schema {
        schema: Vec<(String, DataType)>,
        connection_id: u64,
    },
    ReturnRow {
        row: Vec<Vec<u8>>,
        connection_id: u64,
    },
    Success {
        connection_id: u64,
    },
    Failure {
        reason: String,
        connection_id: u64,
    },
}

impl Message {
    pub fn send<W>(&self, buf: &mut W) -> Result<(), String>
    where
        W: WriteBytesExt,
    {
        let payload = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        buf.write_u32::<BigEndian>(payload.len() as u32)
            .map_err(|e| e.to_string())?;
        buf.write_all(&payload).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn receive<R>(buf: &mut R) -> Result<Self, String>
    where
        R: ReadBytesExt,
    {
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
        projection: Vec<AstColumnReference>,
    },
    Update {
        table: String,
        assignments: Vec<AstAssignment>,
    },
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Plan {
    Aggregate {
        table: Box<Plan>,
        aggregates: Vec<Plan>,
        groups: Vec<Plan>,
    },
    BeginTransaction,
    ColumnReference {
        column: Column,
    },
    CommitTransaction,
    Comparative {
        name: String,
        left: Box<Plan>,
        right: Box<Plan>,
    },
    Connective {
        name: String,
        terms: Vec<Plan>,
    },
    CreateTable {
        table: Table,
    },
    Delete {
        from_table: Table,
        filter: Option<Box<Plan>>,
    },
    DropTable {
        table: Table,
    },
    Function {
        name: String,
        arguments: Vec<Plan>,
        output_type: String,
    },
    Insert {
        into_table: Table,
        input: Box<Plan>,
    },
    Join {
        l_table: Box<Plan>,
        r_table: Box<Plan>,
        filter: Option<Box<Plan>>,
    },
    Limit {
        table: Box<Plan>,
        limit: usize,
    },
    Literal {
        value: String,
        literal_type: String,
    },
    Project {
        table: Box<Plan>,
        projection: Vec<Plan>,
    },
    Row {
        values: Vec<Plan>,
    },
    Select {
        table: Box<Plan>,
        filter: Box<Plan>,
    },
    TableReference {
        table: Table,
    },
    Update {
        table: Table,
        columns: Vec<Column>,
        assignments: Vec<Plan>,
        filter: Option<Box<Plan>>,
    },
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum ColumnType {
    I32,
    I64,
    Char(i32),
    VarChar(i32),
}

pub fn get_column_len(column_type: &ColumnType) -> usize {
    match column_type {
        ColumnType::I32 => 4,
        ColumnType::I64 => 8,
        ColumnType::Char(i) | ColumnType::VarChar(i) => *i as usize,
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum ColumnAnnotation {
    PrimaryKey,
    ForeignKey,
    DerivedFromPrimaryKey(i32),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub column__type: ColumnType,
    id: Option<usize>,
    pub table: String,
    pub annotation: Option<ColumnAnnotation>,
    pub alias: Option<String>,
}

impl Column {
    pub fn new(
        name: &str,
        column_type: &str,
        table: &str,
        annotation: Option<ColumnAnnotation>,
    ) -> Self {
        Column {
            name: name.to_string(),
            column_type: column_type.to_string(),
            column__type: ColumnType::I32,
            id: None,
            table: table.to_string(),
            annotation,
            alias: None,
        }
    }

    pub fn new2(
        name: &str,
        column__type: ColumnType,
        id: usize,
        table: &str,
        annotation: Option<ColumnAnnotation>,
    ) -> Self {
        Column {
            name: name.to_string(),
            column_type: "".to_string(),
            column__type,
            id: Some(id),
            table: table.to_string(),
            annotation,
            alias: None,
        }
    }

    pub fn id(&self) -> usize {
        self.id.unwrap()
    }
}

pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table_name: &str, table: Table) {
        self.tables.insert(table_name.to_string(), table);
    }

    pub fn find_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    column_map: HashMap<String, usize>,
}

impl Table {
    pub fn new(name: &str, columns: Vec<Column>) -> Self {
        let mut column_map = HashMap::new();
        for i in 0..columns.len() {
            column_map.insert(columns[i].name.clone(), i);
        }

        Table {
            name: name.to_string(),
            columns,
            column_map,
        }
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<&Column> {
        if let Some(i) = self.column_map.get(name) {
            Some(&self.columns[*i])
        } else {
            None
        }
    }

    pub fn get_column_by_id(&self, i: usize) -> Option<&Column> {
        if i < self.columns.len() {
            Some(&self.columns[i])
        } else {
            None
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOperation {
    Negate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOperation {
    Add,
    Substract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Literal {
    Int32(i32),
    Int32Reverse(Reverse<i32>),
    Int64(i64),
    Int64Reverse(Reverse<i64>),
    Char(String),
    CharReverse(Reverse<String>),
}

pub fn get_flip_literal(literal: &Literal) -> Literal {
    match literal {
        Literal::Int32(i) => Literal::Int32Reverse(Reverse(*i)),
        Literal::Int32Reverse(Reverse(i)) => Literal::Int32(*i),
        Literal::Int64(i) => Literal::Int64Reverse(Reverse(*i)),
        Literal::Int64Reverse(Reverse(i)) => Literal::Int64(*i),
        Literal::Char(s) => Literal::CharReverse(Reverse(s.clone())),
        Literal::CharReverse(Reverse(s)) => Literal::Char(s.clone()),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Scalar {
    ScalarLiteral(Literal),
    ScalarAttribute(Column),
    UnaryExpression {
        operation: UnaryOperation,
        operand: Box<Scalar>,
    },
    BinaryExpression {
        operation: BinaryOperation,
        left: Box<Scalar>,
        right: Box<Scalar>,
    },
}

pub fn get_referenced_attribute(scalar: &Scalar) -> HashSet<Column> {
    match scalar {
        Scalar::ScalarLiteral(_) => HashSet::new(),
        Scalar::ScalarAttribute(c) => [c.clone()].iter().cloned().collect(),
        Scalar::UnaryExpression {
            operation: _,
            operand,
        } => get_referenced_attribute(operand),
        Scalar::BinaryExpression {
            operation: _,
            left,
            right,
        } => {
            let mut columns = get_referenced_attribute(left);
            for column in get_referenced_attribute(right) {
                columns.insert(column);
            }

            columns
        }
    }
}

pub fn replaces_new_scalar(
    scalar: &mut Scalar,
    scalar_attributes: &Vec<Column>,
    scalar_new: &Vec<Box<Scalar>>,
) {
    if let Scalar::ScalarAttribute(c) = scalar {
        if c == &scalar_attributes[0] {}
        for i in 0..scalar_attributes.len() {
            let scalar_attribute = &scalar_attributes[i];
            if c == scalar_attribute {
                *scalar = *scalar_new[i].clone();
                break;
            }
        }
    }
}

fn replaces_new_scalar_helper(
    scalar: &mut Box<Scalar>,
    scalar_attributes: &Vec<Column>,
    scalar_new: &Vec<Box<Scalar>>,
) {
    if let Scalar::ScalarAttribute(c) = &**scalar {
        for i in 0..scalar_attributes.len() {
            let scalar_attribute = &scalar_attributes[i];
            if c == scalar_attribute {
                scalar.clone_from(&scalar_new[i]);
                break;
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Predicate {
    True,
    False,
    Between {
        operand: Box<Scalar>,
        begin: Box<Scalar>,
        end: Box<Scalar>,
    },
    Comparison {
        comparison: Comparison,
        left: Box<Scalar>,
        right: Box<Scalar>,
    },
    Negation {
        operand: Box<Predicate>,
    },
    Conjunction {
        static_operand_list: Vec<Predicate>,
        dynamic_operand_list: Vec<Predicate>,
    },
    Disjunction {
        static_operand_list: Vec<Predicate>,
        dynamic_operand_list: Vec<Predicate>,
    },
}

pub fn get_referenced_attributes(predicate: &Predicate) -> HashSet<Column> {
    match predicate {
        Predicate::True | Predicate::False => HashSet::new(),
        Predicate::Between {
            operand,
            begin: _,
            end: _,
        } => get_referenced_attribute(operand),
        Predicate::Comparison {
            comparison: _,
            left,
            right,
        } => {
            let mut columns = get_referenced_attribute(left);
            for column in get_referenced_attribute(right) {
                columns.insert(column);
            }

            columns
        }
        Predicate::Negation { operand } => get_referenced_attributes(operand),
        Predicate::Conjunction {
            static_operand_list: _,
            dynamic_operand_list,
        }
        | Predicate::Disjunction {
            static_operand_list: _,
            dynamic_operand_list,
        } => {
            let mut columns = HashSet::new();
            for predicate in dynamic_operand_list {
                for column in get_referenced_attributes(predicate) {
                    columns.insert(column);
                }
            }

            columns
        }
    }
}

pub fn predicate_replaces_new_scalar(
    predicate: &mut Predicate,
    scalar_attributes: &Vec<Column>,
    scalar_new: &Vec<Box<Scalar>>,
) {
    match predicate {
        Predicate::Comparison {
            comparison: _,
            left,
            right: _,
        } => {
            replaces_new_scalar_helper(left, scalar_attributes, scalar_new);
            // FIXME: check right
        }
        Predicate::Between {
            operand,
            begin: _,
            end: _,
        } => {
            replaces_new_scalar_helper(operand, scalar_attributes, scalar_new);
        }
        Predicate::Conjunction {
            static_operand_list: _,
            dynamic_operand_list,
        }
        | Predicate::Disjunction {
            static_operand_list: _,
            dynamic_operand_list,
        } => {
            for dynamic_predicate in dynamic_operand_list {
                predicate_replaces_new_scalar(dynamic_predicate, scalar_attributes, scalar_new);
            }
        }
        _ => unimplemented!(),
    }
}

pub fn merge_conjunction_predicates(predicates: Vec<&Predicate>) -> Predicate {
    let mut merged_static = vec![];
    let mut merged_dynamic = vec![];
    for predicate in predicates {
        match predicate {
            Predicate::True => (),
            Predicate::False => {
                merged_static.clear();
                merged_static.push(predicate.clone());
            }
            Predicate::Comparison { .. }
            | Predicate::Negation { .. }
            | Predicate::Between { .. } => {
                merged_dynamic.push(predicate.clone());
            }
            Predicate::Conjunction {
                static_operand_list,
                dynamic_operand_list,
            } => {
                for static_operand in static_operand_list {
                    merged_static.push(static_operand.clone());
                }

                for dynamic_operand in dynamic_operand_list {
                    merged_dynamic.push(dynamic_operand.clone());
                }
            }
            Predicate::Disjunction { .. } => unimplemented!(),
        }
    }

    Predicate::Conjunction {
        static_operand_list: merged_static,
        dynamic_operand_list: merged_dynamic,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateFunction {
    Sum,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregateContext {
    pub function: AggregateFunction,
    pub argument: Scalar,
    pub is_distinct: bool,
}

impl AggregateContext {
    pub fn new(function: AggregateFunction, argument: Scalar, is_distinct: bool) -> Self {
        AggregateContext {
            function,
            argument,
            is_distinct,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinContext {
    pub table_name: String,
    pub join_column_id: usize,
    pub max_count: usize,
    pub predicate: Option<Predicate>,
    pub payload_columns: Vec<Column>,
}

impl JoinContext {
    pub fn new(
        table_name: &str,
        join_column_id: usize,
        max_count: usize,
        predicate: Option<Predicate>,
        payload_columns: Vec<Column>,
    ) -> Self {
        JoinContext {
            table_name: table_name.to_owned(),
            join_column_id,
            max_count,
            predicate,
            payload_columns,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataSource {
    BaseRelation(Scalar),
    JoinHashTable(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EvaluationOrder {
    BaseRelation,
    ExistenceJoin(usize),
    GroupByIndex(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OutputSource {
    Key(usize),
    // PayloadIndex(usize /* dimension */),
    Payload(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PhysicalPlan {
    Aggregate {
        table: Box<PhysicalPlan>,
        aggregates: Vec<AggregateContext>,
        groups: Vec<Scalar>,
        filter: Option<Predicate>,
        output_schema: Vec<OutputSource>,
    },
    BinaryJoin {
        l_table: Box<PhysicalPlan>,
        r_table: Box<PhysicalPlan>,
        l_join_attributes: Vec<Column>,
        r_join_attributes: Vec<Column>,
        output_schema: Vec<Scalar>,
    },
    CreateTable {
        table: Table,
    },
    Delete {
        from_table: Table,
        filter: Option<Predicate>,
    },
    DropTable {
        table: Table,
    },
    StarJoin {
        fact_table: Box<PhysicalPlan>,
        fact_table_filter: Option<Predicate>,
        fact_table_join_column_ids: Vec<usize>,
        dim_tables: Vec<JoinContext>,
        output_schema: Vec<Scalar>,
    },
    StarJoinAggregate {
        fact_table: Box<PhysicalPlan>,
        fact_table_filter: Option<Predicate>,
        fact_table_join_column_ids: Vec<usize>,
        dim_tables: Vec<JoinContext>,
        aggregate_context: AggregateContext,
        groups: Vec<DataSource>,
        evaluation_order: Vec<EvaluationOrder>,
        groups_from_fact_table: Vec<usize>,
        output_schema: Vec<OutputSource>,
    },
    Selection {
        table: Box<PhysicalPlan>,
        filter: Option<Predicate>,
        project_expressions: Vec<Scalar>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        sort_attributes: Vec<(OutputSource, bool /*desc*/)>,
        limit: Option<usize>,
        output_schema: Vec<OutputSource>,
    },
    TableReference {
        table: Table,
        alias: Option<String>,
        attribute_list: Vec<Column>,
    },
    TopLevelPlan {
        plan: Box<PhysicalPlan>,
        shared_subplans: Vec<PhysicalPlan>,
    },
    TriangleCount {},
    Update {
        table: Table,
        columns: Vec<Column>,
        assignments: Vec<Scalar>,
        filter: Option<Predicate>,
    },
}

#[derive(Debug, Clone)]
pub enum JoinHashTable {
    Existence(Arc<Vec<Mutex<bool>>>),
    PrimaryIntKeyIntPayload(Arc<Vec<Mutex<i32>>>),
    PrimaryIntKeyStringPayload(Arc<Vec<Mutex<String>>>),
    // GenericHashTable(Arc<Mutex<HashMap<Vec<Literal>, Vec<Literal>>>>),
}

#[derive(Debug, Clone)]
pub struct JoinHashTables {
    pub join_hash_tables: Vec<JoinHashTable>,
}

impl JoinHashTables {
    pub fn new(join_hash_tables: Vec<JoinHashTable>) -> Self {
        JoinHashTables { join_hash_tables }
    }
}

#[derive(Clone)]
pub enum AggregateState {
    SingleStates(Arc<Vec<std::sync::atomic::AtomicI64>>),
    GroupByStatesDashMap(Arc<DashMap<Vec<Literal>, i64>>),
    // GroupByStatesOpt(i32, Arc<DashMap<Vec<&str>, Vec<i64>>>),
}

#[derive(Clone)]
pub enum SortInput {
    Aggregate(AggregateState),
    // Relation(...)
}

#[derive(Clone)]
pub enum QueryResult {
    Aggregate(AggregateState),
    Sort(Arc<Mutex<BTreeMap<Vec<Literal>, Vec<Literal>>>>),
}
