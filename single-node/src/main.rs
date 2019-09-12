extern crate core_affinity;

use std::sync::mpsc;
use std::sync::Arc;

use hustle_common::{
    get_column_len, AggregateContext, AggregateFunction, BinaryOperation, Column, ColumnAnnotation,
    ColumnType, Comparison, DataSource, Database, JoinContext, Literal, Message, OutputSource,
    PhysicalPlan, Predicate, Scalar, Table, CUSTOMER_FILENAME, C_RELATION_NAME, DDATE_FILENAME,
    D_RELATION_NAME, LINEORDER_FILENAME, LO_RELATION_NAME, NUM_CUSTOMER, NUM_DDATE, NUM_LINEORDER,
    NUM_PART_BASE, NUM_SUPPLIER, PART_FILENAME, P_RELATION_NAME, SCALE_FACTOR, SSB_CUSTOMER,
    SSB_DDATE, SSB_LINEORDER, SSB_PART, SSB_SUPPLIER, SUPPLIER_FILENAME, S_RELATION_NAME,
};
use hustle_storage::StorageManager;

const DATA_DIRECTORY: &str = "/mnt/disk/data/ssb-sf1";

const PLACEHOLDER: char = ' ';

fn main() {
    let core_ids = core_affinity::get_core_ids().unwrap();
    let num_workers = core_ids.len();
    // core_affinity::set_for_current(*core_ids.last().unwrap());

    let mut database = Database::new();
    add_ssb_schemas(&mut database);

    let sm = Arc::new(hustle_storage::StorageManager::new());
    let re = sm.relational_engine();

    if !re.exists(LO_RELATION_NAME) {
        load_lo(&*sm, &database);
    }

    if !re.exists(P_RELATION_NAME) {
        load_p(&*sm, &database);
    }

    if !re.exists(S_RELATION_NAME) {
        load_s(&*sm, &database);
    }

    if !re.exists(C_RELATION_NAME) {
        load_c(&*sm, &database);
    }

    if !re.exists(D_RELATION_NAME) {
        load_d(&*sm, &database);
    }

    for query_id in 0..5 {
        let mut plan =
        /*
            q1(&database);
            q2(&database);
            q3(&database);
            q4(&database);
            q5(&database);
            q6(&database);
         */
            q6opt(&database);
        /*
           q7(&database);
           q8(&database);
           q9(&database);
           q10(&database);
           q11(&database);
           q12(&database);
           q13(&database);
        */

        // println!("{:#?}", plan);

        let physical_generator = hustle_optimizer::PhysicalGenerator::new();
        physical_generator.optimize_plan(&database, &mut plan, query_id == 0);

        let mut query_plan = hustle_operators::QueryPlan::new(query_id);
        let mut query_plan_dag = hustle_operators::QueryPlanDag::new();

        let mut execution_generator = hustle_optimizer::ExecutionGenerator::new(Arc::clone(&sm));
        execution_generator.generate_plan(&plan, &mut query_plan, &mut query_plan_dag);

        // println!("{:#?}", query_plan);
        // println!("{:#?}", query_plan_dag);

        let mut execution_state =
            hustle_scheduling::ExecutionState::new(query_plan, &query_plan_dag);

        let (scheduler_tx, scheduler_rx) = mpsc::channel();
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut core_id_index = 0;
        for id in 0..num_workers {
            let (worker_tx, worker_rx) = mpsc::channel();
            worker_txs.push(worker_tx);
            let mut worker = hustle_scheduling::worker::Worker::new(
                id,
                worker_rx,
                scheduler_tx.clone(),
                Arc::clone(&sm),
            );
            let core_id = core_ids[core_id_index];
            core_id_index += 1;
            std::thread::spawn(move || {
                core_affinity::set_for_current(core_id);
                worker.run();
            });
        }

        let start_time = std::time::Instant::now();
        let mut id = 0usize;
        let work_orders = execution_state.get_next_work_orders();
        for (work_order, op_index) in work_orders {
            let request = construct_work_order_message(query_id, op_index, work_order);
            worker_txs[id].send(request.serialize().unwrap()).unwrap();
            id += 1;
            if id == num_workers {
                id = 0;
            }
        }

        loop {
            let response = Message::deserialize(&scheduler_rx.recv().unwrap()).unwrap();
            match response {
                Message::WorkOrderCompletion {
                    query_id,
                    op_index,
                    is_normal_work_order: _,
                    worker_id,
                } => {
                    execution_state.mark_work_order_completion(op_index);

                    if execution_state.check_normal_execution_completion(op_index) {
                        execution_state.mark_operator_completion(op_index, &query_plan_dag);

                        let mut id = worker_id;
                        let work_orders = execution_state.get_next_work_orders();
                        for (work_order, op_index) in work_orders {
                            let request =
                                construct_work_order_message(query_id, op_index, work_order);
                            worker_txs[id].send(request.serialize().unwrap()).unwrap();
                            id += 1;
                            if id == num_workers {
                                id = 0;
                            }
                        }
                    }

                    if execution_state.done() {
                        let duration = start_time.elapsed();
                        eprintln!("rs-{} {:?} ns", query_id, duration.as_nanos());
                        eprintln!("rs-{} {:?} ms", query_id, duration.as_millis());
                        eprintln!("rs-{} {:?} s", query_id, duration.as_secs());

                        execution_state.display();
                        break;
                    }
                }
                _ => println!("Unexpected message {:?}", response),
            };
        }
    }
}

fn construct_work_order_message(
    query_id: usize,
    op_index: usize,
    work_order: Box<dyn hustle_operators::WorkOrder>,
) -> Message {
    let work_order_raw = Box::into_raw(work_order);
    let boxed_work_order = Box::new(work_order_raw);
    let work_order = Box::into_raw(boxed_work_order) as *mut usize as usize;
    Message::WorkOrder {
        query_id,
        op_index,
        work_order,
        is_normal_work_order: true,
    }
}

#[allow(dead_code)]
fn q1(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_quantity = lo
        .get_column_by_id(SSB_LINEORDER::LO_QUANTITY as usize)
        .unwrap();
    let lo_extendedprice = lo
        .get_column_by_id(SSB_LINEORDER::LO_EXTENDEDPRICE as usize)
        .unwrap();
    let lo_discount = lo
        .get_column_by_id(SSB_LINEORDER::LO_DISCOUNT as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![],
    });

    let mut dynamic_operand_list = vec![];
    {
        let operand = Box::new(Scalar::ScalarAttribute(lo_discount.clone()));
        let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(1)));
        let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(3)));
        dynamic_operand_list.push(Predicate::Between {
            operand,
            begin,
            end,
        });
    }

    {
        let left = Box::new(Scalar::ScalarAttribute(lo_quantity.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(25)));
        dynamic_operand_list.push(Predicate::Comparison {
            comparison: Comparison::Less,
            left,
            right,
        });
    }

    let fact_table_filter = Some(Predicate::Conjunction {
        static_operand_list: vec![],
        dynamic_operand_list,
    });

    let mut dim_tables = vec![];
    {
        let d = database.find_table(D_RELATION_NAME).unwrap();
        let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
        let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1993)));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        dim_tables.push(JoinContext::new(
            D_RELATION_NAME,
            SSB_DDATE::D_DATEKEY as usize,
            19981230 + 1,
            predicate,
            vec![],
        ));
    }

    let output_schema = vec![
        Scalar::ScalarAttribute(lo_extendedprice.clone()),
        Scalar::ScalarAttribute(lo_discount.clone()),
    ];
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter,
        fact_table_join_column_ids: vec![SSB_LINEORDER::LO_ORDERDATE as usize],
        dim_tables,
        output_schema,
    });

    let mut aggregates = vec![];
    {
        let left = Box::new(Scalar::ScalarAttribute(lo_extendedprice.clone()));
        let right = Box::new(Scalar::ScalarAttribute(lo_discount.clone()));
        aggregates.push(AggregateContext::new(
            AggregateFunction::Sum,
            Scalar::BinaryExpression {
                operation: BinaryOperation::Multiply,
                left,
                right,
            },
            false,
        ));
    }

    let output_schema = vec![OutputSource::Payload(0)];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates,
        groups: vec![],
        filter: None,
        output_schema,
    });

    Box::new(PhysicalPlan::TopLevelPlan {
        plan: aggregate,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q2(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_quantity = lo
        .get_column_by_id(SSB_LINEORDER::LO_QUANTITY as usize)
        .unwrap();
    let lo_extendedprice = lo
        .get_column_by_id(SSB_LINEORDER::LO_EXTENDEDPRICE as usize)
        .unwrap();
    let lo_discount = lo
        .get_column_by_id(SSB_LINEORDER::LO_DISCOUNT as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![],
    });

    let mut dynamic_operand_list = vec![];
    {
        let operand = Box::new(Scalar::ScalarAttribute(lo_quantity.clone()));
        let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(26)));
        let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(35)));
        dynamic_operand_list.push(Predicate::Between {
            operand,
            begin,
            end,
        });
    }

    {
        let operand = Box::new(Scalar::ScalarAttribute(lo_discount.clone()));
        let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(4)));
        let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(6)));
        dynamic_operand_list.push(Predicate::Between {
            operand,
            begin,
            end,
        });
    }

    let fact_table_filter = Some(Predicate::Conjunction {
        static_operand_list: vec![],
        dynamic_operand_list,
    });

    let mut dim_tables = vec![];
    {
        let d = database.find_table(D_RELATION_NAME).unwrap();
        let d_yearmontnum = d
            .get_column_by_id(SSB_DDATE::D_YEARMONTHNUM as usize)
            .unwrap();
        let left = Box::new(Scalar::ScalarAttribute(d_yearmontnum.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(199401)));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        dim_tables.push(JoinContext::new(
            D_RELATION_NAME,
            SSB_DDATE::D_DATEKEY as usize,
            19981230 + 1,
            predicate,
            vec![],
        ));
    }

    let output_schema = vec![
        Scalar::ScalarAttribute(lo_extendedprice.clone()),
        Scalar::ScalarAttribute(lo_discount.clone()),
    ];
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter,
        fact_table_join_column_ids: vec![SSB_LINEORDER::LO_ORDERDATE as usize],
        dim_tables,
        output_schema,
    });

    let mut aggregates = vec![];
    {
        let left = Box::new(Scalar::ScalarAttribute(lo_extendedprice.clone()));
        let right = Box::new(Scalar::ScalarAttribute(lo_discount.clone()));
        aggregates.push(AggregateContext::new(
            AggregateFunction::Sum,
            Scalar::BinaryExpression {
                operation: BinaryOperation::Multiply,
                left,
                right,
            },
            false,
        ));
    }

    let output_schema = vec![OutputSource::Payload(0)];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates,
        groups: vec![],
        filter: None,
        output_schema,
    });

    Box::new(PhysicalPlan::TopLevelPlan {
        plan: aggregate,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q3(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_quantity = lo
        .get_column_by_id(SSB_LINEORDER::LO_QUANTITY as usize)
        .unwrap();
    let lo_extendedprice = lo
        .get_column_by_id(SSB_LINEORDER::LO_EXTENDEDPRICE as usize)
        .unwrap();
    let lo_discount = lo
        .get_column_by_id(SSB_LINEORDER::LO_DISCOUNT as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![],
    });

    let mut dynamic_operand_list = vec![];
    {
        let operand = Box::new(Scalar::ScalarAttribute(lo_quantity.clone()));
        let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(36)));
        let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(40)));
        dynamic_operand_list.push(Predicate::Between {
            operand,
            begin,
            end,
        });
    }

    {
        let operand = Box::new(Scalar::ScalarAttribute(lo_discount.clone()));
        let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(5)));
        let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(7)));
        dynamic_operand_list.push(Predicate::Between {
            operand,
            begin,
            end,
        });
    }

    let fact_table_filter = Some(Predicate::Conjunction {
        static_operand_list: vec![],
        dynamic_operand_list,
    });

    let mut dim_tables = vec![];
    {
        let d = database.find_table(D_RELATION_NAME).unwrap();

        let mut dynamic_operand_list = vec![];
        {
            let d_weeknuminyear = d
                .get_column_by_id(SSB_DDATE::D_WEEKNUMINYEAR as usize)
                .unwrap();
            let left = Box::new(Scalar::ScalarAttribute(d_weeknuminyear.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(6)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }
        {
            let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
            let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1994)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Conjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });
        /*
        let d_datekey = d.get_column_by_id(SSB_DDATE::D_DATEKEY as usize).unwrap();
        let operand = Box::new(Scalar::ScalarAttribute(d_datekey.clone()));
        let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(19940204)));
        let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(19940210)));
        let predicate = Some(Predicate::Between {
            operand,
            begin,
            end,
        });
        */

        dim_tables.push(JoinContext::new(
            D_RELATION_NAME,
            SSB_DDATE::D_DATEKEY as usize,
            19981230 + 1,
            predicate,
            vec![],
        ));
    }

    let output_schema = vec![
        Scalar::ScalarAttribute(lo_extendedprice.clone()),
        Scalar::ScalarAttribute(lo_discount.clone()),
    ];
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter,
        fact_table_join_column_ids: vec![SSB_LINEORDER::LO_ORDERDATE as usize],
        dim_tables,
        output_schema,
    });

    let mut aggregates = vec![];
    {
        let left = Box::new(Scalar::ScalarAttribute(lo_extendedprice.clone()));
        let right = Box::new(Scalar::ScalarAttribute(lo_discount.clone()));
        aggregates.push(AggregateContext::new(
            AggregateFunction::Sum,
            Scalar::BinaryExpression {
                operation: BinaryOperation::Multiply,
                left,
                right,
            },
            false,
        ));
    }

    let output_schema = vec![OutputSource::Payload(0)];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates,
        groups: vec![],
        filter: None,
        output_schema,
    });

    Box::new(PhysicalPlan::TopLevelPlan {
        plan: aggregate,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q4(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    {
        let p = database.find_table(P_RELATION_NAME).unwrap();

        let part_factor = (SCALE_FACTOR as f64).log2() as usize;
        let max_count = NUM_PART_BASE * (1 + part_factor) + 1;

        let p_category = p.get_column_by_id(SSB_PART::P_CATEGORY as usize).unwrap();
        let left = Box::new(Scalar::ScalarAttribute(p_category.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char("MFGR#12".to_string())));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();
        let payload_columns = vec![p_brand1.clone()];
        dim_tables.push(JoinContext::new(
            P_RELATION_NAME,
            SSB_PART::P_PARTKEY as usize,
            max_count,
            predicate,
            payload_columns,
        ));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_region = s.get_column_by_id(SSB_SUPPLIER::S_REGION as usize).unwrap();
        let s_region_len = get_column_len(&s_region.column__type);

        let mut str_literal = String::from("AMERICA");
        while str_literal.len() < s_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let operand = Box::new(Scalar::ScalarAttribute(s_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left: operand,
            right,
        });

        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![],
        ));
    }

    let d = database.find_table(D_RELATION_NAME).unwrap();
    let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
    dim_tables.push(JoinContext::new(
        D_RELATION_NAME,
        SSB_DDATE::D_DATEKEY as usize,
        19981230 + 1,
        None,
        vec![d_year.clone()],
    ));

    let p = database.find_table(P_RELATION_NAME).unwrap();
    let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();
    let output_schema = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(p_brand1.clone()),
        Scalar::ScalarAttribute(lo_revenue.clone()),
    ];
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_PARTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::ScalarAttribute(lo_revenue.clone()),
        false,
    );

    let groups = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(p_brand1.clone()),
    ];

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema,
    });

    let sort_attributes = vec![(OutputSource::Key(0), false), (OutputSource::Key(1), false)];
    let output_schema = vec![
        OutputSource::Payload(0),
        OutputSource::Key(0),
        OutputSource::Key(1),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q5(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    {
        let part_factor = (SCALE_FACTOR as f64).log2() as usize;
        let max_count = NUM_PART_BASE * (1 + part_factor) + 1;

        let p = database.find_table(P_RELATION_NAME).unwrap();
        let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();

        let operand = Box::new(Scalar::ScalarAttribute(p_brand1.clone()));
        let begin = Box::new(Scalar::ScalarLiteral(Literal::Char(
            "MFGR#2221".to_string(),
        )));
        let end = Box::new(Scalar::ScalarLiteral(Literal::Char(
            "MFGR#2228".to_string(),
        )));
        let predicate = Some(Predicate::Between {
            operand,
            begin,
            end,
        });

        dim_tables.push(JoinContext::new(
            P_RELATION_NAME,
            SSB_PART::P_PARTKEY as usize,
            max_count,
            predicate,
            vec![p_brand1.clone()],
        ));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_region = s.get_column_by_id(SSB_SUPPLIER::S_REGION as usize).unwrap();
        let s_region_len = get_column_len(&s_region.column__type);

        let mut str_literal = String::from("ASIA");
        while str_literal.len() < s_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let operand = Box::new(Scalar::ScalarAttribute(s_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left: operand,
            right,
        });

        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![],
        ));
    }

    let d = database.find_table(D_RELATION_NAME).unwrap();
    let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
    dim_tables.push(JoinContext::new(
        D_RELATION_NAME,
        SSB_DDATE::D_DATEKEY as usize,
        19981230 + 1,
        None,
        vec![d_year.clone()],
    ));

    let p = database.find_table(P_RELATION_NAME).unwrap();
    let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();
    let output_schema = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(p_brand1.clone()),
        Scalar::ScalarAttribute(lo_revenue.clone()),
    ];
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_PARTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::ScalarAttribute(lo_revenue.clone()),
        false,
    );

    let groups = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(p_brand1.clone()),
    ];

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema,
    });

    let sort_attributes = vec![(OutputSource::Key(0), false), (OutputSource::Key(1), false)];
    let output_schema = vec![
        OutputSource::Payload(0),
        OutputSource::Key(0),
        OutputSource::Key(1),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q6(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    {
        let part_factor = (SCALE_FACTOR as f64).log2() as usize;
        let max_count = NUM_PART_BASE * (1 + part_factor) + 1;

        let p = database.find_table(P_RELATION_NAME).unwrap();
        let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();

        let left = Box::new(Scalar::ScalarAttribute(p_brand1.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(
            "MFGR#2221".to_string(),
        )));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        dim_tables.push(JoinContext::new(
            P_RELATION_NAME,
            SSB_PART::P_PARTKEY as usize,
            max_count,
            predicate,
            vec![p_brand1.clone()],
        ));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_region = s.get_column_by_id(SSB_SUPPLIER::S_REGION as usize).unwrap();
        let s_region_len = get_column_len(&s_region.column__type);

        let mut str_literal = String::from("EUROPE");
        while str_literal.len() < s_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let operand = Box::new(Scalar::ScalarAttribute(s_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left: operand,
            right,
        });

        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![],
        ));
    }

    let d = database.find_table(D_RELATION_NAME).unwrap();
    let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
    dim_tables.push(JoinContext::new(
        D_RELATION_NAME,
        SSB_DDATE::D_DATEKEY as usize,
        19981230 + 1,
        None,
        vec![d_year.clone()],
    ));

    let p = database.find_table(P_RELATION_NAME).unwrap();
    let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();
    let output_schema = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(p_brand1.clone()),
        Scalar::ScalarAttribute(lo_revenue.clone()),
    ];
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_PARTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::ScalarAttribute(lo_revenue.clone()),
        false,
    );

    let groups = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(p_brand1.clone()),
    ];

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema,
    });

    let sort_attributes = vec![(OutputSource::Key(0), false), (OutputSource::Key(1), false)];
    let output_schema = vec![
        OutputSource::Payload(0),
        OutputSource::Key(0),
        OutputSource::Key(1),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q6opt(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    {
        let part_factor = (SCALE_FACTOR as f64).log2() as usize;
        let max_count = NUM_PART_BASE * (1 + part_factor) + 1;

        let p = database.find_table(P_RELATION_NAME).unwrap();
        let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();

        let left = Box::new(Scalar::ScalarAttribute(p_brand1.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(
            "MFGR#2221".to_string(),
        )));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        dim_tables.push(JoinContext::new(
            P_RELATION_NAME,
            SSB_PART::P_PARTKEY as usize,
            max_count,
            predicate,
            vec![],
        ));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_region = s.get_column_by_id(SSB_SUPPLIER::S_REGION as usize).unwrap();
        let s_region_len = get_column_len(&s_region.column__type);

        let mut str_literal = String::from("EUROPE");
        while str_literal.len() < s_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let operand = Box::new(Scalar::ScalarAttribute(s_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left: operand,
            right,
        });

        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![],
        ));
    }

    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::ScalarAttribute(lo_revenue.clone()),
        false,
    );

    let lo_orderdate = lo
        .get_column_by_id(SSB_LINEORDER::LO_ORDERDATE as usize)
        .unwrap();
    let left = Box::new(Scalar::ScalarAttribute(lo_orderdate.clone()));
    let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(10000)));

    let output_schema = vec![
        OutputSource::Payload(0),
        OutputSource::Key(0),
        OutputSource::Key(1),
    ];
    let plan = Box::new(PhysicalPlan::StarJoinAggregateSort {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_PARTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
        ],
        dim_tables,
        aggregate_context,
        group: DataSource::BaseRelation(Scalar::BinaryExpression {
            operation: BinaryOperation::Divide,
            left,
            right,
        }),
        constant_groups: vec![(1, Literal::Char("MFGR#2221".to_string()))],
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q7(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    let mut output_schema = vec![];
    let mut groups = vec![];
    {
        let c = database.find_table(C_RELATION_NAME).unwrap();
        let c_region = c.get_column_by_id(SSB_CUSTOMER::C_REGION as usize).unwrap();
        let c_region_len = get_column_len(&c_region.column__type);

        let mut str_literal = String::from("ASIA");
        while str_literal.len() < c_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(c_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let c_nation = c.get_column_by_id(SSB_CUSTOMER::C_NATION as usize).unwrap();
        dim_tables.push(JoinContext::new(
            C_RELATION_NAME,
            SSB_CUSTOMER::C_CUSTKEY as usize,
            NUM_CUSTOMER + 1,
            predicate,
            vec![c_nation.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(c_nation.clone()));
        groups.push(Scalar::ScalarAttribute(c_nation.clone()));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_region = s.get_column_by_id(SSB_SUPPLIER::S_REGION as usize).unwrap();
        let s_region_len = get_column_len(&s_region.column__type);

        let mut str_literal = String::from("ASIA");
        while str_literal.len() < s_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(s_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let s_nation = s.get_column_by_id(SSB_SUPPLIER::S_NATION as usize).unwrap();
        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![s_nation.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(s_nation.clone()));
        groups.push(Scalar::ScalarAttribute(s_nation.clone()));
    }

    {
        let d = database.find_table(D_RELATION_NAME).unwrap();
        let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();

        let mut dynamic_operand_list = vec![];
        {
            let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1992)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::GreaterEqual,
                left,
                right,
            });
        }
        {
            let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1997)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::LessEqual,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Conjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });
        /*
        let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1998)));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::NotEqual,
            left,
            right,
        });
        */

        dim_tables.push(JoinContext::new(
            D_RELATION_NAME,
            SSB_DDATE::D_DATEKEY as usize,
            19981230 + 1,
            predicate,
            vec![d_year.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(d_year.clone()));
        groups.push(Scalar::ScalarAttribute(d_year.clone()));
    }

    output_schema.push(Scalar::ScalarAttribute(lo_revenue.clone()));
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_CUSTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::ScalarAttribute(lo_revenue.clone()),
        false,
    );

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Key(2),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema,
    });

    let sort_attributes = vec![
        (OutputSource::Key(2), false),
        (OutputSource::Payload(0), true),
    ];
    let output_schema = vec![
        OutputSource::Payload(0),
        OutputSource::Payload(1),
        OutputSource::Key(0),
        OutputSource::Key(1),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q8(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    let mut output_schema = vec![];
    let mut groups = vec![];
    {
        let c = database.find_table(C_RELATION_NAME).unwrap();
        let c_nation = c.get_column_by_id(SSB_CUSTOMER::C_NATION as usize).unwrap();
        let c_nation_len = get_column_len(&c_nation.column__type);

        let mut str_literal = String::from("UNITED STATES");
        while str_literal.len() < c_nation_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(c_nation.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let c_city = c.get_column_by_id(SSB_CUSTOMER::C_CITY as usize).unwrap();
        dim_tables.push(JoinContext::new(
            C_RELATION_NAME,
            SSB_CUSTOMER::C_CUSTKEY as usize,
            NUM_CUSTOMER + 1,
            predicate,
            vec![c_city.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(c_city.clone()));
        groups.push(Scalar::ScalarAttribute(c_city.clone()));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_nation = s.get_column_by_id(SSB_SUPPLIER::S_NATION as usize).unwrap();
        let s_nation_len = get_column_len(&s_nation.column__type);

        let mut str_literal = String::from("UNITED STATES");
        while str_literal.len() < s_nation_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(s_nation.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let s_city = s.get_column_by_id(SSB_SUPPLIER::S_CITY as usize).unwrap();
        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![s_city.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(s_city.clone()));
        groups.push(Scalar::ScalarAttribute(s_city.clone()));
    }

    {
        let d = database.find_table(D_RELATION_NAME).unwrap();
        let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();

        let mut dynamic_operand_list = vec![];
        {
            let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1992)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::GreaterEqual,
                left,
                right,
            });
        }
        {
            let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1997)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::LessEqual,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Conjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });
        /*
        let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1998)));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::NotEqual,
            left,
            right,
        });
        */

        dim_tables.push(JoinContext::new(
            D_RELATION_NAME,
            SSB_DDATE::D_DATEKEY as usize,
            19981230 + 1,
            predicate,
            vec![d_year.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(d_year.clone()));
        groups.push(Scalar::ScalarAttribute(d_year.clone()));
    }

    output_schema.push(Scalar::ScalarAttribute(lo_revenue.clone()));
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_CUSTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::ScalarAttribute(lo_revenue.clone()),
        false,
    );

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Key(2),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema,
    });

    let sort_attributes = vec![
        (OutputSource::Key(2), false),
        (OutputSource::Payload(0), true),
    ];
    let output_schema = vec![
        OutputSource::Payload(0),
        OutputSource::Payload(1),
        OutputSource::Key(0),
        OutputSource::Key(1),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q9(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    let mut output_schema = vec![];
    let mut groups = vec![];
    {
        let c = database.find_table(C_RELATION_NAME).unwrap();
        let c_city = c.get_column_by_id(SSB_CUSTOMER::C_CITY as usize).unwrap();
        let c_city_len = get_column_len(&c_city.column__type);

        let mut dynamic_operand_list = vec![];
        {
            let mut str_literal = String::from("UNITED KI1");
            while str_literal.len() < c_city_len {
                str_literal.push(PLACEHOLDER);
            }
            let left = Box::new(Scalar::ScalarAttribute(c_city.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        {
            let mut str_literal = String::from("UNITED KI5");
            while str_literal.len() < c_city_len {
                str_literal.push(PLACEHOLDER);
            }
            let left = Box::new(Scalar::ScalarAttribute(c_city.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Disjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });

        dim_tables.push(JoinContext::new(
            C_RELATION_NAME,
            SSB_CUSTOMER::C_CUSTKEY as usize,
            NUM_CUSTOMER + 1,
            predicate,
            vec![c_city.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(c_city.clone()));
        groups.push(Scalar::ScalarAttribute(c_city.clone()));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_city = s.get_column_by_id(SSB_SUPPLIER::S_CITY as usize).unwrap();
        let s_city_len = get_column_len(&s_city.column__type);

        let mut dynamic_operand_list = vec![];
        {
            let mut str_literal = String::from("UNITED KI1");
            while str_literal.len() < s_city_len {
                str_literal.push(PLACEHOLDER);
            }
            let left = Box::new(Scalar::ScalarAttribute(s_city.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        {
            let mut str_literal = String::from("UNITED KI5");
            while str_literal.len() < s_city_len {
                str_literal.push(PLACEHOLDER);
            }
            let left = Box::new(Scalar::ScalarAttribute(s_city.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Disjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });

        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![s_city.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(s_city.clone()));
        groups.push(Scalar::ScalarAttribute(s_city.clone()));
    }

    {
        let d = database.find_table(D_RELATION_NAME).unwrap();
        let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();

        let mut dynamic_operand_list = vec![];
        {
            let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1992)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::GreaterEqual,
                left,
                right,
            });
        }
        {
            let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1997)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::LessEqual,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Conjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });
        /*
        let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1998)));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::NotEqual,
            left,
            right,
        });
        */

        dim_tables.push(JoinContext::new(
            D_RELATION_NAME,
            SSB_DDATE::D_DATEKEY as usize,
            19981230 + 1,
            predicate,
            vec![d_year.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(d_year.clone()));
        groups.push(Scalar::ScalarAttribute(d_year.clone()));
    }

    output_schema.push(Scalar::ScalarAttribute(lo_revenue.clone()));
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_CUSTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::ScalarAttribute(lo_revenue.clone()),
        false,
    );

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Key(2),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema,
    });

    let sort_attributes = vec![
        (OutputSource::Key(2), false),
        (OutputSource::Payload(0), true),
    ];
    let output_schema = vec![
        OutputSource::Payload(0),
        OutputSource::Payload(1),
        OutputSource::Key(0),
        OutputSource::Key(1),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q10(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    let mut output_schema = vec![];
    let mut groups = vec![];
    {
        let c = database.find_table(C_RELATION_NAME).unwrap();
        let c_city = c.get_column_by_id(SSB_CUSTOMER::C_CITY as usize).unwrap();
        let c_city_len = get_column_len(&c_city.column__type);

        let mut dynamic_operand_list = vec![];
        {
            let mut str_literal = String::from("UNITED KI1");
            while str_literal.len() < c_city_len {
                str_literal.push(PLACEHOLDER);
            }
            let left = Box::new(Scalar::ScalarAttribute(c_city.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        {
            let mut str_literal = String::from("UNITED KI5");
            while str_literal.len() < c_city_len {
                str_literal.push(PLACEHOLDER);
            }
            let left = Box::new(Scalar::ScalarAttribute(c_city.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Disjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });

        dim_tables.push(JoinContext::new(
            C_RELATION_NAME,
            SSB_CUSTOMER::C_CUSTKEY as usize,
            NUM_CUSTOMER + 1,
            predicate,
            vec![c_city.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(c_city.clone()));
        groups.push(Scalar::ScalarAttribute(c_city.clone()));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_city = s.get_column_by_id(SSB_SUPPLIER::S_CITY as usize).unwrap();
        let s_city_len = get_column_len(&s_city.column__type);

        let mut dynamic_operand_list = vec![];
        {
            let mut str_literal = String::from("UNITED KI1");
            while str_literal.len() < s_city_len {
                str_literal.push(PLACEHOLDER);
            }
            let left = Box::new(Scalar::ScalarAttribute(s_city.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        {
            let mut str_literal = String::from("UNITED KI5");
            while str_literal.len() < s_city_len {
                str_literal.push(PLACEHOLDER);
            }
            let left = Box::new(Scalar::ScalarAttribute(s_city.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Disjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });

        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![s_city.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(s_city.clone()));
        groups.push(Scalar::ScalarAttribute(s_city.clone()));
    }

    {
        let d = database.find_table(D_RELATION_NAME).unwrap();
        let d_yearmonth = d.get_column_by_id(SSB_DDATE::D_YEARMONTH as usize).unwrap();
        let left = Box::new(Scalar::ScalarAttribute(d_yearmonth.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char("Dec1997".to_string())));
        /*
        let d_yearmonthnum = d
            .get_column_by_id(SSB_DDATE::D_YEARMONTHNUM as usize)
            .unwrap();
        let left = Box::new(Scalar::ScalarAttribute(d_yearmonthnum.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(199712)));
        */
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
        dim_tables.push(JoinContext::new(
            D_RELATION_NAME,
            SSB_DDATE::D_DATEKEY as usize,
            19981230 + 1,
            predicate,
            vec![d_year.clone()],
        ));
        output_schema.push(Scalar::ScalarAttribute(d_year.clone()));
        groups.push(Scalar::ScalarAttribute(d_year.clone()));
    }

    output_schema.push(Scalar::ScalarAttribute(lo_revenue.clone()));
    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_CUSTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::ScalarAttribute(lo_revenue.clone()),
        false,
    );

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Key(2),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema,
    });

    let sort_attributes = vec![
        (OutputSource::Key(2), false),
        (OutputSource::Payload(0), true),
    ];
    let output_schema = vec![
        OutputSource::Payload(0),
        OutputSource::Payload(1),
        OutputSource::Key(0),
        OutputSource::Key(1),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q11(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let lo_supplycost = lo
        .get_column_by_id(SSB_LINEORDER::LO_SUPPLYCOST as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    {
        let c = database.find_table(C_RELATION_NAME).unwrap();
        let c_region = c.get_column_by_id(SSB_CUSTOMER::C_REGION as usize).unwrap();
        let c_region_len = get_column_len(&c_region.column__type);

        let mut str_literal = String::from("AMERICA");
        while str_literal.len() < c_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(c_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let c_nation = c.get_column_by_id(SSB_CUSTOMER::C_NATION as usize).unwrap();
        dim_tables.push(JoinContext::new(
            C_RELATION_NAME,
            SSB_CUSTOMER::C_CUSTKEY as usize,
            NUM_CUSTOMER + 1,
            predicate,
            vec![c_nation.clone()],
        ));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_region = s.get_column_by_id(SSB_SUPPLIER::S_REGION as usize).unwrap();
        let s_region_len = get_column_len(&s_region.column__type);

        let mut str_literal = String::from("AMERICA");
        while str_literal.len() < s_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(s_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![],
        ));
    }

    {
        let part_factor = (SCALE_FACTOR as f64).log2() as usize;
        let max_count = NUM_PART_BASE * (1 + part_factor) + 1;

        let p = database.find_table(P_RELATION_NAME).unwrap();
        let p_mfgr = p.get_column_by_id(SSB_PART::P_MFGR as usize).unwrap();

        let mut dynamic_operand_list = vec![];
        {
            let left = Box::new(Scalar::ScalarAttribute(p_mfgr.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char("MFGR#1".to_string())));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        {
            let left = Box::new(Scalar::ScalarAttribute(p_mfgr.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char("MFGR#2".to_string())));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        let predicate = Some(Predicate::Disjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });
        dim_tables.push(JoinContext::new(
            P_RELATION_NAME,
            SSB_PART::P_PARTKEY as usize,
            max_count,
            predicate,
            vec![],
        ));
    }

    let d = database.find_table(D_RELATION_NAME).unwrap();
    let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
    dim_tables.push(JoinContext::new(
        D_RELATION_NAME,
        SSB_DDATE::D_DATEKEY as usize,
        19981230 + 1,
        None,
        vec![d_year.clone()],
    ));

    let c = database.find_table(C_RELATION_NAME).unwrap();
    let c_nation = c.get_column_by_id(SSB_CUSTOMER::C_NATION as usize).unwrap();
    let groups = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(c_nation.clone()),
    ];
    let mut output_schema = groups.clone();
    output_schema.push(Scalar::ScalarAttribute(lo_revenue.clone()));
    output_schema.push(Scalar::ScalarAttribute(lo_supplycost.clone()));

    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_CUSTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_PARTKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let left = Box::new(Scalar::ScalarAttribute(lo_revenue.clone()));
    let right = Box::new(Scalar::ScalarAttribute(lo_supplycost.clone()));
    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::BinaryExpression {
            operation: BinaryOperation::Substract,
            left,
            right,
        },
        false,
    );

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema: output_schema.clone(),
    });

    let sort_attributes = vec![(OutputSource::Key(0), false), (OutputSource::Key(1), false)];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q12(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let lo_supplycost = lo
        .get_column_by_id(SSB_LINEORDER::LO_SUPPLYCOST as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    {
        let c = database.find_table(C_RELATION_NAME).unwrap();
        let c_region = c.get_column_by_id(SSB_CUSTOMER::C_REGION as usize).unwrap();
        let c_region_len = get_column_len(&c_region.column__type);

        let mut str_literal = String::from("AMERICA");
        while str_literal.len() < c_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(c_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        dim_tables.push(JoinContext::new(
            C_RELATION_NAME,
            SSB_CUSTOMER::C_CUSTKEY as usize,
            NUM_CUSTOMER + 1,
            predicate,
            vec![],
        ));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_region = s.get_column_by_id(SSB_SUPPLIER::S_REGION as usize).unwrap();
        let s_region_len = get_column_len(&s_region.column__type);

        let mut str_literal = String::from("AMERICA");
        while str_literal.len() < s_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(s_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let s_nation = s.get_column_by_id(SSB_SUPPLIER::S_NATION as usize).unwrap();
        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![s_nation.clone()],
        ));
    }

    {
        let part_factor = (SCALE_FACTOR as f64).log2() as usize;
        let max_count = NUM_PART_BASE * (1 + part_factor) + 1;

        let p = database.find_table(P_RELATION_NAME).unwrap();
        let p_mfgr = p.get_column_by_id(SSB_PART::P_MFGR as usize).unwrap();

        let mut dynamic_operand_list = vec![];
        {
            let left = Box::new(Scalar::ScalarAttribute(p_mfgr.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char("MFGR#1".to_string())));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }

        {
            let left = Box::new(Scalar::ScalarAttribute(p_mfgr.clone()));
            let right = Box::new(Scalar::ScalarLiteral(Literal::Char("MFGR#2".to_string())));
            dynamic_operand_list.push(Predicate::Comparison {
                comparison: Comparison::Equal,
                left,
                right,
            });
        }
        let predicate = Some(Predicate::Disjunction {
            static_operand_list: vec![],
            dynamic_operand_list,
        });

        let p_category = p.get_column_by_id(SSB_PART::P_CATEGORY as usize).unwrap();
        dim_tables.push(JoinContext::new(
            P_RELATION_NAME,
            SSB_PART::P_PARTKEY as usize,
            max_count,
            predicate,
            vec![p_category.clone()],
        ));
    }

    let d = database.find_table(D_RELATION_NAME).unwrap();
    let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();
    let mut dynamic_operand_list = vec![];
    {
        let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1997)));
        dynamic_operand_list.push(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });
    }
    {
        let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1998)));
        dynamic_operand_list.push(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });
    }

    let predicate = Some(Predicate::Disjunction {
        static_operand_list: vec![],
        dynamic_operand_list,
    });
    /*
    let operand = Box::new(Scalar::ScalarAttribute(d_year.clone()));
    let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(1997)));
    let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(1998)));
    let predicate = Some(Predicate::Between {
        operand,
        begin,
        end,
    });
    */
    dim_tables.push(JoinContext::new(
        D_RELATION_NAME,
        SSB_DDATE::D_DATEKEY as usize,
        19981230 + 1,
        predicate,
        vec![d_year.clone()],
    ));

    let s = database.find_table(S_RELATION_NAME).unwrap();
    let s_nation = s.get_column_by_id(SSB_SUPPLIER::S_NATION as usize).unwrap();
    let p = database.find_table(P_RELATION_NAME).unwrap();
    let p_category = p.get_column_by_id(SSB_PART::P_CATEGORY as usize).unwrap();
    let groups = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(s_nation.clone()),
        Scalar::ScalarAttribute(p_category.clone()),
    ];
    let mut output_schema = groups.clone();
    output_schema.push(Scalar::ScalarAttribute(lo_revenue.clone()));
    output_schema.push(Scalar::ScalarAttribute(lo_supplycost.clone()));

    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_CUSTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_PARTKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let left = Box::new(Scalar::ScalarAttribute(lo_revenue.clone()));
    let right = Box::new(Scalar::ScalarAttribute(lo_supplycost.clone()));
    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::BinaryExpression {
            operation: BinaryOperation::Substract,
            left,
            right,
        },
        false,
    );

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Key(2),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema: output_schema.clone(),
    });

    let sort_attributes = vec![
        (OutputSource::Key(0), false),
        (OutputSource::Key(1), false),
        (OutputSource::Key(2), false),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

#[allow(dead_code)]
fn q13(database: &Database) -> Box<PhysicalPlan> {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let lo_revenue = lo
        .get_column_by_id(SSB_LINEORDER::LO_REVENUE as usize)
        .unwrap();
    let lo_supplycost = lo
        .get_column_by_id(SSB_LINEORDER::LO_SUPPLYCOST as usize)
        .unwrap();
    let fact_table = Box::new(PhysicalPlan::TableReference {
        table: lo.clone(),
        alias: None,
        attribute_list: vec![/*FIXME*/],
    });

    let mut dim_tables = vec![];
    {
        let part_factor = (SCALE_FACTOR as f64).log2() as usize;
        let max_count = NUM_PART_BASE * (1 + part_factor) + 1;

        let p = database.find_table(P_RELATION_NAME).unwrap();
        let p_category = p.get_column_by_id(SSB_PART::P_CATEGORY as usize).unwrap();
        let left = Box::new(Scalar::ScalarAttribute(p_category.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char("MFGR#14".to_string())));
        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();
        dim_tables.push(JoinContext::new(
            P_RELATION_NAME,
            SSB_PART::P_PARTKEY as usize,
            max_count,
            predicate,
            vec![p_brand1.clone()],
        ));
    }

    {
        let s = database.find_table(S_RELATION_NAME).unwrap();
        let s_nation = s.get_column_by_id(SSB_SUPPLIER::S_NATION as usize).unwrap();
        let s_nation_len = get_column_len(&s_nation.column__type);

        let mut str_literal = String::from("UNITED STATES");
        while str_literal.len() < s_nation_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(s_nation.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        let s_city = s.get_column_by_id(SSB_SUPPLIER::S_CITY as usize).unwrap();
        dim_tables.push(JoinContext::new(
            S_RELATION_NAME,
            SSB_SUPPLIER::S_SUPPKEY as usize,
            NUM_SUPPLIER + 1,
            predicate,
            vec![s_city.clone()],
        ));
    }

    {
        let c = database.find_table(C_RELATION_NAME).unwrap();
        let c_region = c.get_column_by_id(SSB_CUSTOMER::C_REGION as usize).unwrap();
        let c_region_len = get_column_len(&c_region.column__type);

        let mut str_literal = String::from("AMERICA");
        while str_literal.len() < c_region_len {
            str_literal.push(PLACEHOLDER);
        }
        let left = Box::new(Scalar::ScalarAttribute(c_region.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Char(str_literal)));

        let predicate = Some(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });

        dim_tables.push(JoinContext::new(
            C_RELATION_NAME,
            SSB_CUSTOMER::C_CUSTKEY as usize,
            NUM_CUSTOMER + 1,
            predicate,
            vec![],
        ));
    }

    let d = database.find_table(D_RELATION_NAME).unwrap();
    let d_year = d.get_column_by_id(SSB_DDATE::D_YEAR as usize).unwrap();

    let mut dynamic_operand_list = vec![];
    {
        let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1997)));
        dynamic_operand_list.push(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });
    }
    {
        let left = Box::new(Scalar::ScalarAttribute(d_year.clone()));
        let right = Box::new(Scalar::ScalarLiteral(Literal::Int32(1998)));
        dynamic_operand_list.push(Predicate::Comparison {
            comparison: Comparison::Equal,
            left,
            right,
        });
    }

    let predicate = Some(Predicate::Disjunction {
        static_operand_list: vec![],
        dynamic_operand_list,
    });
    /*
    let operand = Box::new(Scalar::ScalarAttribute(d_year.clone()));
    let begin = Box::new(Scalar::ScalarLiteral(Literal::Int32(1997)));
    let end = Box::new(Scalar::ScalarLiteral(Literal::Int32(1998)));
    let predicate = Some(Predicate::Between {
        operand,
        begin,
        end,
    });
    */
    dim_tables.push(JoinContext::new(
        D_RELATION_NAME,
        SSB_DDATE::D_DATEKEY as usize,
        19981230 + 1,
        predicate,
        vec![d_year.clone()],
    ));

    let s = database.find_table(S_RELATION_NAME).unwrap();
    let s_city = s.get_column_by_id(SSB_SUPPLIER::S_CITY as usize).unwrap();
    let p = database.find_table(P_RELATION_NAME).unwrap();
    let p_brand1 = p.get_column_by_id(SSB_PART::P_BRAND1 as usize).unwrap();
    let groups = vec![
        Scalar::ScalarAttribute(d_year.clone()),
        Scalar::ScalarAttribute(s_city.clone()),
        Scalar::ScalarAttribute(p_brand1.clone()),
    ];
    let mut output_schema = groups.clone();
    output_schema.push(Scalar::ScalarAttribute(lo_revenue.clone()));
    output_schema.push(Scalar::ScalarAttribute(lo_supplycost.clone()));

    let join = Box::new(PhysicalPlan::StarJoin {
        fact_table,
        fact_table_filter: None,
        fact_table_join_column_ids: vec![
            SSB_LINEORDER::LO_PARTKEY as usize,
            SSB_LINEORDER::LO_SUPPKEY as usize,
            SSB_LINEORDER::LO_CUSTKEY as usize,
            SSB_LINEORDER::LO_ORDERDATE as usize,
        ],
        dim_tables,
        output_schema,
    });

    let left = Box::new(Scalar::ScalarAttribute(lo_revenue.clone()));
    let right = Box::new(Scalar::ScalarAttribute(lo_supplycost.clone()));
    let aggregate_context = AggregateContext::new(
        AggregateFunction::Sum,
        Scalar::BinaryExpression {
            operation: BinaryOperation::Substract,
            left,
            right,
        },
        false,
    );

    let output_schema = vec![
        OutputSource::Key(0),
        OutputSource::Key(1),
        OutputSource::Key(2),
        OutputSource::Payload(0),
    ];
    let aggregate = Box::new(PhysicalPlan::Aggregate {
        table: join,
        aggregates: vec![aggregate_context],
        groups,
        filter: None,
        output_schema: output_schema.clone(),
    });

    let sort_attributes = vec![
        (OutputSource::Key(0), false),
        (OutputSource::Key(1), false),
        (OutputSource::Key(2), false),
    ];
    let sort = Box::new(PhysicalPlan::Sort {
        input: aggregate,
        sort_attributes,
        limit: None,
        output_schema,
    });
    Box::new(PhysicalPlan::TopLevelPlan {
        plan: sort,
        shared_subplans: vec![],
    })
}

fn add_ssb_schemas(database: &mut Database) {
    add_lineorder(database);
    add_part(database);
    add_supplier(database);
    add_customer(database);
    add_ddate(database);
}

fn add_lineorder(database: &mut Database) {
    let lo_orderkey = Column::new2(
        "lo_orderkey",
        ColumnType::I32,
        SSB_LINEORDER::LO_ORDERKEY as usize,
        LO_RELATION_NAME,
        Some(ColumnAnnotation::PrimaryKey),
    );
    let lo_linenumber = Column::new2("lo_linenumber", ColumnType::I32, 1, LO_RELATION_NAME, None);
    let lo_custkey = Column::new2(
        "lo_custkey",
        ColumnType::I32,
        2,
        LO_RELATION_NAME,
        Some(ColumnAnnotation::ForeignKey),
    );
    let lo_partkey = Column::new2(
        "lo_partkey",
        ColumnType::I32,
        3,
        LO_RELATION_NAME,
        Some(ColumnAnnotation::ForeignKey),
    );
    let lo_suppkey = Column::new2(
        "lo_suppkey",
        ColumnType::I32,
        4,
        LO_RELATION_NAME,
        Some(ColumnAnnotation::ForeignKey),
    );
    let lo_orderdate = Column::new2(
        "lo_orderdate",
        ColumnType::I32,
        5,
        LO_RELATION_NAME,
        Some(ColumnAnnotation::ForeignKey),
    );
    let lo_orderpriority = Column::new2(
        "lo_orderpriority",
        ColumnType::Char(15),
        6,
        LO_RELATION_NAME,
        None,
    );
    let lo_shippriority = Column::new2(
        "lo_shippriority",
        ColumnType::Char(1),
        7,
        LO_RELATION_NAME,
        None,
    );
    let lo_quantity = Column::new2("lo_quantity", ColumnType::I32, 8, LO_RELATION_NAME, None);
    let lo_extendedprice = Column::new2(
        "lo_extendedprice",
        ColumnType::I32,
        9,
        LO_RELATION_NAME,
        None,
    );
    let lo_ordtotalprice = Column::new2(
        "lo_ordtotalprice",
        ColumnType::I32,
        10,
        LO_RELATION_NAME,
        None,
    );
    let lo_discount = Column::new2("lo_discount", ColumnType::I32, 11, LO_RELATION_NAME, None);
    let lo_revenue = Column::new2("lo_revenue", ColumnType::I32, 12, LO_RELATION_NAME, None);
    let lo_supplycost = Column::new2("lo_supplycost", ColumnType::I32, 13, LO_RELATION_NAME, None);
    let lo_tax = Column::new2("lo_tax", ColumnType::I32, 14, LO_RELATION_NAME, None);
    let lo_commitdate = Column::new2(
        "lo_commitdate",
        ColumnType::I32,
        15,
        LO_RELATION_NAME,
        Some(ColumnAnnotation::ForeignKey),
    );
    let lo_shipmode = Column::new2(
        "lo_shipmode",
        ColumnType::Char(10),
        16,
        LO_RELATION_NAME,
        None,
    );

    let lineorder = Table::new(
        LO_RELATION_NAME,
        vec![
            lo_orderkey,
            lo_linenumber,
            lo_custkey,
            lo_partkey,
            lo_suppkey,
            lo_orderdate,
            lo_orderpriority,
            lo_shippriority,
            lo_quantity,
            lo_extendedprice,
            lo_ordtotalprice,
            lo_discount,
            lo_revenue,
            lo_supplycost,
            lo_tax,
            lo_commitdate,
            lo_shipmode,
        ],
    );

    database.add_table(LO_RELATION_NAME, lineorder);
}

fn add_part(database: &mut Database) {
    let p_partkey = Column::new2(
        "p_partkey",
        ColumnType::I32,
        0,
        P_RELATION_NAME,
        Some(ColumnAnnotation::PrimaryKey),
    );
    let p_name = Column::new2("p_name", ColumnType::VarChar(22), 1, P_RELATION_NAME, None);
    let p_mfgr = Column::new2("p_mfgr", ColumnType::Char(6), 2, P_RELATION_NAME, None);
    let p_category = Column::new2("p_category", ColumnType::Char(7), 3, P_RELATION_NAME, None);
    let p_brand1 = Column::new2("p_brand1", ColumnType::Char(9), 4, P_RELATION_NAME, None);
    let p_color = Column::new2("p_color", ColumnType::VarChar(11), 5, P_RELATION_NAME, None);
    let p_type = Column::new2("p_type", ColumnType::VarChar(25), 6, P_RELATION_NAME, None);
    let p_size = Column::new2("p_size", ColumnType::I32, 7, P_RELATION_NAME, None);
    let p_container = Column::new2(
        "p_container",
        ColumnType::Char(10),
        8,
        P_RELATION_NAME,
        None,
    );

    let part = Table::new(
        P_RELATION_NAME,
        vec![
            p_partkey,
            p_name,
            p_mfgr,
            p_category,
            p_brand1,
            p_color,
            p_type,
            p_size,
            p_container,
        ],
    );

    database.add_table(P_RELATION_NAME, part);
}

fn add_supplier(database: &mut Database) {
    let s_suppkey = Column::new2(
        "s_suppkey",
        ColumnType::I32,
        0,
        S_RELATION_NAME,
        Some(ColumnAnnotation::PrimaryKey),
    );
    let s_name = Column::new2("s_name", ColumnType::Char(25), 1, S_RELATION_NAME, None);
    let s_address = Column::new2(
        "s_address",
        ColumnType::VarChar(25),
        2,
        S_RELATION_NAME,
        None,
    );
    let s_city = Column::new2("s_city", ColumnType::Char(10), 3, S_RELATION_NAME, None);
    let s_nation = Column::new2("s_nation", ColumnType::Char(15), 4, S_RELATION_NAME, None);
    let s_region = Column::new2("s_region", ColumnType::Char(12), 5, S_RELATION_NAME, None);
    let s_phone = Column::new2("s_phone", ColumnType::Char(15), 6, S_RELATION_NAME, None);

    let supplier = Table::new(
        S_RELATION_NAME,
        vec![
            s_suppkey, s_name, s_address, s_city, s_nation, s_region, s_phone,
        ],
    );
    database.add_table(S_RELATION_NAME, supplier);
}

fn add_customer(database: &mut Database) {
    let c_custkey = Column::new2(
        "c_custkey",
        ColumnType::I32,
        0,
        C_RELATION_NAME,
        Some(ColumnAnnotation::PrimaryKey),
    );
    let c_name = Column::new2("c_name", ColumnType::VarChar(25), 1, C_RELATION_NAME, None);
    let c_address = Column::new2(
        "c_address",
        ColumnType::VarChar(25),
        2,
        C_RELATION_NAME,
        None,
    );
    let c_city = Column::new2("c_city", ColumnType::Char(10), 3, C_RELATION_NAME, None);
    let c_nation = Column::new2("c_nation", ColumnType::Char(15), 4, C_RELATION_NAME, None);
    let c_region = Column::new2("c_region", ColumnType::Char(12), 5, C_RELATION_NAME, None);
    let c_phone = Column::new2("c_phone", ColumnType::Char(15), 6, C_RELATION_NAME, None);
    let c_mktsegment = Column::new2(
        "c_mktsegment",
        ColumnType::Char(10),
        7,
        C_RELATION_NAME,
        None,
    );

    let customer = Table::new(
        C_RELATION_NAME,
        vec![
            c_custkey,
            c_name,
            c_address,
            c_city,
            c_nation,
            c_region,
            c_phone,
            c_mktsegment,
        ],
    );
    database.add_table(C_RELATION_NAME, customer);
}

fn add_ddate(database: &mut Database) {
    let d_datekey = Column::new2(
        "d_datekey",
        ColumnType::I32,
        0,
        D_RELATION_NAME,
        Some(ColumnAnnotation::PrimaryKey),
    );
    let d_date = Column::new2("d_date", ColumnType::Char(18), 1, D_RELATION_NAME, None);
    let d_dayofweek = Column::new2("d_dayofweek", ColumnType::Char(9), 2, D_RELATION_NAME, None);
    let d_month = Column::new2("d_month", ColumnType::Char(9), 3, D_RELATION_NAME, None);
    let d_year = Column::new2(
        "d_year",
        ColumnType::I32,
        4,
        D_RELATION_NAME,
        Some(ColumnAnnotation::DerivedFromPrimaryKey(10000)),
    );
    let d_yearmonthnum = Column::new2(
        "d_yearmonthnum",
        ColumnType::I32,
        5,
        D_RELATION_NAME,
        Some(ColumnAnnotation::DerivedFromPrimaryKey(100)),
    );
    let d_yearmonth = Column::new2(
        "d_yearmonth",
        ColumnType::Char(7),
        6,
        D_RELATION_NAME,
        None, /* TODO: Convert to d_yearmonthnum */
    );
    let d_daynuminweek = Column::new2("d_daynuminweek", ColumnType::I32, 7, D_RELATION_NAME, None);
    let d_daynuminmonth =
        Column::new2("d_daynuminmonth", ColumnType::I32, 8, D_RELATION_NAME, None);
    let d_daynuminyear = Column::new2("d_daynuminyear", ColumnType::I32, 9, D_RELATION_NAME, None);
    let d_monthnuminyear = Column::new2(
        "d_monthnuminyear",
        ColumnType::I32,
        10,
        D_RELATION_NAME,
        None,
    );
    let d_weeknuminyear = Column::new2(
        "d_weeknuminyear",
        ColumnType::I32,
        11,
        D_RELATION_NAME,
        None, /* TODO: Convert to d_datekey */
    );
    let d_sellingseason = Column::new2(
        "d_sellingseason",
        ColumnType::VarChar(12),
        12,
        D_RELATION_NAME,
        None,
    );
    let d_lastdayinweekfl = Column::new2(
        "d_lastdayinweekfl",
        ColumnType::I32,
        13,
        D_RELATION_NAME,
        None,
    );
    let d_lastdayinmonthfl = Column::new2(
        "d_lastdayinmonthfl",
        ColumnType::I32,
        14,
        D_RELATION_NAME,
        None,
    );
    let d_holidayfl = Column::new2("d_holidayfl", ColumnType::I32, 15, D_RELATION_NAME, None);
    let d_weekdayfl = Column::new2("d_weekdayfl", ColumnType::I32, 16, D_RELATION_NAME, None);

    let ddate = Table::new(
        D_RELATION_NAME,
        vec![
            d_datekey,
            d_date,
            d_dayofweek,
            d_month,
            d_year,
            d_yearmonthnum,
            d_yearmonth,
            d_daynuminweek,
            d_daynuminmonth,
            d_daynuminyear,
            d_monthnuminyear,
            d_weeknuminyear,
            d_sellingseason,
            d_lastdayinweekfl,
            d_lastdayinmonthfl,
            d_holidayfl,
            d_weekdayfl,
        ],
    );
    database.add_table(D_RELATION_NAME, ddate);
}

fn load_lo(sm: &StorageManager, database: &Database) {
    let lo = database.find_table(LO_RELATION_NAME).unwrap();
    let mut schema = Vec::with_capacity(lo.columns.len());
    let mut schema_len = 0;
    for column in &lo.columns {
        let len = get_column_len(&column.column__type);
        schema_len += len;
        schema.push(len);
    }

    use std::io::BufRead;
    let lo_file = std::fs::File::open([DATA_DIRECTORY, LINEORDER_FILENAME].concat()).unwrap();
    let lo_lines = std::io::BufReader::new(lo_file).lines();
    let mut lo_data: Vec<u8> = Vec::with_capacity(NUM_LINEORDER * schema_len);
    for line in lo_lines {
        let row = line.unwrap();
        let columns: Vec<&str> = row.split('|').collect();

        let mut data = vec![];
        data.extend(
            columns[SSB_LINEORDER::LO_ORDERKEY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_LINENUMBER as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_CUSTKEY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_PARTKEY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_SUPPKEY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_ORDERDATE as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        let mut len = data.len();
        data.extend(columns[SSB_LINEORDER::LO_ORDERPRIORITY as usize].as_bytes());
        len += 15;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_LINEORDER::LO_SHIPPRIORITY as usize].as_bytes());
        debug_assert_eq!(data.len(), len + 1);

        data.extend(
            columns[SSB_LINEORDER::LO_QUANTITY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_EXTENDEDPRICE as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_ORDTOTALPRICE as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_DISCOUNT as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_REVENUE as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_SUPPLYCOST as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_TAX as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_LINEORDER::LO_COMMITDATE as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        len = data.len();
        data.extend(columns[SSB_LINEORDER::LO_SHIPMODE as usize].as_bytes());
        len += 10;
        data.resize(len, PLACEHOLDER as u8);

        lo_data.append(&mut data);
    }

    let re = sm.relational_engine();
    let mut lo_physical_relation = re.create(LO_RELATION_NAME, schema);

    let lo_raw = lo_data.as_ptr() as *const u8;
    let lo_buf = unsafe { std::slice::from_raw_parts(lo_raw, lo_data.len()) };
    lo_physical_relation.bulk_write(lo_buf);
}

fn load_p(sm: &StorageManager, database: &Database) {
    let part_factor = (SCALE_FACTOR as f64).log2() as usize;
    let num_part = NUM_PART_BASE * (1 + part_factor);

    let table = database.find_table(P_RELATION_NAME).unwrap();
    let mut schema = Vec::with_capacity(table.columns.len());
    let mut schema_len = 0;
    for column in &table.columns {
        let len = get_column_len(&column.column__type);
        schema_len += len;
        schema.push(len);
    }

    use std::io::BufRead;
    let file = std::fs::File::open([DATA_DIRECTORY, PART_FILENAME].concat()).unwrap();
    let lines = std::io::BufReader::new(file).lines();
    let mut rows: Vec<u8> = Vec::with_capacity(num_part * schema_len);
    for line in lines {
        let row = line.unwrap();
        let columns: Vec<&str> = row.split('|').collect();

        let mut data = vec![];
        data.extend(
            columns[SSB_PART::P_PARTKEY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        let mut len = data.len();
        data.extend(columns[SSB_PART::P_NAME as usize].as_bytes());
        len += 22;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_PART::P_MFGR as usize].as_bytes());
        len += 6;
        debug_assert_eq!(data.len(), len);

        data.extend(columns[SSB_PART::P_CATEGORY as usize].as_bytes());
        len += 7;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_PART::P_BRAND1 as usize].as_bytes());
        len += 9;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_PART::P_COLOR as usize].as_bytes());
        len += 11;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_PART::P_TYPE as usize].as_bytes());
        len += 25;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(
            columns[SSB_PART::P_SIZE as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        len = data.len();
        data.extend(columns[SSB_PART::P_CONTAINER as usize].as_bytes());
        len += 10;
        data.resize(len, PLACEHOLDER as u8);

        rows.append(&mut data);
    }

    let re = sm.relational_engine();
    let mut physical_relation = re.create(P_RELATION_NAME, schema);

    let raw = rows.as_ptr() as *const u8;
    let buf = unsafe { std::slice::from_raw_parts(raw, rows.len()) };
    physical_relation.bulk_write(buf);
}

fn load_s(sm: &StorageManager, database: &Database) {
    let table = database.find_table(S_RELATION_NAME).unwrap();
    let mut schema = Vec::with_capacity(table.columns.len());
    let mut schema_len = 0;
    for column in &table.columns {
        let len = get_column_len(&column.column__type);
        schema_len += len;
        schema.push(len);
    }

    use std::io::BufRead;

    let file = std::fs::File::open([DATA_DIRECTORY, SUPPLIER_FILENAME].concat()).unwrap();
    let lines = std::io::BufReader::new(file).lines();
    let mut rows: Vec<u8> = Vec::with_capacity(NUM_SUPPLIER * schema_len);
    for line in lines {
        let row = line.unwrap();
        let columns: Vec<&str> = row.split('|').collect();

        let mut data = vec![];
        data.extend(
            columns[SSB_SUPPLIER::S_SUPPKEY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        let mut len = data.len();
        data.extend(columns[SSB_SUPPLIER::S_NAME as usize].as_bytes());
        len += 25;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_SUPPLIER::S_ADDRESS as usize].as_bytes());
        len += 25;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_SUPPLIER::S_CITY as usize].as_bytes());
        len += 10;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_SUPPLIER::S_NATION as usize].as_bytes());
        len += 15;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_SUPPLIER::S_REGION as usize].as_bytes());
        len += 12;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_SUPPLIER::S_PHONE as usize].as_bytes());
        len += 15;
        data.resize(len, PLACEHOLDER as u8);

        rows.append(&mut data);
    }

    let re = sm.relational_engine();
    let mut physical_relation = re.create(S_RELATION_NAME, schema);

    let raw = rows.as_ptr() as *const u8;
    let buf = unsafe { std::slice::from_raw_parts(raw, rows.len()) };
    physical_relation.bulk_write(buf);
}

fn load_c(sm: &StorageManager, database: &Database) {
    let table = database.find_table(C_RELATION_NAME).unwrap();
    let mut schema = Vec::with_capacity(table.columns.len());
    let mut schema_len = 0;
    for column in &table.columns {
        let len = get_column_len(&column.column__type);
        schema_len += len;
        schema.push(len);
    }

    use std::io::BufRead;

    let file = std::fs::File::open([DATA_DIRECTORY, CUSTOMER_FILENAME].concat()).unwrap();
    let lines = std::io::BufReader::new(file).lines();
    let mut rows: Vec<u8> = Vec::with_capacity(NUM_CUSTOMER * schema_len);
    for line in lines {
        let row = line.unwrap();
        let columns: Vec<&str> = row.split('|').collect();

        let mut data = vec![];
        data.extend(
            columns[SSB_CUSTOMER::C_CUSTKEY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        let mut len = data.len();
        data.extend(columns[SSB_CUSTOMER::C_NAME as usize].as_bytes());
        len += 25;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_CUSTOMER::C_ADDRESS as usize].as_bytes());
        len += 25;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_CUSTOMER::C_CITY as usize].as_bytes());
        len += 10;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_CUSTOMER::C_NATION as usize].as_bytes());
        len += 15;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_CUSTOMER::C_REGION as usize].as_bytes());
        len += 12;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_CUSTOMER::C_PHONE as usize].as_bytes());
        len += 15;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_CUSTOMER::C_MKTSEGMENT as usize].as_bytes());
        len += 10;
        data.resize(len, PLACEHOLDER as u8);

        rows.append(&mut data);
    }

    let re = sm.relational_engine();
    let mut physical_relation = re.create(C_RELATION_NAME, schema);

    let raw = rows.as_ptr() as *const u8;
    let buf = unsafe { std::slice::from_raw_parts(raw, rows.len()) };
    physical_relation.bulk_write(buf);
}

fn load_d(sm: &StorageManager, database: &Database) {
    let table = database.find_table(D_RELATION_NAME).unwrap();
    let mut schema = Vec::with_capacity(table.columns.len());
    let mut schema_len = 0;
    for column in &table.columns {
        let len = get_column_len(&column.column__type);
        schema_len += len;
        schema.push(len);
    }

    use std::io::BufRead;

    let file = std::fs::File::open([DATA_DIRECTORY, DDATE_FILENAME].concat()).unwrap();
    let lines = std::io::BufReader::new(file).lines();
    let mut rows: Vec<u8> = Vec::with_capacity(NUM_DDATE * schema_len);
    for line in lines {
        let row = line.unwrap();
        let columns: Vec<&str> = row.split('|').collect();

        let mut data = vec![];
        data.extend(
            columns[SSB_DDATE::D_DATEKEY as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        let mut len = data.len();
        data.extend(columns[SSB_DDATE::D_DATE as usize].as_bytes());
        len += 18;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_DDATE::D_DAYOFWEEK as usize].as_bytes());
        len += 9;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(columns[SSB_DDATE::D_MONTH as usize].as_bytes());
        len += 9;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(
            columns[SSB_DDATE::D_YEAR as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_DDATE::D_YEARMONTHNUM as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        len = data.len();
        data.extend(columns[SSB_DDATE::D_YEARMONTH as usize].as_bytes());
        len += 7;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(
            columns[SSB_DDATE::D_DAYNUMINWEEK as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_DDATE::D_DAYNUMINMONTH as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_DDATE::D_DAYNUMINYEAR as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_DDATE::D_MONTHNUMINYEAR as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_DDATE::D_WEEKNUMINYEAR as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        len = data.len();
        data.extend(columns[SSB_DDATE::D_SELLINGSEASON as usize].as_bytes());
        len += 12;
        data.resize(len, PLACEHOLDER as u8);

        data.extend(
            columns[SSB_DDATE::D_LASTDAYINWEEKFL as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_DDATE::D_LASTDAYINMONTHFL as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_DDATE::D_HOLIDAYFL as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );
        data.extend(
            columns[SSB_DDATE::D_WEEKDAYFL as usize]
                .parse::<i32>()
                .unwrap()
                .to_ne_bytes()
                .iter(),
        );

        rows.append(&mut data);
    }

    let re = sm.relational_engine();
    let mut physical_relation = re.create(D_RELATION_NAME, schema);

    let raw = rows.as_ptr() as *const u8;
    let buf = unsafe { std::slice::from_raw_parts(raw, rows.len()) };
    physical_relation.bulk_write(buf);
}
