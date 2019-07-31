use std::collections::HashSet;

use hustle_operators::{QueryPlan, QueryPlanDag, WorkOrder, WorkOrdersContainer};

pub struct ExecutionState {
    query_plan: QueryPlan,
    num_operators: usize,
    producers: Vec<HashSet<usize>>,
    done_work_order_generation: Vec<bool>,
    // rebuild_status: HashMap<usize, (bool /* has_initiated_rebuild */, usize /* num_pending_rebuild_work_orders */)>,
    op_completion: Vec<bool>,
    num_operators_completion: usize,
    work_orders_container: WorkOrdersContainer,
}

impl ExecutionState {
    pub fn new(query_plan: QueryPlan, query_plan_dag: &QueryPlanDag) -> Self {
        let num_operators = query_plan.num_operators();

        let mut producers = (0..num_operators)
            .map(|_| HashSet::new())
            .collect::<Vec<HashSet<usize>>>();
        for producer_op_index in 0..num_operators {
            for consumer_op_index in &query_plan_dag.dependents()[producer_op_index] {
                producers[*consumer_op_index].insert(producer_op_index);
            }
        }

        let mut execution_state = ExecutionState {
            query_plan,
            num_operators,
            producers,
            done_work_order_generation: vec![false; num_operators],
            // rebuild_status: HashMap::new(),
            op_completion: vec![false; num_operators],
            num_operators_completion: 0,
            work_orders_container: WorkOrdersContainer::new(num_operators),
        };

        Self::init(&mut execution_state, query_plan_dag);
        execution_state
    }

    fn init(&mut self, query_plan_dag: &QueryPlanDag) {
        for i in 0..self.num_operators {
            if Self::check_dependencies_met(self, i) && !Self::fetch_normal_work_orders(self, i) {
                Self::mark_operator_completion(self, i, query_plan_dag);
            }
        }
    }

    pub fn get_next_work_orders(&mut self) -> Vec<(Box<dyn WorkOrder>, usize)> {
        self.work_orders_container.get_next_normal_work_orders()
    }

    pub fn mark_work_order_completion(&mut self, op_index: usize) {
        debug_assert!(op_index < self.num_operators);
        self.work_orders_container
            .mark_work_order_completion(op_index);
    }

    pub fn fetch_normal_work_orders(&mut self, op_index: usize) -> bool {
        debug_assert!(Self::check_dependencies_met(self, op_index));
        debug_assert!(!self.done_work_order_generation[op_index]);

        let num_pending_work_orders = self
            .work_orders_container
            .get_num_normal_work_orders(op_index);
        if self
            .query_plan
            .get_operator_mutable(op_index)
            .get_all_work_orders(&mut self.work_orders_container)
        {
            self.done_work_order_generation[op_index] = true;
        }

        num_pending_work_orders
            < self
                .work_orders_container
                .get_num_normal_work_orders(op_index)
    }

    pub fn check_dependencies_met(&self, op_index: usize) -> bool {
        debug_assert!(op_index < self.num_operators);

        self.producers[op_index].is_empty()
    }

    pub fn mark_operator_completion(&mut self, op_index: usize, query_plan_dag: &QueryPlanDag) {
        debug_assert!(op_index < self.num_operators);
        debug_assert!(!self.op_completion[op_index]);

        self.op_completion[op_index] = true;
        self.num_operators_completion += 1;

        for consumer_op_index in &query_plan_dag.dependents()[op_index] {
            let consumer_op_index = *consumer_op_index;
            self.producers[consumer_op_index].remove(&op_index);

            self.query_plan
                .get_operator_mutable(consumer_op_index)
                .done_feeding_input(None);
            if Self::check_dependencies_met(self, consumer_op_index)
                && !Self::fetch_normal_work_orders(self, consumer_op_index)
                && Self::check_normal_execution_completion(self, consumer_op_index)
            {
                Self::mark_operator_completion(self, consumer_op_index, query_plan_dag);
            }
        }
    }

    pub fn check_normal_execution_completion(&self, op_index: usize) -> bool {
        debug_assert!(op_index < self.num_operators);

        !self.work_orders_container.has_normal_work_order(op_index)
            && self.done_work_order_generation[op_index]
    }

    pub fn done(&self) -> bool {
        self.num_operators_completion == self.num_operators
    }

    pub fn display(&self) {
        self.query_plan.display();
    }
}
