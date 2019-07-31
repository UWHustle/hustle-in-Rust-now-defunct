use std::collections::HashMap;

use hustle_common::PhysicalPlan;

pub struct PhysicalGenerator {}

impl PhysicalGenerator {
    pub fn new() -> Self {
        PhysicalGenerator {}
    }

    pub fn optimize_plan(
        &self,
        database: &hustle_common::Database,
        physical_plan: &mut PhysicalPlan,
        print_plan: bool,
    ) {
        let mut rules: Vec<Box<dyn crate::rules::Rule>> = vec![];

        rules.push(Box::new(
            crate::rules::rewrite_predicate::RewritePredicate::new(),
        ));
        rules.push(Box::new(crate::rules::eliminate_join::EliminateJoin::new()));
        rules.push(Box::new(crate::rules::fuse_operators::FuseOperators::new()));
        /*
        rules.push(Box::new(
            crate::rules::set_evaluation_order::SetEvaluationOrder::new(),
        ));
        */

        if print_plan {
            let mut plan_visualier = PlanVisualizer::new();
            print!("{}", plan_visualier.visualize(physical_plan, "g_init"));
        }

        for rule in rules {
            rule.apply(database, physical_plan);

            if print_plan {
                let mut plan_visualier = PlanVisualizer::new();
                print!("{}", plan_visualier.visualize(physical_plan, rule.name()));
            }
        }
    }
}

struct NodeInfo {
    pub id: usize,
    pub labels: Vec<String>,
    pub color: String,
}

impl NodeInfo {
    pub fn new(id: usize) -> Self {
        NodeInfo {
            id,
            labels: Vec::new(),
            color: "".to_string(),
        }
    }
}

struct EdgeInfo {
    pub src_id: usize,
    pub dst_id: usize,
    pub labels: Vec<String>,
}

impl EdgeInfo {
    pub fn new(src_id: usize, dst_id: usize) -> Self {
        EdgeInfo {
            src_id,
            dst_id,
            labels: Vec::new(),
        }
    }
}

struct PlanVisualizer {
    nodes: Vec<NodeInfo>,
    edges: Vec<EdgeInfo>,
    node_id_map: HashMap<PhysicalPlan, usize>,
}

impl PlanVisualizer {
    pub fn new() -> Self {
        PlanVisualizer {
            nodes: vec![],
            edges: vec![],
            node_id_map: HashMap::new(),
        }
    }

    pub fn visualize(&mut self, physical_plan: &PhysicalPlan, graph_name: &str) -> String {
        let mut stringified_graph = String::from("digraph ");
        stringified_graph.push_str(graph_name);
        stringified_graph.push_str(" {\n");
        stringified_graph.push_str("  rankdir=BT\n");
        stringified_graph.push_str("  node [penwidth=2]\n");
        stringified_graph.push_str("  edge [fontsize=16 fontcolor=gray penwidth=2]\n\n");

        Self::visit(self, physical_plan);

        for node in &self.nodes {
            stringified_graph.push_str("  ");
            stringified_graph.push_str(&node.id.to_string());
            stringified_graph.push_str(" [");

            stringified_graph.push_str("label=\"");
            stringified_graph.push_str(&node.labels.join("&#10;"));
            stringified_graph.push('"');

            stringified_graph.push_str("");
            if !node.color.is_empty() {
                stringified_graph.push_str(" style=filled fillcolor=\"");
                stringified_graph.push_str(&node.color);

                stringified_graph.push('"');
            }

            stringified_graph.push_str("]\n");
        }
        stringified_graph.push('\n');

        for edge in &self.edges {
            stringified_graph.push_str("  ");
            stringified_graph.push_str(&edge.src_id.to_string());
            stringified_graph.push_str(" -> ");
            stringified_graph.push_str(&edge.dst_id.to_string());
            stringified_graph.push_str(" [");

            stringified_graph.push_str("]\n");
        }

        stringified_graph.push_str("}\n");
        stringified_graph
    }

    fn visit(&mut self, physical_plan: &PhysicalPlan) {
        if let PhysicalPlan::TopLevelPlan { plan, .. } = physical_plan {
            Self::visit_helper(self, &*plan);
        }
    }

    fn visit_helper(&mut self, physical_plan: &PhysicalPlan) {
        let node_id = self.nodes.len();
        self.node_id_map.insert(physical_plan.clone(), node_id);
        self.nodes.push(NodeInfo::new(node_id));
        let mut node = self.nodes.last_mut().unwrap();

        match physical_plan {
            PhysicalPlan::Aggregate { table, .. } => {
                node.labels.push("Aggregate".to_string());
                node.color = "pink".to_string();
                Self::visit_helper(self, &*table);
                let child_id = self.node_id_map.get(&*table).unwrap();
                self.edges.push(EdgeInfo::new(*child_id, node_id));
            }
            PhysicalPlan::StarJoin {
                fact_table,
                fact_table_filter: _,
                fact_table_join_column_ids: _,
                dim_tables,
                ..
            } => {
                node.labels.push("StarJoin".to_string());

                node.color = "red".to_string();

                for dim_table in dim_tables {
                    let dim_node_id = self.nodes.len();
                    self.nodes.push(NodeInfo::new(dim_node_id));
                    let mut dim_node = self.nodes.last_mut().unwrap();
                    dim_node.labels.push(dim_table.table_name.clone());
                    self.edges.push(EdgeInfo::new(dim_node_id, node_id));
                    dim_node.color = "skyblue".to_string();
                }
                Self::visit_helper(self, &*fact_table);
                let child_id = self.node_id_map.get(&*fact_table).unwrap();
                self.edges.push(EdgeInfo::new(*child_id, node_id));
            }
            PhysicalPlan::StarJoinAggregate {
                fact_table,
                fact_table_filter: _,
                fact_table_join_column_ids: _,
                dim_tables,
                ..
            } => {
                node.labels.push("StarJoinAggregate".to_string());

                node.color = "pink".to_string();

                for dim_table in dim_tables {
                    let dim_node_id = self.nodes.len();
                    self.nodes.push(NodeInfo::new(dim_node_id));
                    let mut dim_node = self.nodes.last_mut().unwrap();
                    dim_node.labels.push(dim_table.table_name.clone());
                    self.edges.push(EdgeInfo::new(dim_node_id, node_id));
                    dim_node.color = "skyblue".to_string();
                }

                Self::visit_helper(self, &*fact_table);
                let child_id = self.node_id_map.get(&*fact_table).unwrap();
                self.edges.push(EdgeInfo::new(*child_id, node_id));
            }
            PhysicalPlan::Sort { input, .. } => {
                node.labels.push("Sort".to_string());
                Self::visit_helper(self, &*input);
                let child_id = self.node_id_map.get(&*input).unwrap();
                self.edges.push(EdgeInfo::new(*child_id, node_id));
            }
            PhysicalPlan::TableReference { table, .. } => {
                node.labels.push(table.name.clone());

                node.color = "cyan".to_string();
            }
            PhysicalPlan::TopLevelPlan { .. } => unreachable!(),
            _ => unimplemented!(),
        }
    }
}
