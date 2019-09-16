use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use hustle_common::plan::{Plan, Expression, Query, QueryOperator, Statement};

use crate::Domain;
use crate::policy::ColumnManager;
use hustle_catalog::{Table, Column};
use hustle_types::ComparativeVariant;

type IndexedDomain = HashMap<u64, Vec<Domain>>;

#[derive(Clone, Debug)]
pub struct TransactionStatement {
    pub inner: Statement,
    read_domain: IndexedDomain,
    write_domain: IndexedDomain,
    filter_domain: IndexedDomain,
}

impl TransactionStatement {
    pub fn new(id: u64, transaction_id: u64, plan: Plan, column_manager: &mut ColumnManager) -> Self {
        let mut read_domain = IndexedDomain::new();
        let mut write_domain = IndexedDomain::new();
        let mut filter_domain = IndexedDomain::new();

        Self::parse_domain(
            &plan,
            &mut read_domain,
            &mut write_domain,
            &mut filter_domain,
            column_manager,
        );

        let inner = Statement { id, transaction_id, plan };

        TransactionStatement {
            inner,
            read_domain,
            write_domain,
            filter_domain,
        }
    }

    pub fn conflicts(&self, other: &Self) -> bool {
        !(self.is_read_only() && other.is_read_only())
            && !self.filter_guarantees_no_conflict(other)
            && self.domains_intersect(other)
    }

    pub fn is_read_only(&self) -> bool {
        self.write_domain.is_empty()
    }

    fn filter_guarantees_no_conflict(&self, other: &Self) -> bool {
        Self::compare_domains(
            &self.filter_domain,
            &other.filter_domain,
            |self_domains, other_domains|
                self_domains.iter().any(|self_domain|
                    other_domains.iter().all(|other_domain|
                        !self_domain.intersects(other_domain)
                    )
                ),
        )
    }

    fn domains_intersect(&self, other: &Self) -> bool {
        let intersects = |self_domains: &Vec<Domain>, other_domains: &Vec<Domain>|
            self_domains.iter().any(|self_domain|
                other_domains.iter().any(|other_domain|
                    self_domain.intersects(other_domain)
                )
            );

        Self::compare_domains(&self.read_domain, &other.write_domain, intersects)
            || Self::compare_domains(&self.filter_domain, &other.write_domain, intersects)
            || Self::compare_domains(&self.write_domain, &other.read_domain, intersects)
            || Self::compare_domains(&self.write_domain, &other.filter_domain, intersects)
            || Self::compare_domains(&self.write_domain, &other.write_domain, intersects)
    }

    fn compare_domains(
        domain_a: &IndexedDomain,
        domain_b: &IndexedDomain,
        comparison: impl Fn(&Vec<Domain>, &Vec<Domain>) -> bool
    ) -> bool {
        domain_a.iter().any(|(self_column, self_domains)|
            domain_b.get(self_column)
                .map(|other_domains|
                    (comparison)(self_domains, other_domains)
                )
                .unwrap_or(false)
        )
    }

    fn parse_domain(
        plan: &Plan,
        read_domain: &mut IndexedDomain,
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        match plan {
            Plan::Insert { into_table, bufs } => {
                Self::parse_insert_domain(
                    into_table,
                    bufs,
                    write_domain,
                    filter_domain,
                    column_manager,
                );
            },
            Plan::Update { table, assignments, filter } => {
                Self::parse_update_domain(
                    table,
                    assignments,
                    filter,
                    read_domain,
                    write_domain,
                    filter_domain,
                    column_manager,
                );
            },
            Plan::Delete { from_table, filter } => {
                Self::parse_delete_domain(
                    from_table,
                    filter,
                    write_domain,
                    filter_domain,
                    column_manager,
                );
            },
            Plan::Query(query) => {
                Self::parse_query_domain(query, read_domain, filter_domain, column_manager);
            },
            _ => (), // Ignore all other plan node types for now.
        }
    }

    fn parse_insert_domain(
        into_table: &Table,
        bufs: &[Vec<u8>],
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        // Create a new value lock with the value being inserted for each column.
        for (column, buf) in into_table.columns.iter().zip(bufs) {
            let column_id = column_manager.get_column_id(column.get_table(), column.get_name());
            let domain = Domain::new(
                ComparativeVariant::Eq,
                column.get_type_variant().clone(),
                buf.clone(),
            );
            write_domain.insert(column_id, vec![domain.clone()]);
            filter_domain.insert(column_id, vec![domain]);
        }
    }

    fn parse_update_domain(
        table: &Table,
        assignments: &[(usize, Vec<u8>)],
        filter: &Option<Box<Expression>>,
        read_domain: &mut IndexedDomain,
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        let mut rewrite_filter = false;
        if let Some(f) = filter {
            Self::parse_filter(
                &*f,
                &table.columns,
                &mut rewrite_filter,
                filter_domain,
                column_manager,
            );
        }

        if rewrite_filter {
            for column in &table.columns {
                let column_id = column_manager.get_column_id(column.get_table(), column.get_name());
                write_domain.insert(column_id, vec![Domain::any()]);
            }
            read_domain.extend(
                filter_domain.drain()
                    .filter(|(column_id, _)| !write_domain.contains_key(column_id))
            );
        } else {
            for (col_i, buf) in assignments {
                let column = &table.columns[*col_i];
                let column_id = column_manager.get_column_id(column.get_table(), column.get_name());
                if let Some(mut value_domain) = filter_domain.remove(&column_id) {
                    let domain = Domain::new(
                        ComparativeVariant::Eq,
                        column.get_type_variant().clone(),
                        buf.clone()
                    );
                    value_domain.push(domain);
                    write_domain.insert(column_id, value_domain);
                } else {
                    write_domain.insert(column_id, vec![Domain::any()]);
                }
            }
        }
    }

    fn parse_delete_domain(
        from_table: &Table,
        filter: &Option<Box<Expression>>,
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        // Create a new domain that covers the entire column.
        for column in &from_table.columns {
            let column_id = column_manager.get_column_id(column.get_table(), column.get_name());
            write_domain.insert(column_id, vec![Domain::any()]);
        }

        if let Some(f) = filter {
            Self::parse_delete_filter(
                &*f,
                &from_table.columns,
                write_domain,
                filter_domain,
                column_manager,
            );
        }
    }

    fn parse_query_domain(
        query: &Query,
        read_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        match &query.operator {
            QueryOperator::Cartesian { inputs } => {
                for input in inputs {
                    Self::parse_query_domain(input, read_domain, filter_domain, column_manager);
                }
            },
            QueryOperator::Project { input, cols } => {
                Self::parse_query_domain(&**input, read_domain, filter_domain, column_manager);
                Self::parse_project_domain(
                    cols,
                    &input.output,
                    read_domain,
                    filter_domain,
                    column_manager,
                );
            },
            QueryOperator::Select { input, filter } => {
                Self::parse_query_domain(&**input, read_domain, filter_domain, column_manager);
                Self::parse_select_filter(
                    &**filter,
                    &input.output,
                    read_domain,
                    filter_domain,
                    column_manager,
                );
            },
            _ => (),
        }
    }

    fn parse_project_domain(
        col_is: &[usize],
        columns: &[Column],
        read_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        for &col_i in col_is {
            let column = &columns[col_i];
            let column_id = column_manager.get_column_id(column.get_table(), column.get_name());
            if !filter_domain.contains_key(&column_id) {
                let value_domain = read_domain.entry(column_id).or_default();
                // Do not overwrite other domains that may have been parsed.
                if value_domain.is_empty() {
                    value_domain.push(Domain::any());
                }
            }
        }
    }

    fn parse_delete_filter(
        filter: &Expression,
        columns: &[Column],
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        let mut rewrite_filter = false;
        Self::parse_filter(filter, columns, &mut rewrite_filter, filter_domain, column_manager);
        if rewrite_filter && filter_domain.len() > 1 {
            write_domain.extend(filter_domain.drain());
        } else {
            write_domain.extend(filter_domain.iter().map(|(column_id, value_domain)|
                (column_id.to_owned(), value_domain.clone())
            ));
        }
    }

    fn parse_select_filter(
        filter: &Expression,
        columns: &[Column],
        read_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        let mut rewrite_filter = false;
        Self::parse_filter(filter, columns, &mut rewrite_filter, filter_domain, column_manager);
        if rewrite_filter && filter_domain.len() > 1 {
            read_domain.extend(filter_domain.drain());
        } else {
            for (column_id, _) in filter_domain {
                read_domain.remove(column_id);
            }
        }
    }

    fn parse_filter(
        filter: &Expression,
        columns: &[Column],
        rewrite_filter: &mut bool,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        match filter {
            Expression::Conjunctive { terms } => {
                for term in terms {
                    Self::parse_filter(
                        term,
                        columns,
                        rewrite_filter,
                        filter_domain,
                        column_manager,
                    );
                }
            },
            Expression::Disjunctive { terms } => {
                for term in terms {
                    Self::parse_filter(
                        term,
                        columns,
                        rewrite_filter,
                        filter_domain,
                        column_manager,
                    );
                }
                *rewrite_filter = true;
            },
            Expression::Comparative {
                variant: comparative_variant,
                left: l_expr,
                right: r_expr,
            } => {
                match &**l_expr {
                    Expression::ColumnReference(l_col_i) => {
                        let l_column = &columns[*l_col_i];
                        let l_column_id = column_manager.get_column_id(
                            l_column.get_table(),
                            l_column.get_name(),
                        );

                        match &**r_expr {
                            Expression::Literal { type_variant, buf } => {
                                // Filter predicate.
                                let value_domain = filter_domain.entry(l_column_id).or_default();

                                // If there is a domain of any for this column, replace it.
                                if value_domain.first().map(|d| d.is_any()).unwrap_or(false) {
                                    value_domain.clear();
                                }

                                let domain = Domain::new(
                                    comparative_variant.clone(),
                                    type_variant.clone(),
                                    buf.clone(),
                                );
                                value_domain.push(domain);
                            },
                            Expression::ColumnReference(r_col_i) => {
                                // Join predicate.
                                let r_column = &columns[*r_col_i];
                                let r_column_id = column_manager.get_column_id(
                                    r_column.get_table(),
                                    r_column.get_name(),
                                );

                                for column_id in &[l_column_id, r_column_id] {
                                    let value_domain = filter_domain.entry(*column_id).or_default();
                                    // Do not overwrite other domains that may have been parsed.
                                    if value_domain.is_empty() {
                                        value_domain.push(Domain::any());
                                    }
                                }
                            },
                            _ => panic!("Predicate lock only supports literals or column \
                            references on right side of comparison"),
                        }
                    },
                    _ => panic!("Predicate lock only supports columns on left side of comparison"),
                }
            },
            _ => panic!("Invalid plan node type for filter ({:?})", filter),
        }
    }
}

impl PartialEq for TransactionStatement {
    fn eq(&self, other: &Self) -> bool {
        self.inner.id.eq(&other.inner.id)
    }
}

impl Eq for TransactionStatement {}

impl Hash for TransactionStatement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.id.hash(state);
    }
}

impl Borrow<u64> for TransactionStatement {
    fn borrow(&self) -> &u64 {
        &self.inner.id
    }
}
