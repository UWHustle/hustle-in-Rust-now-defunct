use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use hustle_common::{Column, Plan, Table};
use hustle_types::data_type::DataType;
use hustle_types::operators::Comparator;

use crate::Domain;
use crate::policy::ColumnManager;

type IndexedDomain = HashMap<u64, Vec<Domain>>;

#[derive(Debug)]
pub struct Statement {
    pub id: u64,
    pub transaction_id: u64,
    pub plan: Plan,
    read_domain: IndexedDomain,
    write_domain: IndexedDomain,
    filter_domain: IndexedDomain,
}

impl Statement {
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

        Statement {
            id,
            transaction_id,
            plan,
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
            &|self_domains, other_domains|
                self_domains.iter().any(|self_domain|
                    other_domains.iter().all(|other_domain|
                        !self_domain.intersects(other_domain)
                    )
                ),
        )
    }

    fn domains_intersect(&self, other: &Self) -> bool {
        let intersects = &|self_domains: &Vec<Domain>, other_domains: &Vec<Domain>|
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
        comparison: &Fn(&Vec<Domain>, &Vec<Domain>) -> bool
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
            Plan::Delete { from_table, filter } => {
                Self::parse_delete_domain(
                    from_table,
                    filter,
                    write_domain,
                    filter_domain,
                    column_manager,
                );
            },
            Plan::Insert { into_table, input } => {
                Self::parse_insert_domain(
                    into_table,
                    input,
                    write_domain,
                    filter_domain,
                    column_manager,
                );
            },
            Plan::Project { table, projection } => {
                Self::parse_domain(
                    &**table,
                    read_domain,
                    write_domain,
                    filter_domain,
                    column_manager,
                );
                Self::parse_project_domain(
                    projection,
                    read_domain,
                    filter_domain,
                    column_manager,
                );
            },
            Plan::Select { table, filter } => {
                Self::parse_domain(
                    &**table,
                    read_domain,
                    write_domain,
                    filter_domain,
                    column_manager,
                );
                Self::parse_select_filter(&**filter, read_domain, filter_domain, column_manager);
            },
            Plan::Update { table: _, columns, assignments, filter } => {
                Self::parse_update_domain(
                    columns,
                    assignments,
                    filter,
                    read_domain,
                    write_domain,
                    filter_domain,
                    column_manager,
                );
            },
            _ => (), // Ignore all other plan node types for now.
        }
    }

    fn parse_delete_domain(
        from_table: &Table,
        filter: &Option<Box<Plan>>,
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        // Create a new domain that covers the entire column.
        for column in &from_table.columns {
            let column_id = column_manager.get_column_id(&column.table, &column.name);
            write_domain.insert(column_id, vec![Domain::any()]);
        }

        if let Some(f) = filter {
            Self::parse_delete_filter(&*f, write_domain, filter_domain, column_manager);
        }
    }

    fn parse_insert_domain(
        into_table: &Table,
        input: &Plan,
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        // Create a new value lock with the value being inserted for each column.
        if let Plan::Row { values } = input {
            for (column, literal) in into_table.columns.iter().zip(values) {
                if let Plan::Literal { value, literal_type } = literal {
                    let column_id = column_manager.get_column_id(&column.table, &column.name);
                    let domain = Self::parse_value(Comparator::Eq, value, literal_type);
                    write_domain.insert(column_id, vec![domain.clone()]);
                    filter_domain.insert(column_id, vec![domain]);
                } else {
                    panic!("Predicate lock only supports inserting literal values")
                }
            }
        } else {
            panic!("Predicate lock only supports inserting a row")
        }
    }

    fn parse_project_domain(
        projection: &Vec<Plan>,
        read_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        for column_reference in projection {
            if let Plan::ColumnReference { column } = column_reference {
                let column_id = column_manager.get_column_id(&column.table, &column.name);

                if !filter_domain.contains_key(&column_id) {
                    let value_domain = read_domain.entry(column_id).or_default();
                    // Do not overwrite other domains that may have been parsed.
                    if value_domain.is_empty() {
                        value_domain.push(Domain::any());
                    }
                }
            } else {
                panic!("Predicate lock only supports column references in projection")
            }
        }
    }

    fn parse_update_domain(
        columns: &Vec<Column>,
        assignments: &Vec<Plan>,
        filter: &Option<Box<Plan>>,
        read_domain: &mut IndexedDomain,
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        let mut rewrite_filter = false;
        if let Some(f) = filter {
            Self::parse_filter(&*f, &mut rewrite_filter, filter_domain, column_manager);
        }

        if rewrite_filter {
            for column in columns {
                let column_id = column_manager.get_column_id(&column.table, &column.name);
                write_domain.insert(column_id, vec![Domain::any()]);
            }
            read_domain.extend(
                filter_domain.drain()
                    .filter(|(column_id, _)| !write_domain.contains_key(column_id))
            );
        } else {
            for (column, literal) in columns.iter().zip(assignments) {
                let column_id = column_manager.get_column_id(&column.table, &column.name);
                if let Some(mut value_domain) = filter_domain.remove(&column_id) {
                    if let Plan::Literal { value, literal_type } = literal {
                        let domain = Self::parse_value(Comparator::Eq, value, literal_type);
                        value_domain.push(domain);
                        write_domain.insert(column_id, value_domain);
                    } else {
                        panic!("Predicate lock only supports literals as assignments in update")
                    }
                } else {
                    write_domain.insert(column_id, vec![Domain::any()]);
                }
            }
        }
    }

    fn parse_delete_filter(
        filter: &Plan,
        write_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        let mut rewrite_filter = false;
        Self::parse_filter(filter, &mut rewrite_filter, filter_domain, column_manager);
        if rewrite_filter && filter_domain.len() > 1 {
            write_domain.extend(filter_domain.drain());
        } else {
            write_domain.extend(filter_domain.iter().map(|(column_id, value_domain)|
                (column_id.to_owned(), value_domain.clone())
            ));
        }
    }

    fn parse_select_filter(
        filter: &Plan,
        read_domain: &mut IndexedDomain,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        let mut rewrite_filter = false;
        Self::parse_filter(filter, &mut rewrite_filter, filter_domain, column_manager);
        if rewrite_filter && filter_domain.len() > 1 {
            read_domain.extend(filter_domain.drain());
        } else {
            for (column_id, _) in filter_domain {
                read_domain.remove(column_id);
            }
        }
    }

    fn parse_filter(
        filter: &Plan,
        rewrite_filter: &mut bool,
        filter_domain: &mut IndexedDomain,
        column_manager: &mut ColumnManager,
    ) {
        match filter {
            Plan::Connective { name, terms } => {
                for term in terms {
                    Self::parse_filter(
                        term,
                        rewrite_filter,
                        filter_domain,
                        column_manager,
                    );
                }
                if name != "and" {
                    *rewrite_filter = true;
                }
            },

            Plan::Comparative { name, left, right } => {
                let comparator = Comparator::from_str(name).unwrap();
                if let Plan::ColumnReference { column: left_column } = &**left {
                    let left_column_id = column_manager.get_column_id(
                        &left_column.table,
                        &left_column.name,
                    );

                    match &**right {
                        Plan::Literal { value, literal_type } => {
                            // Filter predicate.
                            let value_domain = filter_domain.entry(left_column_id).or_default();

                            // If there is a domain of any for this column, replace it.
                            if value_domain.first().map(|d| d.is_any()).unwrap_or(false) {
                                value_domain.clear();
                            }

                            let domain = Self::parse_value(comparator, value, literal_type);
                            value_domain.push(domain);
                        },

                        Plan::ColumnReference { column: right_column } => {
                            // Join predicate.
                            let right_column_id = column_manager.get_column_id(
                                &right_column.table,
                                &right_column.name,
                            );
                            for column_id in &[left_column_id, right_column_id] {
                                let value_domain = filter_domain.entry(*column_id).or_default();
                                // Do not overwrite other domains that may have been parsed.
                                if value_domain.is_empty() {
                                    value_domain.push(Domain::any());
                                }
                            }
                        },

                        _ => panic!("Predicate lock only supports literals or column references on \
                        right side of comparison"),
                    }
                } else {
                    panic!("Predicate lock only supports columns on left side of comparison");
                }
            },
            _ => panic!("Invalid plan node type for filter ({:?})", filter),
        }
    }

    fn parse_value(comparator: Comparator, value_str: &str, data_type_str: &str) -> Domain {
        let data_type = DataType::from_str(data_type_str).unwrap();
        let value = data_type.parse(value_str).unwrap();
        Domain::new(comparator, value)
    }
}

impl PartialEq for Statement {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Statement {}

impl Hash for Statement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Borrow<u64> for Statement {
    fn borrow(&self) -> &u64 {
        &self.id
    }
}
