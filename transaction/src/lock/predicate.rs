use std::collections::HashMap;

use hustle_common::Plan;
use hustle_types::data_type::DataType;
use hustle_types::operators::Comparator;

use crate::lock::{AccessMode, ValueLock};

pub struct PredicateLock {
    access_mode: AccessMode,
    value_locks: HashMap<String, HashMap<String, Vec<ValueLock>>>,
}

impl PredicateLock {
    pub fn from_plan(plan: &Plan) -> Self {
        let mut access_mode = AccessMode::Read;
        let mut value_locks = HashMap::new();
        Self::generate_value_locks(plan, &mut access_mode, &mut value_locks);
        PredicateLock {
            access_mode,
            value_locks,
        }
    }

    pub fn access_mode(&self) -> &AccessMode {
        &self.access_mode
    }

    pub fn conflicts(&self, other: &Self) -> bool {
        if self.access_mode == AccessMode::Read && other.access_mode == AccessMode::Read {
            false
        } else {
            self.value_locks.iter().any(|(self_table, self_columns)|
                other.value_locks.get(self_table)
                    .map(|other_columns|
                        self_columns.iter().any(|(self_column, self_locks)|
                            other_columns.get(self_column)
                                .map(|other_locks|
                                    self_locks.iter().any(|self_lock|
                                        other_locks.iter().any(|other_lock| self_lock.conflicts(other_lock))
                                    )
                                )
                                .unwrap_or(false)
                        )
                    )
                    .unwrap_or(false)
            )
        }
    }

    fn generate_value_locks(
        plan: &Plan,
        access_mode: &mut AccessMode,
        value_locks: &mut HashMap<String, HashMap<String, Vec<ValueLock>>>,
    ) {
        match plan {
            Plan::Delete { from_table, filter: _ } => {
                *access_mode = AccessMode::Write;

                // Create a new value lock that covers the entire domain of each column.
                let mut columns_map = HashMap::new();
                for column in &from_table.columns {
                    let value_lock = ValueLock::new(AccessMode::Write, None);
                    columns_map.insert(column.name.clone(), vec![value_lock]);
                }

                value_locks.insert(from_table.name.clone(), columns_map);
            },

            Plan::Insert { into_table, input } => {
                *access_mode = AccessMode::Write;

                // Create a new value lock with the value being inserted for each column.
                let mut columns_map = HashMap::new();
                if let Plan::Row { values } = &**input {
                    for (column, literal) in into_table.columns.iter().zip(values) {
                        if let Plan::Literal { value, literal_type } = literal {
                            let data_type = DataType::from_str(literal_type).unwrap();
                            let value = data_type.parse(value).unwrap();
                            let value_lock = ValueLock::new(
                                AccessMode::Write,
                                Some((Comparator::Eq, value)),
                            );
                            columns_map.insert(column.name.clone(), vec![value_lock]);
                        } else {
                            panic!("Predicate lock only supports inserting literal values")
                        }
                    }
                } else {
                    panic!("Predicate lock only supports inserting a row")
                }

                value_locks.insert(into_table.name.clone(), columns_map);
            },

            Plan::Project { table, projection } => {
                Self::generate_value_locks(&**table, access_mode, value_locks);
                for column_reference in projection {
                    if let Plan::ColumnReference { column } = column_reference {

                        // TODO: Replace this with raw_entry when it becomes stable.
                        // This will avoid unnecessary clones.
                        let locks = value_locks.entry(column.table.clone())
                            .or_default()
                            .entry(column.name.clone())
                            .or_default();

                        // Do not overwrite other value locks that may have been generated.
                        if locks.is_empty() {
                            locks.push(ValueLock::new(AccessMode::Read, None));
                        }
                    } else {
                        panic!("Predicate lock only supports column references in projection")
                    }
                }
            },

            Plan::Select { table, filter } => {
                Self::generate_value_locks(&**table, access_mode, value_locks);
                Self::generate_value_locks_from_filter(
                    &**filter,
                    &AccessMode::Read,
                    value_locks,
                    true,
                );
            },

            Plan::Update { table, columns, assignments: _, filter } => {
                *access_mode = AccessMode::Write;

                // Create a value lock that covers the entire domain of each column.
                let mut columns_map = HashMap::new();
                for column in columns {
                    let value_lock = ValueLock::new(AccessMode::Write, None);
                    columns_map.insert(
                        column.name.clone(),
                        vec![value_lock],
                    );
                }

                value_locks.insert(table.name.clone(), columns_map);

                filter.as_ref().map(|f| Self::generate_value_locks_from_filter(
                        &*f,
                        &AccessMode::Write,
                        value_locks,
                        false,
                ));
            },

            _ => (),
        }
    }

    fn generate_value_locks_from_filter(
        plan: &Plan,
        access_mode: &AccessMode,
        value_locks: &mut HashMap<String, HashMap<String, Vec<ValueLock>>>,
        overwrite: bool,
    ) {
        match plan {
            Plan::Connective { name: _, terms } => {
                for term in terms {
                    Self::generate_value_locks_from_filter(
                        term,
                        access_mode,
                        value_locks,
                        overwrite,
                    );
                }
            },

            Plan::Comparative { name, left, right } => {
                let comparator = Comparator::from_str(name).unwrap();
                if let Plan::ColumnReference { column } = &**left {
                    if let Plan::Literal { value, literal_type } = &**right {

                        // TODO: Replace this with raw_entry when it becomes stable.
                        // This will avoid unnecessary clones.
                        let value_locks = value_locks
                            .entry(column.table.clone())
                            .or_default()
                            .entry(column.name.clone()).or_default();

                        if overwrite {
                            value_locks.clear();
                        }

                        let data_type = DataType::from_str(literal_type).unwrap();
                        let value = data_type.parse(value).unwrap();
                        let domain = Some((comparator, value));
                        value_locks.push(ValueLock::new(access_mode.clone(), domain));
                    } else {
                        panic!("Predicate lock only supports literals on right side of comparison");
                    }
                } else {
                    panic!("Predicate lock only supports columns on left side of comparison");
                }
            },
            _ => panic!("Invalid plan node type for filter ({:?})", plan),
        }
    }
}

#[cfg(test)]
mod predicate_lock_tests {
    use util;

    use crate::lock::PredicateLock;

    #[test]
    fn delete_conflict() {
        let locks = generate_locks(&[
            "DELETE FROM T WHERE a = 1;",
            "SELECT b FROM T;",
        ]);
        assert!(locks[0].conflicts(&locks[1]));
    }

    #[test]
    fn insert_conflict() {
        let locks = generate_locks(&[
            "INSERT INTO T VALUES (1, 2, 3);",
            "SELECT a FROM T;",
        ]);
        assert!(locks[0].conflicts(&locks[1]));
    }

    #[test]
    fn insert_no_conflict() {
        let locks = generate_locks(&[
            "INSERT INTO T VALUES (1, 2, 3);",
            "SELECT a FROM T WHERE a > 1 AND b = 1 AND c <= 2;",
        ]);
        assert!(!locks[0].conflicts(&locks[1]));
    }

    #[test]
    fn project_no_conflict() {
        let locks = generate_locks(&[
            "SELECT a FROM T;",
            "SELECT a FROM T;",
        ]);
        assert!(!locks[0].conflicts(&locks[1]));
    }

    #[test]
    fn update_conflict() {
        let locks = generate_locks(&[
            "UPDATE T SET a = 1;",
            "SELECT a FROM T WHERE a = 3;",
        ]);
        assert!(locks[0].conflicts(&locks[1]));
    }

    #[test]
    fn update_no_conflict() {
        let locks = generate_locks(&[
            "SELECT a FROM T;",
            "UPDATE T SET b = 1;",
            "UPDATE T SET c = 1;",
        ]);
        assert!(!locks[0].conflicts(&locks[1]));
        assert!(!locks[0].conflicts(&locks[2]));
        assert!(!locks[1].conflicts(&locks[2]));
    }

    #[test]
    fn update_filter_conflict() {
        let locks = generate_locks(&[
            "SELECT a FROM T WHERE b = 1;",
            "UPDATE T SET a = 1 WHERE b = 2;",
        ]);
        assert!(locks[0].conflicts(&locks[1]));
    }

    #[test]
    fn update_same_column_filter_conflict() {
        let locks = generate_locks(&[
            "SELECT a FROM T WHERE b = 1;",
            "UPDATE T SET b = 2 WHERE b = 1;",
        ]);
        assert!(locks[0].conflicts(&locks[1]));
    }

    pub fn generate_locks(sqls: &[&str]) -> Vec<PredicateLock> {
        util::generate_plans(sqls)
            .iter()
            .map(|plan| PredicateLock::from_plan(&plan))
            .collect()
    }
}
