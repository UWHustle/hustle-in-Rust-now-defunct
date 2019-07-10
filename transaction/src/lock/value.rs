use hustle_types::operators::Comparator::{self, *};
use hustle_types::Value;

use crate::lock::AccessMode::{self, *};

pub struct ValueLock {
    access_mode: AccessMode,
    domain: Option<(Comparator, Box<Value + Send>)>,
}

impl ValueLock {
    pub fn new(
        access_mode: AccessMode,
        domain: Option<(Comparator, Box<Value + Send>)>,
    ) -> Self {
        ValueLock {
            access_mode,
            domain,
        }
    }

    pub fn conflicts(&self, other: &Self) -> bool {
        if self.access_mode == Read && other.access_mode == Read {
            // Both locks are readers.
            false
        } else {
            // At least one lock is a writer. Determine whether the domains of the locks intersect.
            self.domain.as_ref().and_then(|(self_comparator, self_value)| {
                match self_comparator {
                    Eq => other.domain.as_ref().map(|(other_comparator, other_value)|
                        self_value.compare(&**other_value, other_comparator.clone())
                    ),
                    Lt => other.domain.as_ref().map(|(other_comparator, other_value)|
                        match other_comparator {
                            Eq | Gt | Ge => self_value.compare(&**other_value, Gt),
                            Lt | Le => true,
                        }
                    ),
                    Le => other.domain.as_ref().map(|(other_comparator, other_value)|
                        match other_comparator {
                            Eq | Ge => self_value.compare(&**other_value, Ge),
                            Gt => self_value.compare(&**other_value, Gt),
                            Lt | Le => true,
                        }
                    ),
                    Gt => other.domain.as_ref().map(|(other_comparator, other_value)|
                        match other_comparator {
                            Eq | Lt | Le => self_value.compare(&**other_value, Lt),
                            Gt | Ge => true,
                        }
                    ),
                    Ge => other.domain.as_ref().map(|(other_comparator, other_value)|
                        match other_comparator {
                            Eq | Le => self_value.compare(&**other_value, Le),
                            Lt => self_value.compare(&**other_value, Lt),
                            Gt | Ge => true,
                        }
                    ),
                }
            }).unwrap_or(true)
        }
    }
}

#[cfg(test)]
mod value_lock_tests {
    use hustle_types::integer::Int1;

    use super::*;

    #[test]
    fn read() {
        // Ensure that readers with intersecting domains do not conflict.
        let vl_1 = new_value_lock(Read, Some((Eq, 1)));
        let vl_2 = new_value_lock(Read, Some((Eq, 1)));
        assert!(!vl_1.conflicts(&vl_2));
    }

    #[test]
    fn write() {
        // Construct the test value locks.
        let mut vls = vec![];
        for cmp in &[Eq, Lt, Le, Gt, Ge] {
            for value in &[1, 2] {
                vls.push(new_value_lock(Write, Some((cmp.clone(), *value))));
            }
        }

        // Ensure that writers with intersecting domains conflict.
        // (= 1) and (= 1) conflict.
        assert!(vls[0].conflicts(&vls[0]));

        // (= 1) and (= 2) do not conflict.
        assert!(!vls[0].conflicts(&vls[1]));

        // (= 1) and (< 1) do not conflict.
        assert!(!vls[0].conflicts(&vls[2]));

        // (= 1) and (< 2) conflict.
        assert!(vls[0].conflicts(&vls[3]));

        // (= 1) and (≤ 1) conflict.
        assert!(vls[0].conflicts(&vls[4]));

        // (= 1) and (≤ 2) conflict.
        assert!(vls[0].conflicts(&vls[5]));

        // (= 1) and (> 1) do not conflict.
        assert!(!vls[0].conflicts(&vls[6]));

        // (= 1) and (> 2) do not conflict.
        assert!(!vls[0].conflicts(&vls[7]));

        // (= 1) and (≥ 1) conflict.
        assert!(vls[0].conflicts(&vls[8]));

        // (= 1) and (≥ 2) do not conflict.
        assert!(!vls[0].conflicts(&vls[9]));

        // (< 1) and (< 1) conflict.
        assert!(vls[2].conflicts(&vls[2]));

        // (< 1) and (< 2) conflict.
        assert!(vls[2].conflicts(&vls[3]));

        // (< 1) and (≤ 1) conflict.
        assert!(vls[2].conflicts(&vls[4]));

        // (< 1) and (≤ 2) conflict.
        assert!(vls[2].conflicts(&vls[5]));

        // (< 1) and (> 1) do not conflict.
        assert!(!vls[2].conflicts(&vls[6]));

        // (< 1) and (> 2) do not conflict.
        assert!(!vls[2].conflicts(&vls[7]));

        // (< 1) and (≥ 1) do not conflict.
        assert!(!vls[2].conflicts(&vls[8]));

        // (< 1) and (≥ 2) do not conflict.
        assert!(!vls[2].conflicts(&vls[9]));

        // (≤ 1) and (≤ 1) conflict.
        assert!(vls[4].conflicts(&vls[4]));

        // (≤ 1) and (≤ 2) conflict.
        assert!(vls[4].conflicts(&vls[5]));

        // (≤ 1) and (> 1) do not conflict.
        assert!(!vls[4].conflicts(&vls[6]));

        // (≤ 1) and (> 2) do not conflict.
        assert!(!vls[4].conflicts(&vls[7]));

        // (≤ 1) and (≥ 1) conflict.
        assert!(vls[4].conflicts(&vls[8]));

        // (≤ 1) and (≥ 2) do not conflict.
        assert!(!vls[4].conflicts(&vls[9]));

        // (> 1) and (> 1) conflict.
        assert!(vls[6].conflicts(&vls[6]));

        // (> 1) and (> 2) conflict.
        assert!(vls[6].conflicts(&vls[7]));

        // (> 1) and (≥ 1) conflict.
        assert!(vls[6].conflicts(&vls[8]));

        // (> 1) and (≥ 2) conflict.
        assert!(vls[6].conflicts(&vls[9]));

        // (≥ 1) and (≥ 1) conflict.
        assert!(vls[8].conflicts(&vls[8]));

        // (≥ 1) and (≥ 2) conflict.
        assert!(vls[8].conflicts(&vls[9]));

        // A value lock with a domain of "any" conflicts with everything.
        let vl_any = new_value_lock(Write, None);
        for vl in vls {
            assert!(vl_any.conflicts(&vl));
        }
    }

    fn new_value_lock(
        access_mode: AccessMode,
        mut domain: Option<(Comparator, u8)>,
    ) -> ValueLock {
        ValueLock::new(
            access_mode,
            domain.take().map(|(cmp, value)|
                (cmp, Box::new(Int1::new(value, false)) as Box<Value + Send>)
            )
        )
    }
}
