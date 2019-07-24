use hustle_types::operators::Comparator::{self, *};
use hustle_types::Value;

#[derive(Clone, Debug)]
pub struct Domain {
    domain: Option<(Comparator, Box<Value>)>,
}

impl Domain {
    pub fn new(comparator: Comparator, value: Box<Value>) -> Self {
        Self::with_domain(Some((comparator, value)))
    }

    pub fn any() -> Self {
        Self::with_domain(None)
    }

    pub fn is_any(&self) -> bool {
        self.domain.is_none()
    }

    fn with_domain(domain: Option<(Comparator, Box<Value>)>) -> Self {
        Domain {
            domain
        }
    }

    pub fn intersects(&self, other: &Self) -> bool {
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

#[cfg(test)]
mod domain_tests {
    use hustle_types::integer::Int1;

    use super::*;

    #[test]
    fn intersects() {
        // Construct the test domains.
        let mut ds = vec![];
        for cmp in &[Eq, Lt, Le, Gt, Ge] {
            for value in &[1, 2] {
                let v = Box::new(Int1::new(*value, false)) as Box<Value + Send>;
                ds.push(Domain::new(cmp.clone(), v));
            }
        }

        // (= 1) and (= 1) conflict.
        assert!(ds[0].intersects(&ds[0]));

        // (= 1) and (= 2) do not conflict.
        assert!(!ds[0].intersects(&ds[1]));

        // (= 1) and (< 1) do not conflict.
        assert!(!ds[0].intersects(&ds[2]));

        // (= 1) and (< 2) conflict.
        assert!(ds[0].intersects(&ds[3]));

        // (= 1) and (≤ 1) conflict.
        assert!(ds[0].intersects(&ds[4]));

        // (= 1) and (≤ 2) conflict.
        assert!(ds[0].intersects(&ds[5]));

        // (= 1) and (> 1) do not conflict.
        assert!(!ds[0].intersects(&ds[6]));

        // (= 1) and (> 2) do not conflict.
        assert!(!ds[0].intersects(&ds[7]));

        // (= 1) and (≥ 1) conflict.
        assert!(ds[0].intersects(&ds[8]));

        // (= 1) and (≥ 2) do not conflict.
        assert!(!ds[0].intersects(&ds[9]));

        // (< 1) and (< 1) conflict.
        assert!(ds[2].intersects(&ds[2]));

        // (< 1) and (< 2) conflict.
        assert!(ds[2].intersects(&ds[3]));

        // (< 1) and (≤ 1) conflict.
        assert!(ds[2].intersects(&ds[4]));

        // (< 1) and (≤ 2) conflict.
        assert!(ds[2].intersects(&ds[5]));

        // (< 1) and (> 1) do not conflict.
        assert!(!ds[2].intersects(&ds[6]));

        // (< 1) and (> 2) do not conflict.
        assert!(!ds[2].intersects(&ds[7]));

        // (< 1) and (≥ 1) do not conflict.
        assert!(!ds[2].intersects(&ds[8]));

        // (< 1) and (≥ 2) do not conflict.
        assert!(!ds[2].intersects(&ds[9]));

        // (≤ 1) and (≤ 1) conflict.
        assert!(ds[4].intersects(&ds[4]));

        // (≤ 1) and (≤ 2) conflict.
        assert!(ds[4].intersects(&ds[5]));

        // (≤ 1) and (> 1) do not conflict.
        assert!(!ds[4].intersects(&ds[6]));

        // (≤ 1) and (> 2) do not conflict.
        assert!(!ds[4].intersects(&ds[7]));

        // (≤ 1) and (≥ 1) conflict.
        assert!(ds[4].intersects(&ds[8]));

        // (≤ 1) and (≥ 2) do not conflict.
        assert!(!ds[4].intersects(&ds[9]));

        // (> 1) and (> 1) conflict.
        assert!(ds[6].intersects(&ds[6]));

        // (> 1) and (> 2) conflict.
        assert!(ds[6].intersects(&ds[7]));

        // (> 1) and (≥ 1) conflict.
        assert!(ds[6].intersects(&ds[8]));

        // (> 1) and (≥ 2) conflict.
        assert!(ds[6].intersects(&ds[9]));

        // (≥ 1) and (≥ 1) conflict.
        assert!(ds[8].intersects(&ds[8]));

        // (≥ 1) and (≥ 2) conflict.
        assert!(ds[8].intersects(&ds[9]));

        // A domain of "any" conflicts with everything.
        let vl_any = Domain::any();
        for vl in ds {
            assert!(vl_any.intersects(&vl));
        }
    }
}
