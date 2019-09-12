use hustle_types::{self as types, ComparativeVariant::{self, *}, TypeVariant};

#[derive(Clone, Debug)]
pub struct Domain(Option<(ComparativeVariant, TypeVariant, Vec<u8>)>);

impl Domain {
    pub fn new(
        comparative_variant: ComparativeVariant,
        type_variant: TypeVariant,
        buf: Vec<u8>,
    ) -> Self {
        Domain(Some((comparative_variant, type_variant, buf)))
    }

    pub fn any() -> Self {
        Domain(None)
    }

    pub fn is_any(&self) -> bool {
        self.0.is_none()
    }

    pub fn intersects(&self, other: &Self) -> bool {
        self.0.as_ref()
            .and_then(|(l_comparative_variant, l_type_variant, l_buf)| {
                match l_comparative_variant {
                    Eq => other.0.as_ref()
                        .map(|(r_comparative_variant, r_type_variant, r_buf)|
                            types::compare(
                                *r_comparative_variant,
                                l_type_variant,
                                r_type_variant,
                                l_buf,
                                r_buf
                            ).unwrap()
                        ),
                    Lt => other.0.as_ref()
                        .map(|(r_comparative_variant, r_type_variant, r_buf)|
                            match r_comparative_variant {
                                Eq | Gt | Ge => types::compare(
                                    Gt,
                                    l_type_variant,
                                    r_type_variant,
                                    l_buf,
                                    r_buf,
                                ).unwrap(),
                                Lt | Le => true,
                            }
                        ),
                    Le => other.0.as_ref()
                        .map(|(r_comparative_variant, r_type_variant, r_buf)|
                            match r_comparative_variant {
                                Eq | Ge => types::compare(
                                    Ge,
                                    l_type_variant,
                                    r_type_variant,
                                    l_buf,
                                    r_buf,
                                ).unwrap(),
                                Gt => types::compare(
                                    Gt,
                                    l_type_variant,
                                    r_type_variant,
                                    l_buf,
                                    r_buf,
                                ).unwrap(),
                                Lt | Le => true,
                            }
                        ),
                    Gt => other.0.as_ref()
                        .map(|(r_comparative_variant, r_type_variant, r_buf)|
                            match r_comparative_variant {
                                Eq | Lt | Le => types::compare(
                                    Lt,
                                    l_type_variant,
                                    r_type_variant,
                                    l_buf,
                                    r_buf,
                                ).unwrap(),
                                Gt | Ge => true,
                            }
                        ),
                    Ge => other.0.as_ref()
                        .map(|(r_comparative_variant, r_type_variant, r_buf)|
                            match r_comparative_variant {
                                Eq | Le => types::compare(
                                    Le,
                                    l_type_variant,
                                    r_type_variant,
                                    l_buf,
                                    r_buf,
                                ).unwrap(),
                                Lt => types::compare(
                                    Lt,
                                    l_type_variant,
                                    r_type_variant,
                                    l_buf,
                                    r_buf,
                                ).unwrap(),
                                Gt | Ge => true,
                            }
                        ),
                }
            }).unwrap_or(true)
    }
}

#[cfg(test)]
mod domain_tests {
    use hustle_types::Int8;

    use super::*;

    #[test]
    fn intersects() {
        // Construct the test domains.
        let mut ds = vec![];
        for cmp in &[Eq, Lt, Le, Gt, Ge] {
            for val in &[1, 2] {
                let int8_type = Int8;
                let buf = int8_type.new_buf(*val);
                ds.push(Domain::new(cmp.clone(), TypeVariant::Int8(int8_type), buf));
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
