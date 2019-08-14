pub trait BatchFunction {
    fn apply(&self, rows: impl Iterator<Vec<u8>>);
}

pub struct LessThan {

}

impl BatchFunction for LessThan {
    fn apply(&self, rows: impl Iterator<Vec<u8>>) {

    }
}
