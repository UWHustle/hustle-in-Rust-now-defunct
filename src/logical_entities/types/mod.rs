
pub mod integer;

pub trait DataType:Sized {
    fn get_size() -> usize;
}