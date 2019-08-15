use byteorder::{ByteOrder, LittleEndian};
use crate::{Comparable, PrimitiveType, HustleType, HustleTypeVariant};

const INT_16_SIZE: usize = 2;

pub struct Int16 {
    buf: [u8; 2],
}

impl Int16 {
    pub fn new(n: i16) -> Self {
        let mut buf = [0; INT_16_SIZE];
        LittleEndian::write_i16(&mut buf, n);
        Int16 {
            buf,
        }
    }
}

impl HustleType for Int16 {
    fn variant(&self) -> HustleTypeVariant {
        HustleTypeVariant::Int16
    }

    fn size(&self) -> usize {
        INT_16_SIZE
    }
}

impl PrimitiveType for Int16 {
    type Primitive = i16;

    fn as_primitive(&self) -> Self::Primitive {
        unsafe { *(self.buf.as_ptr() as *const i16) }
    }
}

impl Comparable<Int16> for Int16 {
    fn eq(&self, other: &Int16) -> bool {
        self.as_primitive() == other.as_primitive()
    }

    fn lt(&self, other: &Int16) -> bool {
        self.as_primitive() < other.as_primitive()
    }
}
