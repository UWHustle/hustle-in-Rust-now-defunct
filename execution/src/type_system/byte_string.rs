extern crate byteorder;

use self::byteorder::{ByteOrder, LittleEndian};

use super::*;

/// A sequence of bytes with a predefined max length
#[derive(Clone, Debug)]
pub struct ByteString {
    value: Vec<u8>,
    nullable: bool,
    is_null: bool,
    varchar: bool,
}

impl ByteString {
    pub fn new(buffer: &[u8], nullable: bool, max_size: usize, varchar: bool) -> Self {
        let mut buffer_sub = buffer;
        if buffer.len() > max_size {
            buffer_sub = &buffer[0..max_size];
        }
        let mut value: Vec<u8> = vec![0; buffer_sub.len()];
        value.clone_from_slice(buffer_sub);
        let pad_char = match varchar {
            true => 0x00,
            false => 0x20,
        };
        value.resize(max_size, pad_char);
        Self {
            value,
            nullable,
            is_null: false,
            varchar,
        }
    }

    pub fn from(string: &str) -> Self {
        Self::new(string.as_bytes(), true, string.len(), false)
    }

    pub fn create_null(max_size: usize, varchar: bool) -> Self {
        let fill_char = match varchar {
            true => 0x00,
            false => 0x20,
        };
        let value: Vec<u8> = vec![fill_char; max_size];
        Self {
            value,
            nullable: true,
            is_null: true,
            varchar,
        }
    }

    pub fn marshall(
        data: &[u8],
        nullable: bool,
        is_null: bool,
        max_size: usize,
        varchar: bool,
    ) -> Self {
        let mut output = Self::new(data, nullable, max_size, varchar);
        output.is_null = is_null;
        output
    }

    pub fn next_size(data: &[u8]) -> usize {
        LittleEndian::read_u64(&data[..8]) as usize
    }

    pub fn value(&self) -> &[u8] {
        if self.is_null {
            panic!(null_value(self.data_type()));
        }
        &self.value
    }

    fn trimmed_slice(&self) -> &[u8] {
        match self.varchar {
            true => {
                let mut end_idx = self.value.len();
                for i in 0..self.value.len() {
                    if self.value[i] == 0x00 {
                        end_idx = i;
                        break;
                    }
                }
                &self.value[0..end_idx]
            }
            false => &self.value,
        }
    }
}

impl Value for ByteString {
    fn data_type(&self) -> DataType {
        DataType::new(
            Variant::ByteString(self.value.len(), self.varchar),
            self.nullable,
        )
    }

    fn is_null(&self) -> bool {
        self.is_null
    }

    fn to_string(&self) -> String {
        if self.is_null {
            String::new()
        } else {
            String::from(
                std::str::from_utf8(self.trimmed_slice()).expect("Cannot convert to UTF-8 string"),
            )
        }
    }

    fn un_marshall(&self) -> OwnedBuffer {
        let mut value: Vec<u8> = vec![0; self.size()];
        value.clone_from_slice(&self.value);
        OwnedBuffer::new(value, self.data_type(), self.is_null())
    }

    fn compare(&self, other: &Value, comp: Comparator) -> bool {
        match other.data_type().variant {
            Variant::ByteString(_, _) => comp.apply(
                self.trimmed_slice(),
                cast_value::<ByteString>(other).trimmed_slice(),
            ),
            _ => {
                panic!(incomparable(self.data_type(), other.data_type()));
            }
        }
    }

    /// Returns the size of the string (overrides the default implementation)
    fn size(&self) -> usize {
        self.value.len()
    }
}

/* ============================================================================================== */

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn utf8_string_un_marshall() {
        let utf8_string_value = ByteString::from("Hello!");
        let utf8_string_buffer = utf8_string_value.un_marshall();
        assert_eq!(
            DataType::new(Variant::ByteString("Hello!".len(), false), true),
            utf8_string_buffer.data_type()
        );

        let data = utf8_string_buffer.data();
        assert_eq!('H' as u8, data[0]);
        assert_eq!('e' as u8, data[1]);
        assert_eq!('l' as u8, data[2]);
        assert_eq!('l' as u8, data[3]);
        assert_eq!('o' as u8, data[4]);
        assert_eq!('!' as u8, data[5]);
    }

    #[test]
    fn utf8_string_size() {
        let utf8_string = ByteString::from("Do no evil");
        assert_eq!(10, utf8_string.size());
    }

    #[test]
    fn utf8_string_type_id() {
        let utf8_string = ByteString::from("Chocolate donuts");
        assert_eq!(
            DataType::new(Variant::ByteString(16, false), true),
            utf8_string.data_type()
        );
    }

    #[test]
    fn utf8_string_compare() {
        let alphabet = ByteString::from("alphabet");

        let aardvark = ByteString::from("aardvark");
        assert!(!alphabet.less(&aardvark));
        assert!(alphabet.greater(&aardvark));
        assert!(!alphabet.equals(&aardvark));

        let elephant = ByteString::from("elephant");
        assert!(alphabet.less(&elephant));
        assert!(!alphabet.greater(&elephant));
        assert!(!alphabet.equals(&elephant));
    }

    #[test]
    #[should_panic]
    fn invalid_utf8_string_compare() {
        let alphabet = ByteString::from("alphabet");
        let int4 = Int4::from(1234);
        int4.equals(&alphabet);
    }
}
