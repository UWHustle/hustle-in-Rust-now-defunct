use hustle_catalog::Column;
use hustle_types::{Bits, HustleType};

use crate::router::BlockPoolDestinationRouter;

pub fn new_destination_router(cols: &[Column]) -> BlockPoolDestinationRouter {
    let mut col_sizes = Vec::with_capacity(cols.len() + 1);

    // Include 2 bits to signify "valid" and "ready" states.
    let mut bits_len = 2;

    for col in cols {
        col_sizes.push(col.type_variant.byte_len());
        if col.nullable {
            // Include 1 bit to signify "null" state.
            bits_len += 1;
        }
    }

    // Append a hidden column to the end of the schema to store the bit flags.
    col_sizes.push(Bits::new(bits_len).byte_len());

    BlockPoolDestinationRouter::new(col_sizes)
}


