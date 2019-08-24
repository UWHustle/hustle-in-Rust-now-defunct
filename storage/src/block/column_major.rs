use std::io::Cursor;
use std::iter::{FromIterator, StepBy, Take, Zip};
use std::ops::{Range, RangeFrom};
use std::slice;

use bit_vec::BitVec;
use memmap::MmapMut;

use block::latch::{Latch, LatchGuard};
use hustle_types::{Bits, HustleType};

pub struct RowMask {
    bits: BitVec
}

#[derive(Serialize, Deserialize)]
struct Header {
    col_sizes: Vec<usize>,
    n_flags: usize,
}

struct Metadata {
    n_rows: usize,
    row_cap: usize,
    col_offsets: Vec<usize>,
    bits_type: Bits,
}

pub struct ColumnMajorBlock {
    header: Header,
    metadata: Metadata,
    latch: Latch,
    data: *mut u8,
    _mmap: MmapMut,
}

impl ColumnMajorBlock {
    pub fn new(col_sizes: Vec<usize>, n_flags: usize, latch: Latch, mut mmap: MmapMut) -> Self {
        assert!(!col_sizes.is_empty(), "Cannot create a block with no columns");

        let header = Header { col_sizes, n_flags };

        let data_offset = {
            let mut cursor = Cursor::new(mmap.as_mut());
            serde_json::to_writer(&mut cursor, &header).unwrap();
            cursor.position() as usize
        };

        Self::with_header(header, data_offset, latch, mmap)
    }

    pub fn from_buf(latch: Latch, mut mmap: MmapMut) -> Self {
        let (header, data_offset) = {
            let mut cursor = Cursor::new(mmap.as_mut());
            let header = serde_json::from_reader(&mut cursor).unwrap();
            (header, cursor.position() as usize)
        };

        Self::with_header(header, data_offset, latch, mmap)
    }

    fn with_header(
        mut header: Header,
        data_offset: usize,
        latch: Latch,
        mut mmap: MmapMut
    ) -> Self {
        let (data, data_len) = {
            let data = &mut mmap[data_offset..];
            (data.as_mut_ptr(), data.len())
        };

        // Include 2 bits to signify "valid" and "ready" states.
        let bits_len = header.n_flags + 2;

        // Append a hidden column to the end of the schema to store the bit flags.
        let bits_type = Bits::new(bits_len);
        header.col_sizes.push(bits_type.byte_len());

        let row_size = header.col_sizes.iter().sum::<usize>() + bits_type.byte_len();
        let row_cap = data_len / row_size;

        let col_offsets = header.col_sizes.iter()
            .scan(0, |state, &col_size| {
                let col_offset = *state;
                *state = col_offset + row_cap * col_size;
                Some(col_offset)
            })
            .collect::<Vec<usize>>();

        let metadata = Metadata { n_rows: 0, row_cap, col_offsets, bits_type };

        ColumnMajorBlock {
            header,
            metadata,
            latch,
            data,
            _mmap: mmap,
        }
    }

    pub fn insert_row<'a>(&self, row: impl Iterator<Item = &'a [u8]>) {
        let _guard = self.latch.write();

        let (row_i, _) = self.get_valid()
            .zip(self.get_ready())
            .enumerate()
            .find(|&(_, (valid, ready))| valid && ready)
            .unwrap();

        for (dst_buf, src_buf) in RowIterMut::new(row_i, self).zip(row) {
            dst_buf.copy_from_slice(src_buf);
        }
    }

    pub fn insert_rows<'a>(&'a self, rows: impl Iterator<Item = impl Iterator<Item = &'a [u8]>>) {
        let _guard = self.latch.write();

        for ((row_i, _), row) in self.get_valid()
            .zip(self.get_ready())
            .enumerate()
            .filter(|&(_, (valid, ready))| valid && ready)
            .zip(rows)
        {
            for (dst_buf, src_buf) in RowIterMut::new(row_i, self).zip(row) {
                dst_buf.copy_from_slice(src_buf);
            }
        }
    }

    pub fn filter_col(&self, col_i: usize, f: impl Fn(&[u8]) -> bool) -> RowMask {
        let _guard = self.latch.read();

        let bits = BitVec::from_iter(
            self.get_valid()
                .zip(self.get_ready())
                .zip(ColIter::new(col_i, self))
                .map(|((valid, ready), buf)| valid && ready && f(buf))
        );

        RowMask { bits }
    }

    pub fn get_rows_with_mask(&self, mask: RowMask) -> RowMajorIter {
        let guard = self.latch.read();
        RowMajorIter::new(mask, self, guard)
    }

    pub fn get_rows(&self) -> RowMajorIter {
        let guard = self.latch.read();
        let mask = RowMask {
            bits: BitVec::from_iter(
                self.get_valid()
                    .zip(self.get_ready())
                    .map(|(valid, ready)| valid && ready)
            )
        };
        RowMajorIter::new(mask, self, guard)
    }

    pub fn delete_rows_with_mask(&self, mask: RowMask) {
        let _guard = self.latch.write();

        for (buf, _) in ColIter::new(self.header.col_sizes.len(), self)
            .zip(mask.bits.iter())
            .filter(|&(_, bit)| bit)
        {
            self.metadata.bits_type.set(self.valid_flag_i(), false, buf);
        }
    }

    pub fn delete_rows(&self) {
        let _guard = self.latch.write();

        for buf in ColIter::new(self.header.col_sizes.len(), self) {
            self.metadata.bits_type.set(self.valid_flag_i(), false, buf);
        }
    }

    pub fn update_col(&self, col_i: usize, value: &[u8]) {
        let _guard = self.latch.write();

        for buf in ColIter::new(col_i, self) {
            buf.copy_from_slice(value);
        }
    }

    pub fn update_col_with_mask(&self, col_i: usize, value: &[u8], mask: RowMask) {
        let _guard = self.latch.write();

        for (buf, _) in ColIter::new(col_i, self)
            .zip(mask.bits.iter())
            .filter(|&(_, bit)| bit)
        {
            buf.copy_from_slice(value);
        }
    }

    fn get_valid(&self) -> FlagIter {
        FlagIter::new(self.valid_flag_i(), self)
    }

    fn get_ready(&self) -> FlagIter {
        FlagIter::new(self.ready_flag_i(), self)
    }

    fn valid_flag_i(&self) -> usize {
        self.header.n_flags
    }

    fn ready_flag_i(&self) -> usize {
        self.header.n_flags + 1
    }
}

struct RowIterMut<'a> {
    row_i: usize,
    inner: Zip<slice::Iter<'a, usize>, slice::Iter<'a, usize>>,
    block: &'a ColumnMajorBlock,
}

impl<'a> RowIterMut<'a> {
    fn new(row_i: usize, block: &'a ColumnMajorBlock) -> Self {
        let inner = block.metadata.col_offsets.iter().zip(&block.header.col_sizes);
        RowIterMut {
            row_i,
            inner,
            block,
        }
    }
}

impl<'a> Iterator for RowIterMut<'a> {
    type Item = &'a mut [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
            .map(move |(&col_offset, &col_size)| {
                let offset = col_offset + self.row_i * col_size;
                unsafe {
                    slice::from_raw_parts_mut(self.block.data.offset(offset as isize), col_size)
                }
            })
    }
}

pub struct RowIter<'a> {
    inner: RowIterMut<'a>,
}

impl<'a> RowIter<'a> {
    fn new(row_i: usize, block: &'a ColumnMajorBlock) -> Self {
        let inner = RowIterMut::new(row_i, block);
        RowIter {
            inner,
        }
    }
}

impl<'a> Iterator for RowIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|v| &v[..])
    }
}

pub struct ColIter<'a> {
    inner: Take<StepBy<RangeFrom<usize>>>,
    col_size: usize,
    block: &'a ColumnMajorBlock,
}

impl<'a> ColIter<'a> {
    fn new(col_i: usize, block: &'a ColumnMajorBlock) -> Self {
        let col_size = block.header.col_sizes[col_i];
        let inner = (block.metadata.col_offsets[col_i]..)
            .step_by(col_size)
            .take(block.metadata.row_cap);

        ColIter {
            inner,
            col_size,
            block,
        }
    }
}

impl<'a> Iterator for ColIter<'a> {
    type Item = &'a mut [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
            .map(|offset| unsafe {
                slice::from_raw_parts_mut(self.block.data.offset(offset as isize), self.col_size)
            })
    }
}

pub struct FlagIter<'a> {
    inner: ColIter<'a>,
    flag_i: usize,
    block: &'a ColumnMajorBlock,
}

impl<'a> FlagIter<'a> {
    fn new(flag_i: usize, block: &'a ColumnMajorBlock) -> Self {
        let inner = ColIter::new(block.header.col_sizes.len(), block);
        FlagIter {
            inner,
            flag_i,
            block,
        }
    }
}

impl<'a> Iterator for FlagIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|buf| self.block.metadata.bits_type.get(self.flag_i, buf))
    }
}

pub struct RowMajorIter<'a> {
    inner: Zip<Range<usize>, bit_vec::IntoIter>,
    block: &'a ColumnMajorBlock,
    _guard: LatchGuard<'a>,
}

impl<'a> RowMajorIter<'a> {
    fn new(mask: RowMask, block: &'a ColumnMajorBlock, guard: LatchGuard<'a>) -> Self {
        let inner = (0..block.metadata.n_rows).zip(mask.bits.into_iter());
        RowMajorIter {
            inner,
            block,
            _guard: guard,
        }
    }
}

impl<'a> Iterator for RowMajorIter<'a> {
    type Item = RowIter<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((row_i, bit)) = self.inner.next() {
            if bit {
                return Some(RowIter::new(row_i, self.block))
            }
        }
        None
    }
}
