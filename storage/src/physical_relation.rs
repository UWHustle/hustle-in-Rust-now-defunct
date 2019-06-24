use buffer_manager::BufferManager;
use relational_block::{RelationalBlock, RowBuilder};
use relational_storage_engine::RelationalStorageEngine;
use std::cmp::min;
use std::sync::Arc;

/// A physical relation backed by one or more blocks on storage.
pub struct PhysicalRelation<'a> {
    key: &'a str,
    schema: Vec<usize>,
    buffer_manager: Arc<BufferManager>,
}

impl<'a> PhysicalRelation<'a> {
    pub fn new(key: &'a str, schema: Vec<usize>, buffer_manager: Arc<BufferManager>) -> Self {
        let key_for_new_block = RelationalStorageEngine::formatted_key_for_block(key, 0);
        RelationalBlock::new(&key_for_new_block, &schema, &buffer_manager);
        PhysicalRelation {
            key,
            schema,
            buffer_manager
        }
    }

    /// Creates a new `PhysicalRelation` from an existing block, if it exists.
    pub fn try_from_block(key: &'a str, buffer_manager: Arc<BufferManager>) -> Option<Self> {
        let key_for_first_block = RelationalStorageEngine::formatted_key_for_block(key, 0);
        buffer_manager.get(&key_for_first_block)
            .map(|block| {
                let mut schema = vec![];
                schema.extend_from_slice(block.get_schema());
                PhysicalRelation {
                    key,
                    schema,
                    buffer_manager
                }
            })
    }

    /// Gets the `RelationalBlock` at the specified `block_index`.
    pub fn get_block(&self, block_index: usize) -> Option<RelationalBlock> {
        let key = RelationalStorageEngine::formatted_key_for_block(self.key, block_index);
        self.buffer_manager.get(&key)
    }

    /// Returns an iterator over the blocks of the `PhyscialRelation`.
    pub fn blocks(&self) -> BlockIter {
        BlockIter::new(self)
    }

    /// Returns a `RowBuilder` that is used to insert a row into the `PhysicalRelation`. The row is
    /// inserted into the first block that has free space.
    pub fn insert_row(&self) -> RowBuilder {
        let mut block_index = 0;
        for mut block in self.blocks() {
            if block.get_n_rows() < block.get_row_capacity() {
                return block.insert_row()
            }
            block_index += 1
        }

        let key_for_new_block = RelationalStorageEngine::formatted_key_for_block(self.key, block_index);
        let mut block = RelationalBlock::new(
            &key_for_new_block,
            &self.schema,
            &self.buffer_manager);
        block.insert_row()
    }

    /// Deletes all rows in the `PhysicalRelation`, but maintains the relation itself by keeping
    /// the first block on storage.
    pub fn clear(&self) {

        let first_block = self.get_block(0)
            .expect("Could not find block 0 for relation.");
        first_block.clear();

        let mut block_index = 1;
        let mut key_for_block = RelationalStorageEngine::formatted_key_for_block(
            self.key, block_index);
        while self.buffer_manager.exists(&key_for_block) {
            self.buffer_manager.erase(&key_for_block);
            block_index += 1;
            key_for_block = RelationalStorageEngine::formatted_key_for_block(self.key, block_index);
        }
    }

    /// Returns the entire data of the `PhysicalRelation`, concatenated into a vector of bytes in
    /// row-major format.
    pub fn bulk_read(&self) -> Vec<u8> {
        let mut result = vec![];
        for block in self.blocks() {
            result.extend(block.bulk_read());
        }
        result
    }

    /// Overwrites the entire `PhysicalRelation` with the `value`, which must be in row-major
    /// format.
    pub fn bulk_write(&self, value: &[u8]) {
        let mut block_index = 0;
        let mut offset = 0;
        while offset < value.len() {
            let ref mut block = self.get_block(block_index)
                .unwrap_or_else(|| RelationalBlock::new(
                    &RelationalStorageEngine::formatted_key_for_block(self.key, block_index),
                    &self.schema,
                    &self.buffer_manager
                ));
            let size = min(block.get_row_size() * block.get_row_capacity(), value.len() - offset);
            block.bulk_write(&value[offset..offset + size]);
            block_index += 1;
            offset += size;
        }
    }
}

pub struct BlockIter<'a> {
    relation: &'a PhysicalRelation<'a>,
    block_index: usize
}

impl<'a> BlockIter<'a> {
    fn new(relation: &'a PhysicalRelation) -> Self {
        BlockIter {
            relation,
            block_index: 0
        }
    }
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = RelationalBlock;

    fn next(&mut self) -> Option<RelationalBlock> {
        let block = self.relation.get_block(self.block_index);
        self.block_index += 1;
        block
    }
}
