use buffer_manager::BufferManager;
use relational_block::{RelationalBlock, RowBuilder};
use relational_storage_engine::RelationalStorageEngine;
use std::cmp::min;
use std::rc::Rc;

pub struct PhysicalRelation<'a> {
    key: &'a str,
    schema: Vec<usize>,
    buffer_manager: Rc<BufferManager>,
}

impl<'a> PhysicalRelation<'a> {
    pub fn new(key: &'a str, schema: Vec<usize>, buffer_manager: Rc<BufferManager>) -> Self {
        let key_for_new_block = RelationalStorageEngine::formatted_key_for_block(key, 0);
        RelationalBlock::new(&key_for_new_block, &schema, &buffer_manager);
        PhysicalRelation {
            key,
            schema,
            buffer_manager
        }
    }

    pub fn get_block(&self, block_index: usize) -> Option<RelationalBlock> {
        let key = RelationalStorageEngine::formatted_key_for_block(self.key, block_index);
        self.buffer_manager.get(&key)
    }

    pub fn blocks(&self) -> RelationalBlockIter {
        RelationalBlockIter::new(self)
    }

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

    pub fn bulk_read(&self) -> Vec<u8> {
        let mut result = vec![];
        for block in self.blocks() {
            result.extend(block.bulk_read());
        }
        result
    }

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

pub struct RelationalBlockIter<'a> {
    relation: &'a PhysicalRelation<'a>,
    block_index: usize
}

impl<'a> RelationalBlockIter<'a> {
    fn new(relation: &'a PhysicalRelation) -> Self {
        RelationalBlockIter {
            relation,
            block_index: 0
        }
    }
}

impl<'a> Iterator for RelationalBlockIter<'a> {
    type Item = RelationalBlock;

    fn next(&mut self) -> Option<RelationalBlock> {
        let block = self.relation.get_block(self.block_index);
        self.block_index += 1;
        block
    }
}
