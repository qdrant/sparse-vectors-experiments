use crate::vector::SparseVector;
use std::collections::HashMap;
use crate::sparse_index::types::{DimId, RecordId};

#[derive(Debug)]
pub struct MutableSparseVectorIndex {
    pub map: HashMap<DimId, Vec<RecordId>>,
}

impl MutableSparseVectorIndex {
    pub fn new() -> MutableSparseVectorIndex {
        MutableSparseVectorIndex {
            map: HashMap::new(),
        }
    }

    pub fn get(&self, index: &DimId) -> Option<&Vec<RecordId>> {
        self.map.get(index)
    }

    pub fn add(&mut self, vector_id: RecordId, sparse_vector: &SparseVector) {
        for index in &sparse_vector.indices {
            self.map
                .entry(*index)
                .or_insert(Vec::new()) // init if not exists
                .push(vector_id); // add vector id to posting list
        }
    }
}
