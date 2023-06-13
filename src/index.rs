use crate::vector::SparseVector;
use std::collections::HashMap;

#[derive(Debug)]
pub struct MutableSparseVectorIndex {
    pub map: HashMap<usize, Vec<usize>>,
}

impl MutableSparseVectorIndex {
    pub fn new() -> MutableSparseVectorIndex {
        MutableSparseVectorIndex {
            map: HashMap::new(),
        }
    }

    pub fn get(&self, index: usize) -> Option<&Vec<usize>> {
        self.map.get(&index)
    }

    pub fn add(&mut self, vector_id: usize, sparse_vector: &SparseVector) {
        for index in &sparse_vector.indices {
            self.map
                .entry(*index)
                .or_insert(Vec::new()) // init if not exists
                .push(vector_id); // add vector id to posting list
        }
    }
}
