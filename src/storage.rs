use crate::index::MutableSparseVectorIndex;
use crate::vector::{ScoredCandidate, SparseVector};
use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io::BufReader;

#[derive(Debug)]
pub struct SparseVectorStorage {
    sparse_vectors: Vec<Option<SparseVector>>, // ordered by id for quick access
    sparse_vector_index: MutableSparseVectorIndex, // position -> posting of vector ids
}

impl SparseVectorStorage {
    pub fn new() -> SparseVectorStorage {
        SparseVectorStorage {
            sparse_vectors: Vec::new(), //
            sparse_vector_index: MutableSparseVectorIndex::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn load_SPLADE_embeddings(path: &str) -> SparseVectorStorage {
        let f = File::open(path).unwrap();
        let reader = BufReader::new(f);
        // steam jsonl values
        let stream = Deserializer::from_reader(reader).into_iter::<Value>();

        let mut internal_index = 0;
        let mut storage = SparseVectorStorage::new();

        for value in stream {
            let value = value.expect("Unable to parse JSON");
            match value {
                Value::Object(map) => {
                    let keys_count = map.len();
                    let mut indices = Vec::with_capacity(keys_count);
                    let mut values = Vec::with_capacity(keys_count);
                    for (key, value) in map {
                        indices.push(key.parse::<usize>().unwrap());
                        values.push(value.as_f64().unwrap() as f32);
                    }
                    storage.add(internal_index, SparseVector::new(indices, values));
                    internal_index += 1;
                }
                _ => panic!("Unexpected value"),
            }
        }
        storage
    }

    pub fn add(&mut self, vector_id: usize, sparse_vector: SparseVector) {
        self.sparse_vector_index.add(vector_id, &sparse_vector);
        match self.sparse_vectors.get_mut(vector_id) {
            Some(current) => *current = Some(sparse_vector),
            None => {
                // out of bounds, resize and insert
                self.sparse_vectors.resize_with(vector_id + 1, || None);
                self.sparse_vectors[vector_id] = Some(sparse_vector);
            }
        }
    }

    /// Panics if vector_id is out of bounds
    pub fn get(&self, vector_id: usize) -> &Option<SparseVector> {
        match self.sparse_vectors.get(vector_id) {
            Some(sparse_vector) => sparse_vector,
            None => panic!("Vector storage not allocated for {}", vector_id),
        }
    }

    pub fn query_full_scan(
        &self,
        limit: usize,
        sparse_vector: &SparseVector,
    ) -> Vec<ScoredCandidate> {
        let mut scored_candidates: Vec<_> = self
            .sparse_vectors
            .iter()
            .enumerate()
            .filter_map(|(id, v)| v.as_ref().map(|v| (id, v)))
            .map(|(vector_id, vector)| {
                // sparse dot similarity
                let score = sparse_vector.dot_product(vector);
                ScoredCandidate {
                    score,
                    vector_id,
                    vector,
                }
            })
            .collect();

        // sort by score
        scored_candidates.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        // return top n
        scored_candidates.into_iter().take(limit).collect()
    }

    pub fn query_index(&self, limit: usize, sparse_vector: &SparseVector) -> Vec<ScoredCandidate> {
        let mut candidates = Vec::new();
        for index in &sparse_vector.indices {
            if let Some(posting) = self.sparse_vector_index.get(*index) {
                candidates.extend_from_slice(posting);
            }
        }
        // remove duplicates
        candidates.sort();
        candidates.dedup();
        // score candidates
        let mut scored_candidates: Vec<_> = candidates
            .into_iter()
            .filter_map(|vector_id| self.get(vector_id).as_ref().map(|v| (vector_id, v)))
            .map(|(vector_id, vector)| {
                // sparse dot similarity
                let score = sparse_vector.dot_product(vector);
                ScoredCandidate {
                    score,
                    vector_id,
                    vector,
                }
            })
            .collect();
        // sort by score
        scored_candidates.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        // return top n
        scored_candidates.into_iter().take(limit).collect()
    }

    pub fn print_index_statistics(&self) {
        let mut max_posting_list_size = 0;
        let mut max_posting_list_size_index = 0;

        let mut min_posting_list_size = usize::MAX;
        let mut min_posting_list_size_index = 0;

        for (k, v) in self.sparse_vector_index.map.iter() {
            let size = v.len();
            if size > max_posting_list_size {
                max_posting_list_size = size;
                max_posting_list_size_index = *k;
            }
            if size < min_posting_list_size {
                min_posting_list_size = size;
                min_posting_list_size_index = *k;
            }
        }
        println!("Index size: {} keys", self.sparse_vector_index.map.len());
        println!(
            "Max posting list size for key {} with {} vector ids",
            max_posting_list_size_index, max_posting_list_size
        );
        println!(
            "Min posting list size for key {} with {} vector ids",
            min_posting_list_size_index, min_posting_list_size
        );
    }

    pub fn print_data_statistics(&self) {
        let mut vector_count = 0;

        let mut max_index = 0;
        let mut max_value = 0.0;
        let mut min_index = usize::MAX;
        let mut min_value = f32::MAX;
        let mut max_length = 0;
        let mut min_length = usize::MAX;
        let mut sum_length = 0;
        for sparse_vector in self.sparse_vectors.iter().flatten() {
            let length = sparse_vector.indices.len();
            if length > max_length {
                max_length = length;
            }
            if length < min_length {
                min_length = length;
            }
            sum_length += length;
            for &index in &sparse_vector.indices {
                if index > max_index {
                    max_index = index;
                }
                if index < min_index {
                    min_index = index;
                }
            }
            for &value in &sparse_vector.values {
                if value > max_value {
                    max_value = value;
                }
                if value < min_value {
                    min_value = value;
                }
            }
            vector_count += 1;
        }
        println!("Data size: {} sparse vectors", vector_count);
        println!("Max sparse index: {}", max_index);
        println!("Min sparse index: {}", min_index);
        println!("Max sparse value: {}", max_value);
        println!("Min sparse value: {}", min_value);
        println!("Max sparse vector length: {}", max_length);
        println!("Min sparse length: {}", min_length);
        println!(
            "Avg sparse length: {}",
            sum_length as f64 / vector_count as f64
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::SparseVectorStorage;
    use crate::vector::SparseVector;
    use crate::SPLADE_DATA_PATH;

    #[test]
    fn search_equivalence() {
        let storage = SparseVectorStorage::load_SPLADE_embeddings(SPLADE_DATA_PATH);

        let limit = 10;
        let query = SparseVector::new(vec![0, 1000, 2000, 3000], vec![1.0, 0.2, 0.9, 0.5]);

        let full_scan_results = storage.query_full_scan(limit, &query);
        let index_results = storage.query_index(limit, &query);
        assert_eq!(full_scan_results, index_results);
    }
}
