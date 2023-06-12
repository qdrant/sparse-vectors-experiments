use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug)]
pub struct SparseVectorStorage {
    sparse_vectors: Vec<Option<SparseVector>>, // ordered by id for quick access
    sparse_vector_index: SparseVectorIndex,    // position -> posting of vector ids
}

impl SparseVectorStorage {
    pub fn new() -> SparseVectorStorage {
        SparseVectorStorage {
            sparse_vectors: Vec::new(), //
            sparse_vector_index: SparseVectorIndex::default(),
        }
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

    pub fn query(&self, limit: usize, sparse_vector: &SparseVector) -> Vec<ScoredCandidate> {
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

#[derive(Debug, Default)]
struct SparseVectorIndex {
    map: HashMap<usize, Vec<usize>>,
}

impl SparseVectorIndex {
    pub fn get(&self, index: usize) -> Option<&Vec<usize>> {
        self.map.get(&index)
    }

    fn add(&mut self, vector_id: usize, sparse_vector: &SparseVector) {
        for index in &sparse_vector.indices {
            self.map
                .entry(*index)
                .or_insert(Vec::new()) // init if not exists
                .push(vector_id); // add vector id to posting list
        }
    }
}

#[derive(Debug)]
pub struct ScoredCandidate<'a> {
    pub score: f32,
    pub vector_id: usize,
    pub vector: &'a SparseVector,
}

#[derive(Debug)]
pub struct SparseVector {
    pub indices: Vec<usize>,
    pub values: Vec<f32>,
}

impl SparseVector {
    pub fn new(indices: Vec<usize>, values: Vec<f32>) -> SparseVector {
        SparseVector { indices, values }
    }

    pub fn dot_product(&self, other: &SparseVector) -> f32 {
        let mut result = 0.0;
        let mut i = 0;
        let mut j = 0;
        while i < self.indices.len() && j < other.indices.len() {
            match self.indices[i].cmp(&other.indices[j]) {
                Ordering::Less => {
                    // move forward in self
                    i += 1;
                }
                Ordering::Equal => {
                    // dot product
                    result += self.values[i] * other.values[j];
                    i += 1;
                    j += 1;
                }
                Ordering::Greater => {
                    // move forward in other
                    j += 1;
                }
            }
        }
        result
    }
}
