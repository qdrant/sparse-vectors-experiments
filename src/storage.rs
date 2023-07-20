use crate::sparse_index::common::scored_candidate::ScoredCandidate;
use crate::sparse_index::common::types::RecordId;
use crate::sparse_index::common::vector::SparseVector;
use crate::sparse_index::immutable::inverted_index::{InvertedIndex, InvertedIndexBuilder};
use crate::sparse_index::immutable::posting_list::PostingBuilder;
use crate::sparse_index::immutable::search_context::SearchContext;
use ordered_float::OrderedFloat;
use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io::BufReader;

use crate::sparse_index::mutable::mutable_index::MutableSparseVectorIndex;

pub struct SparseVectorStorage {
    vectors: Vec<Option<SparseVector>>, // ordered by id for quick access
    mutable_index: MutableSparseVectorIndex, // position -> posting of vector ids
    immutable_index: Option<InvertedIndex>,
}

impl SparseVectorStorage {
    pub fn new() -> SparseVectorStorage {
        SparseVectorStorage {
            vectors: Vec::new(),
            mutable_index: MutableSparseVectorIndex::new(),
            immutable_index: None,
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
                        indices.push(key.parse::<u32>().unwrap());
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

    /// No upserts allowed
    pub fn add(&mut self, vector_id: usize, sparse_vector: SparseVector) {
        self.mutable_index
            .add(vector_id as RecordId, &sparse_vector);
        match self.vectors.get_mut(vector_id) {
            Some(_current) => panic!("Vector {} already exists", vector_id),
            None => {
                // out of bounds, resize and insert
                self.vectors.resize_with(vector_id + 1, || None);
                self.vectors[vector_id] = Some(sparse_vector);
            }
        }
    }

    /// Build immutable index from mutable index
    pub fn build_immutable_index(&mut self) {
        let mut inverted_index_builder = InvertedIndexBuilder::new();
        for (position, vector_ids) in self.mutable_index.map.iter() {
            let mut posting_list_builder = PostingBuilder::new();
            for vec_id in vector_ids {
                // get vector from storage
                let sparse_vector = self.get(*vec_id).as_ref().expect("Vector not found");
                if let Some(offset) = sparse_vector.indices.iter().position(|x| x == position) {
                    let weight = sparse_vector.weights[offset];
                    posting_list_builder.add(*vec_id as RecordId, weight);
                } else {
                    panic!("Vector {} does not contain position {}", vec_id, position);
                }
            }
            inverted_index_builder.add(*position, posting_list_builder.build());
        }
        self.immutable_index = Some(inverted_index_builder.build());
    }

    /// Panics if vector_id is out of bounds
    pub fn get(&self, vector_id: RecordId) -> &Option<SparseVector> {
        match self.vectors.get(vector_id as usize) {
            Some(sparse_vector) => sparse_vector,
            None => panic!("Vector storage not allocated for {}", vector_id),
        }
    }

    pub fn query_full_scan(
        &self,
        limit: usize,
        query_vector: &SparseVector,
    ) -> Vec<ScoredCandidate> {
        let mut scored_candidates: Vec<_> = self
            .vectors
            .iter()
            .enumerate()
            .filter_map(|(id, v)| v.as_ref().map(|v| (id, v)))
            .map(|(vector_id, vector)| {
                // sparse dot similarity
                let score = query_vector.dot_product(vector);
                ScoredCandidate {
                    score,
                    vector_id: vector_id as RecordId,
                }
            })
            .collect();

        // sort by score descending
        scored_candidates.sort_by(|a, b| OrderedFloat(b.score).cmp(&OrderedFloat(a.score)));
        // return top n
        scored_candidates.into_iter().take(limit).collect()
    }

    pub fn query_mutable_index(
        &self,
        top: usize,
        query_vector: &SparseVector,
    ) -> Vec<ScoredCandidate> {
        let mut candidates = Vec::new();
        for index in &query_vector.indices {
            if let Some(posting) = self.mutable_index.get(index) {
                candidates.extend_from_slice(posting);
            }
        }
        // remove duplicates
        candidates.sort();
        candidates.dedup();
        // score candidates
        let mut scored_candidates: Vec<_> = candidates
            .into_iter()
            .map(|vector_id| {
                let vector = self
                    .get(vector_id)
                    .as_ref()
                    .expect("must be found in storage");
                // sparse dot similarity
                let score = query_vector.dot_product(vector);
                ScoredCandidate { score, vector_id }
            })
            .collect();
        // sort by score descending
        scored_candidates.sort_by(|a, b| OrderedFloat(b.score).cmp(&OrderedFloat(a.score)));

        // return top n
        scored_candidates.into_iter().take(top).collect()
    }

    pub fn query_immutable_index(
        &self,
        top: usize,
        query_vector: SparseVector,
    ) -> Vec<ScoredCandidate> {
        let mut search_context =
            SearchContext::new(query_vector, top, self.immutable_index.as_ref().unwrap());
        search_context.search()
    }

    pub fn print_mutable_index_statistics(&self) {
        let mut max_posting_list_size = 0;
        let mut max_posting_list_size_index = 0;

        let mut min_posting_list_size = usize::MAX;
        let mut min_posting_list_size_index = 0;

        for (k, v) in self.mutable_index.map.iter() {
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
        println!("\nMutable sparse vector statistics:");
        println!("Index size: {} keys", self.mutable_index.map.len());
        println!(
            "Max posting list size for key {} with {} vector ids",
            max_posting_list_size_index, max_posting_list_size
        );
        println!(
            "Min posting list size for key {} with {} vector ids",
            min_posting_list_size_index, min_posting_list_size
        );
    }

    pub fn print_immutable_index_statistics(&self) {
        let mut max_posting_list_size = 0;
        let mut max_posting_list_size_index = 0;

        let mut min_posting_list_size = usize::MAX;
        let mut min_posting_list_size_index = 0;

        let index = self.immutable_index.as_ref().unwrap();
        let mut index_size = 0;
        for (k, posting) in index.postings.iter().enumerate() {
            let size = posting.elements.len();
            // exclude empty placeholder posting lists
            if size > 0 {
                index_size += 1;
                if size > max_posting_list_size {
                    max_posting_list_size = size;
                    max_posting_list_size_index = k;
                }
                if size < min_posting_list_size {
                    min_posting_list_size = size;
                    min_posting_list_size_index = k;
                }
            }
        }

        println!("\nImmutable sparse vector statistics:");
        println!("Index size: {} keys", index_size);
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
        let mut min_index = u32::MAX;
        let mut min_value = f32::MAX;
        let mut max_length = 0;
        let mut min_length = usize::MAX;
        let mut sum_length = 0;
        for sparse_vector in self.vectors.iter().flatten() {
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
            for &value in &sparse_vector.weights {
                if value > max_value {
                    max_value = value;
                }
                if value < min_value {
                    min_value = value;
                }
            }
            vector_count += 1;
        }
        println!("\nStorage statistics:");
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
    use crate::sparse_index::common::types::RecordId;
    use crate::sparse_index::common::vector::SparseVector;
    use crate::storage::SparseVectorStorage;
    use crate::SPLADE_DATA_PATH;
    use float_cmp::approx_eq;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use std::sync::{OnceLock, RwLock};

    fn storage() -> &'static RwLock<SparseVectorStorage> {
        static STORAGE: OnceLock<RwLock<SparseVectorStorage>> = OnceLock::new();
        STORAGE.get_or_init(|| {
            eprintln!("Loading test storage...");
            let mut storage = SparseVectorStorage::load_SPLADE_embeddings(SPLADE_DATA_PATH);
            // build immutable index
            storage.build_immutable_index();
            RwLock::new(storage)
        })
    }

    #[test]
    fn validate_data_equivalence() {
        let storage = storage().read().unwrap();
        let immutable_index = storage.immutable_index.as_ref().unwrap();

        for (vector_id, vector) in storage.vectors.iter().enumerate() {
            if let Some(vector) = vector {
                for (index, &stored_weight) in vector.indices.iter().zip(vector.weights.iter()) {
                    let record_id = &(vector_id as RecordId);
                    // control data in mutable index
                    // mutable_index contains record_id for dimension index
                    assert!(storage
                        .mutable_index
                        .get(index)
                        .unwrap()
                        .contains(record_id));

                    // control data in immutable index
                    let posting_list = immutable_index.get(index).unwrap();
                    let elem_index = posting_list
                        .elements
                        .binary_search_by(|elem| elem.id.cmp(record_id));
                    let elem = posting_list.elements[elem_index.unwrap()];
                    // immutable_index contains correct weight and record_id for dimension index
                    assert_eq!(elem.weight, stored_weight);
                }
            }
        }
    }

    fn search_equivalence(top: u8, query: SparseVector) {
        // memoized storage
        let storage = storage().read().unwrap();

        // results from all three search methods
        let full_scan_results = storage.query_full_scan(top as usize, &query);
        let mutable_index_results = storage.query_mutable_index(top as usize, &query);
        let immutable_index_results = storage.query_immutable_index(top as usize, query);

        // The ties are not broken in any way, so the order of results may differ in terms of vector ids
        for (((i, full), mutable), immutable) in full_scan_results
            .iter()
            .enumerate()
            .zip(mutable_index_results)
            .zip(immutable_index_results)
        {
            // https://docs.rs/float-cmp/latest/float_cmp/
            assert!(
                approx_eq!(f32, full.score, mutable.score),
                "i:{} full_scan: {:?}, mutable: {:?} (id: {:?} vs {:?})",
                i,
                full.score,
                mutable.score,
                full.vector_id,
                mutable.vector_id
            );
            assert!(
                approx_eq!(f32, full.score, immutable.score),
                "i:{} full_scan: {:?}, immutable:{:?} (id: {:?} vs {:?})",
                i,
                full.score,
                immutable.score,
                full.vector_id,
                immutable.vector_id
            );
        }
    }

    // More runs with QUICKCHECK_TESTS=100000 cargo test --release validate_search_equivalence
    #[quickcheck]
    fn validate_search_equivalence(top: u8, query: SparseVector) {
        // skip top zero (max 256)
        if top == 0 {
            return;
        }
        search_equivalence(top, query);
    }

    // bunch of failing cases detected by quickcheck captured for non regression
    #[test]
    fn search_equivalence_example_one() {
        let top = 51;
        let query = SparseVector::new(
            vec![3655, 14336, 19313, 27039],
            vec![0.01, 0.01, 100.0, 100.0],
        );
        search_equivalence(top, query);
    }

    #[test]
    fn search_equivalence_example_two() {
        let top = 8;
        let query = SparseVector::new(vec![7146, 16390, 20913], vec![0.01, 100.0, 100.0]);
        search_equivalence(top, query);
    }

    #[test]
    fn search_equivalence_example_three() {
        let top = 1;
        let query = SparseVector::new(vec![1012, 10434, 21517], vec![0.01, 0.01, 100.0]);
        search_equivalence(top, query);
    }

    #[test]
    fn search_equivalence_example_four() {
        let top = 16;
        let query = SparseVector::new(vec![2215, 2387, 8111], vec![100.0, 100.0, 100.0]);
        search_equivalence(top, query);
    }

    #[test]
    fn search_equivalence_example_five() {
        let top = 1;
        let query = SparseVector::new(vec![9834, 13025, 21650], vec![0.01, 100.0, 100.0]);
        search_equivalence(top, query);
    }

    // quickcheck arbitrary impls
    impl Arbitrary for SparseVector {
        fn arbitrary(g: &mut Gen) -> SparseVector {
            // max u8 = 255
            let len = u8::arbitrary(g);
            // max u16 = 65_535
            let mut indices: Vec<_> = (0..len).map(|_| u16::arbitrary(g) as u32).collect();
            // remove potential duplicates indices
            indices.sort();
            indices.dedup();
            // restrict weights to be < 100 to avoid really high scores
            let weights = (0..indices.len())
                .map(|_| f32::arbitrary(g).clamp(0.0, 100.0))
                .collect();
            SparseVector::new(indices, weights)
        }
    }
}
