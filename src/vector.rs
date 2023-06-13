use std::cmp::Ordering;

#[derive(Debug, PartialEq)]
pub struct ScoredCandidate<'a> {
    pub score: f32,
    pub vector_id: usize,
    pub vector: &'a SparseVector,
}

#[derive(Debug, PartialEq)]
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
