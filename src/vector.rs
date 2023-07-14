use std::cmp::Ordering;
use crate::sparse_index::types::{DimId, DimWeight};

#[derive(Debug, PartialEq)]
pub struct SparseVector {
    pub indices: Vec<DimId>,
    pub weights: Vec<DimWeight>,
}

impl SparseVector {
    pub fn new(indices: Vec<DimId>, weights: Vec<DimWeight>) -> SparseVector {
        SparseVector { indices, weights }
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
                    result += self.weights[i] * other.weights[j];
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

#[cfg(test)]
mod tests {

    #[test]
    fn test_dot_product_aligned() {
        use super::*;
        let v1 = SparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        let v2 = SparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        assert_eq!(v1.dot_product(&v2), 14.0);
    }

    #[test]
    fn test_dot_product_missing() {
        use super::*;
        let v1 = SparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        let v2 = SparseVector::new(vec![1, 2], vec![1.0, 2.0]);
        assert_eq!(v1.dot_product(&v2), 5.0);
    }

}
