use crate::sparse_index::common::types::{DimWeight, RecordId};
use ordered_float::OrderedFloat;
use std::cmp::Ordering;

#[derive(Debug, PartialEq)]
pub struct ScoredCandidate {
    pub score: DimWeight,
    pub vector_id: RecordId,
}

impl Eq for ScoredCandidate {}

impl Ord for ScoredCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
