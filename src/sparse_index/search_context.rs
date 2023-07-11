use std::cmp::Ordering;
use crate::sparse_index::fixed_length_pq::FixedLengthPriorityQueue;
use crate::sparse_index::inverted_index::InvertedIndex;
use crate::sparse_index::posting::PostingListIterator;
use crate::sparse_index::types::{DimId, DimWeight, RecordId};


#[derive(Debug, PartialEq)]
pub struct SparseVector {
    pub indices: Vec<DimId>,
    pub weights: Vec<DimWeight>,
}

#[derive(Debug, PartialEq)]
pub struct ScoredCandidate {
    pub score: DimWeight,
    pub vector_id: RecordId,
}

impl Eq for ScoredCandidate {}

impl PartialOrd<Self> for ScoredCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.score.partial_cmp(&other.score).map(|o| o.reverse())
    }
}

impl Ord for ScoredCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

struct IndexedPostingListIterator<'a> {
    posting_list_iterator: PostingListIterator<'a>,
    query_weight_offset: usize,
}


pub struct SearchContext<'a> {
    postings_iterators: Vec<IndexedPostingListIterator<'a>>,
    query: SparseVector,
    result_queue: FixedLengthPriorityQueue<ScoredCandidate>,
}


impl<'a> SearchContext<'a> {
    pub fn new(query: SparseVector, top: usize, inverted_index: &'a InvertedIndex) -> SearchContext<'a> {
        let mut postings_iterators = Vec::new();

        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            if let Some(posting) = inverted_index.get(*id) {
                postings_iterators.push(IndexedPostingListIterator {
                    posting_list_iterator: PostingListIterator::new(posting),
                    query_weight_offset,
                });
            }
        }

        postings_iterators.sort_by_key(|i| i.posting_list_iterator.len_left());
        postings_iterators.reverse();

        SearchContext {
            postings_iterators,
            query,
            result_queue: FixedLengthPriorityQueue::new(top),
        }
    }

    /// Example
    ///
    /// postings_iterators:
    ///
    /// 1,  30, 34, 60, 230
    /// 10, 30, 35, 51, 230
    /// 2,  21, 34, 60, 200
    /// 2,  30, 34, 60, 230
    ///
    /// Next:
    ///
    /// a,  30, 34, 60, 230
    /// 10, 30, 35, 51, 230
    /// 2,  21, 34, 60, 200
    /// 2,  30, 34, 60, 230
    ///
    /// Next:
    ///
    /// a,  30, 34, 60, 230
    /// 10, 30, 35, 51, 230
    /// b,  21, 34, 60, 200
    /// b,  30, 34, 60, 230
    ///
    /// Next:
    ///
    /// a,  30, 34, 60, 230
    /// c,  30, 35, 51, 230
    /// b,  21, 34, 60, 200
    /// b,  30, 34, 60, 230

    fn advance(&mut self) -> Option<ScoredCandidate> {
        let mut min_record_id = 0;
        // Indicates that posting iterators are not empty
        let mut found = false;

        for posting_iterator in self.postings_iterators.iter() {
            if let Some(element) = posting_iterator.posting_list_iterator.peek() {
                found = true;
                if element.id < min_record_id {
                    min_record_id = element.id;
                }
            }
        }

        if !found {
            return None;
        }

        let mut score = 0.0;

        // Iterate second time to advance posting iterators
        for posting_iterator in self.postings_iterators.iter_mut() {
            if let Some(record_id) = posting_iterator.posting_list_iterator.peek().map(|element| element.id) {
                if record_id == min_record_id {
                    let element = posting_iterator.posting_list_iterator.next().unwrap();
                    score += element.weight * self.query.weights[posting_iterator.query_weight_offset];
                }
            }
        }

        Some(ScoredCandidate {
            score,
            vector_id: min_record_id,
        })
    }
}