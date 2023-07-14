use std::cmp::Ordering;
use ordered_float::OrderedFloat;
use crate::sparse_index::fixed_length_pq::FixedLengthPriorityQueue;
use crate::sparse_index::inverted_index::InvertedIndex;
use crate::sparse_index::posting::PostingListIterator;
use crate::sparse_index::types::{DimWeight, RecordId};
use crate::vector::SparseVector;

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

struct IndexedPostingListIterator<'a> {
    posting_list_iterator: PostingListIterator<'a>,
    query_weight_offset: usize,
}


pub struct SearchContext<'a> {
    postings_iterators: Vec<IndexedPostingListIterator<'a>>,
    query: SparseVector,
    top: usize,
    result_queue: FixedLengthPriorityQueue<ScoredCandidate>, // keep the largest elements and pop smallest
}


impl<'a> SearchContext<'a> {
    pub fn new(query: SparseVector, top: usize, inverted_index: &'a InvertedIndex) -> SearchContext<'a> {
        let mut postings_iterators = Vec::new();

        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            if let Some(posting) = inverted_index.get(id) {
                postings_iterators.push(IndexedPostingListIterator {
                    posting_list_iterator: PostingListIterator::new(posting),
                    query_weight_offset,
                });
            }
        }

        // TODO: sort by highest weight in one pass
        postings_iterators.sort_by_key(|i| i.posting_list_iterator.len_left());
        postings_iterators.reverse();

        SearchContext {
            postings_iterators,
            query,
            top,
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
        // Initialize min record id with max value
        let mut min_record_id = u32::MAX;
        // Indicates that posting iterators are not empty
        let mut found = false;

        // Iterate first time to find min record id at the head of the posting lists
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

    /// Make sure the longest posting list is at the head of the posting list iterators
    pub fn sort_posting_lists_by_len(&mut self) {
        self.postings_iterators.sort_by_key(|i| i.posting_list_iterator.len_left());
        self.postings_iterators.reverse();
    }

    pub fn search(&mut self) -> Vec<ScoredCandidate> {
        // sort posting lists by length to start by longest
        self.sort_posting_lists_by_len();

        loop {
            if let Some(candidate) = self.advance() {
                // push candidate to result queue
                self.result_queue.push(candidate);
            } else {
                // all posting list iterators are empty
                break;
            }

            eprintln!("result queue len: {} min is {:?}", self.result_queue.len(), self.result_queue.top().map(|c| c.score));
            if self.result_queue.len() == self.top {
                // we *potentially* have enough results to prune low performing posting lists
                let min_score = self.result_queue.top().unwrap().score;
                self.prune_longest_posting_list(min_score);
            }
        }
        let queue = std::mem::take(&mut self.result_queue);
        queue.into_vec()
    }

    /// Prune posting lists that cannot possibly contribute to the top results
    ///  Assumes longest posting list is at the head of the posting list iterators
    pub fn prune_longest_posting_list(&mut self, min_score: f32) {
        let skip_to = if self.postings_iterators.len() == 1 {
            // if there is only one posting list iterator, we can skip to the end
            u32::MAX
        } else {
            // otherwise, we skip to the next element in the next longest posting list
            let next_posting_iterator = &self.postings_iterators[1];
            next_posting_iterator.posting_list_iterator.peek().map(|element| element.id).unwrap_or(u32::MAX)
        };
        let posting_iterator = &mut self.postings_iterators[0];
        if let Some(element) = posting_iterator.posting_list_iterator.peek() {
            let max_weight_from_list = element.weight.max(element.max_next_weight);
            let score_contribution = max_weight_from_list * self.query.weights[posting_iterator.query_weight_offset];
            if score_contribution  < min_score {
                eprintln!("Skipping posting list with max weight {} and query weight {} because {} < min_score: {}",
                          max_weight_from_list,
                          self.query.weights[posting_iterator.query_weight_offset],
                            score_contribution,
                          min_score);
                posting_iterator.posting_list_iterator.skip_to(skip_to);
            } else {
                eprintln!("no pruning took place")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sparse_index::inverted_index::InvertedIndexBuilder;
    use crate::sparse_index::posting::PostingList;
    use super::*;

    #[test]
    fn advance_basic_test() {
        let inverted_index = InvertedIndexBuilder::new()
            .add(1, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            10,
            &inverted_index,
        );

        assert_eq!(search_context.advance(), Some(ScoredCandidate { score: 30.0, vector_id: 1 }));
        assert_eq!(search_context.advance(), Some(ScoredCandidate { score: 60.0, vector_id: 2 }));
        assert_eq!(search_context.advance(), Some(ScoredCandidate { score: 90.0, vector_id: 3 }));
    }

    #[test]
    fn search() {
        let inverted_index = InvertedIndexBuilder::new()
            .add(1, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            10,
            &inverted_index,
        );

        assert_eq!(search_context.search(), vec![
            ScoredCandidate { score: 90.0, vector_id: 3 },
            ScoredCandidate { score: 60.0, vector_id: 2 },
            ScoredCandidate { score: 30.0, vector_id: 1 },
        ]);
    }

    #[test]
    fn search_with_non_balanced() {
        let inverted_index = InvertedIndexBuilder::new()
            .add(1, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0), (4, 1.0), (5, 2.0), (6, 3.0), (7, 4.0), (8, 5.0), (9, 6.0)]))
            .add(2, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from_records(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            3,
            &inverted_index,
        );

        assert_eq!(search_context.search(), vec![
            ScoredCandidate { score: 90.0, vector_id: 3 },
            ScoredCandidate { score: 60.0, vector_id: 2 },
            ScoredCandidate { score: 30.0, vector_id: 1 },
        ]);

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            4,
            &inverted_index,
        );

        assert_eq!(search_context.search(), vec![
            ScoredCandidate { score: 90.0, vector_id: 3 },
            ScoredCandidate { score: 60.0, vector_id: 2 },
            ScoredCandidate { score: 30.0, vector_id: 1 },
            ScoredCandidate { score: 6.0, vector_id: 9 },
        ]);
    }
}