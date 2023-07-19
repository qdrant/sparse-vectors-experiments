use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;

/// This is a MinHeap by default - it will keep the largest elements, pop smallest
/// Extracted from qdrant repo
#[derive(Clone, Debug)]
pub struct FixedLengthPriorityQueue<T: Ord> {
    heap: BinaryHeap<Reverse<T>>,
    length: usize,
}

impl<T: Ord> Default for FixedLengthPriorityQueue<T> {
    fn default() -> Self {
        Self::new(1)
    }
}

impl<T: Ord> FixedLengthPriorityQueue<T> {
    pub fn new(length: usize) -> Self {
        assert!(length > 0);
        FixedLengthPriorityQueue::<T> {
            heap: BinaryHeap::with_capacity(length + 1),
            length,
        }
    }

    pub fn push(&mut self, value: T) -> Option<T> {
        if self.heap.len() < self.length {
            self.heap.push(Reverse(value));
            return None;
        }

        let mut x = self.heap.peek_mut().unwrap();
        let mut value = Reverse(value);
        if x.0 < value.0 {
            std::mem::swap(&mut *x, &mut value);
        }
        Some(value.0)
    }

    pub fn into_vec(self) -> Vec<T> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(x)| x)
            .collect()
    }

    pub fn top(&self) -> Option<&T> {
        self.heap.peek().map(|x| &x.0)
    }

    /// Returns actual length of the queue
    pub fn len(&self) -> usize {
        self.heap.len()
    }
}
