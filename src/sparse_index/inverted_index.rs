use std::collections::HashMap;
use crate::sparse_index::posting::PostingList;
use crate::sparse_index::types::RecordId;

pub struct InvertedIndex {
    postings: Vec<PostingList>,
}

impl InvertedIndex {
    pub fn get(&self, id: RecordId) -> Option<&PostingList> {
        self.postings.get(id as usize)
    }
}

struct InvertedIndexBuilder {
    postings: HashMap<u32, PostingList>,
}

impl InvertedIndexBuilder {

    pub fn new() -> InvertedIndexBuilder {
        InvertedIndexBuilder {
            postings: HashMap::new(),
        }
    }

    pub fn add(&mut self, id: u32, posting: PostingList) {
        self.postings.insert(id, posting);
    }

    pub fn build(mut self) -> InvertedIndex {


        // Get sorted keys
        let mut keys: Vec<u32> = self.postings.keys().map(|k| *k).collect();
        keys.sort();

        let last_key = *keys.last().unwrap_or(&0);

        // Allocate postings of max key size
        let mut postings = Vec::new();
        postings.resize(last_key as usize, PostingList::default());

        for key in keys {
            postings.push(self.postings.remove(&key).unwrap());
        }
        InvertedIndex {
            postings,
        }
    }
}