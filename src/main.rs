mod mutable_index;
mod storage;
mod vector;

use crate::storage::SparseVectorStorage;
use crate::vector::SparseVector;

pub const SPLADE_DATA_PATH: &str = "./data/sparse-vectors.jsonl";

fn main() {
    let storage = SparseVectorStorage::load_SPLADE_embeddings(SPLADE_DATA_PATH);

    // print some stats about storage
    storage.print_data_statistics();
    storage.print_index_statistics();

    println!("\nSearch fullscan");
    let now = std::time::Instant::now();
    let limit = 10;
    let query = SparseVector::new(vec![0, 1000, 2000, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    let results = storage.query_full_scan(limit, &query);
    let elapsed = now.elapsed();
    println!(
        "Top {} results for full scan query {:?} in {} micros",
        limit,
        query,
        elapsed.as_micros()
    );
    for r in results {
        println!("Score {:?} id {}", r.score, r.vector_id);
    }

    println!("\nSearch happy path");
    let now = std::time::Instant::now();
    let limit = 10;
    let query = SparseVector::new(vec![0, 1000, 2000, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    let results = storage.query_index(limit, &query);
    let elapsed = now.elapsed();
    println!(
        "Top {} results for index query {:?} in {} micros",
        limit,
        query,
        elapsed.as_micros()
    );
    for r in results {
        println!("Score {:?} id {}", r.score, r.vector_id);
    }

    println!("\nSearch hot key");
    let now = std::time::Instant::now();
    let limit = 10;
    // '2839' is vey hot (34461 entries)
    let query = SparseVector::new(vec![0, 1000, 2839, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    let results = storage.query_index(limit, &query);
    let elapsed = now.elapsed();
    println!(
        "Top {} results for index query {:?} in {} micros",
        limit,
        query,
        elapsed.as_micros()
    );
    for r in results {
        println!("Score {:?} id {}", r.score, r.vector_id);
    }
}
