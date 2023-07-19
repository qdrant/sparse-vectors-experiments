mod sparse_index;
mod storage;

use crate::storage::SparseVectorStorage;
use sparse_index::common::vector::SparseVector;

pub const SPLADE_DATA_PATH: &str = "./data/sparse-vectors.jsonl";

fn main() {
    let mut storage = SparseVectorStorage::load_SPLADE_embeddings(SPLADE_DATA_PATH);
    storage.build_immutable_index();

    // print some stats about storage & indexes
    storage.print_data_statistics();
    storage.print_mutable_index_statistics();
    storage.print_immutable_index_statistics();

    println!("\nSearch fullscan happy path (storage)");
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

    println!("\nSearch happy path (mutable index)");
    let now = std::time::Instant::now();
    let limit = 10;
    let query = SparseVector::new(vec![0, 1000, 2000, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    let results = storage.query_mutable_index(limit, &query);
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

    println!("\nSearch happy path (immutable index)");
    let now = std::time::Instant::now();
    let limit = 10;
    let query = SparseVector::new(vec![0, 1000, 2000, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    let results = storage.query_immutable_index(limit, query.clone());
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

    println!("\nSearch fullscan hot key (storage)");
    let now = std::time::Instant::now();
    let limit = 10;
    // '2839' is vey hot (34461 entries)
    let query = SparseVector::new(vec![0, 1000, 2839, 3000], vec![1.0, 0.2, 0.9, 0.5]);
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

    println!("\nSearch hot key (mutable index)");
    let now = std::time::Instant::now();
    let limit = 10;
    // '2839' is vey hot (34461 entries)
    let query = SparseVector::new(vec![0, 1000, 2839, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    let results = storage.query_mutable_index(limit, &query);
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

    println!("\nSearch hot key (immutable index)");
    let now = std::time::Instant::now();
    let limit = 10;
    // '2839' is vey hot (34461 entries)
    let query = SparseVector::new(vec![0, 1000, 2839, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    let results = storage.query_immutable_index(limit, query.clone());
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
