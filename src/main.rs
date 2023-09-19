mod sparse_index;
mod storage;

use crate::storage::SparseVectorStorage;
use float_cmp::approx_eq;
use sparse_index::common::vector::SparseVector;
use std::fs::File;
use tempfile::Builder;

pub const SPLADE_DATA_PATH: &str = "./data/sparse-vectors.jsonl";

fn main() {
    // check file size
    let f = File::open(SPLADE_DATA_PATH).unwrap();
    let data_len = f.metadata().unwrap().len();
    drop(f);
    println!("Data size: {} mb", data_len / 1024 / 1024);

    // load in storage
    let now = std::time::Instant::now();
    let mut storage = SparseVectorStorage::load_SPLADE_embeddings(SPLADE_DATA_PATH);
    println!("Data loaded in {} ms", now.elapsed().as_millis());

    let tmp_dir_path = Builder::new()
        .prefix("sparse_mmap_index_dir")
        .tempdir()
        .unwrap();

    // Immutable index
    let now = std::time::Instant::now();
    storage.build_immutable_index(Some(tmp_dir_path.path()));
    println!("Immutable index built in {} ms", now.elapsed().as_millis());

    // print some stats about storage & indexes
    storage.print_data_statistics();
    storage.print_mutable_index_statistics();
    storage.print_immutable_index_statistics();

    // how many results to return
    let limit = 100;

    // easy because no hot key
    let easy_query = SparseVector::new(vec![0, 1000, 2000, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    query_and_validate(&storage, limit, easy_query.clone(), "easy");

    // '2839' is vey hot (34461 entries)
    let hard_query = SparseVector::new(vec![0, 1000, 2839, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    query_and_validate(&storage, limit, hard_query.clone(), "hot");
}

fn query_and_validate(
    storage: &SparseVectorStorage,
    limit: usize,
    query: SparseVector,
    label: &str,
) {
    println!("\nQuery with ({}) {:?} with limit {}", label, query, limit);

    let now = std::time::Instant::now();
    let full_scan_results = storage.query_full_scan(limit, &query);
    let elapsed = now.elapsed();
    println!("Search full scan storage in {} ms", elapsed.as_millis());

    let now = std::time::Instant::now();
    let mutable_index_results = storage.query_mutable_index(limit, &query);
    let elapsed = now.elapsed();
    println!("Search mutable index in {} ms", elapsed.as_millis());

    let now = std::time::Instant::now();
    let immutable_index_results = storage.query_immutable_index(limit, query.clone());
    let elapsed = now.elapsed();
    println!("Search immutable index in {} micros", elapsed.as_micros());

    // validate equivalence of results
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
