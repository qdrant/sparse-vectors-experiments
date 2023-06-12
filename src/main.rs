mod storage;

use crate::storage::{SparseVector, SparseVectorStorage};
use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io::BufReader;

fn main() {
    let path = "./data/sparse-vectors.jsonl";
    let f = File::open(path).unwrap();
    let reader = BufReader::new(f);
    // steam jsonl values
    let stream = Deserializer::from_reader(reader).into_iter::<Value>();

    let mut internal_index = 0;
    let mut storage = SparseVectorStorage::new();

    for value in stream {
        let value = value.expect("Unable to parse JSON");
        match value {
            Value::Object(map) => {
                let mut indices = Vec::new();
                let mut values = Vec::new();
                for (key, value) in map {
                    indices.push(key.parse::<usize>().unwrap());
                    values.push(value.as_f64().unwrap() as f32);
                }
                storage.add(internal_index, SparseVector::new(indices, values));
                internal_index += 1;
            }
            _ => panic!("Unexpected value"),
        }
    }

    // print some stats about storage
    storage.print_data_statistics();
    storage.print_index_statistics();

    // search
    let now = std::time::Instant::now();
    let limit = 10;
    let query = SparseVector::new(vec![0, 1000, 2000, 3000], vec![1.0, 0.2, 0.9, 0.5]);
    let results = storage.query(limit, &query);
    let elapsed = now.elapsed();
    println!(
        "Top {} results for query {:?} in {} micros",
        limit,
        query,
        elapsed.as_micros()
    );
    for r in results {
        println!("Score {:?} id {}", r.score, r.vector_id);
    }
}
