# Sparse vectors experiments

## Data

35K sparse vectors (~300 MB)

- download dataset: https://console.cloud.google.com/storage/browser/dataset-sparse-vectors;tab=objects?forceOnBucketsSortingFiltering=true&authuser=0&project=key-surf-337615&prefix=&forceOnObjectsSortingFiltering=false
- generated with notebook: https://colab.research.google.com/drive/1acKdYvUf1CK0jA9qECF95YNkrTHYVTtk?usp=sharing

## Statistics

```
Data size: 264 mb
Data loaded in 4764 ms
Immutable index built in 2812 ms

Storage statistics:
Data size: 34880 sparse vectors
Max sparse index: 30265
Min sparse index: 100
Max sparse value: 3.4626007
Min sparse value: 0.00000023841855
Max sparse vector length: 480
Min sparse length: 32
Avg sparse length: 278.5090883027523

Mutable sparse vector statistics:
Index size: 26372 keys
Max posting list size for key 2839 with 34461 vector ids
Min posting list size for key 27186 with 1 vector ids

Immutable sparse vector statistics:
Index size: 26372 keys
Max posting list size for key 2839 with 34461 vector ids
Min posting list size for key 1026 with 1 vector ids
```

## Experiments

Hot keys have a high impact on the search time.

Hitting one of those is basically equivalent to a full scan without an optimized index.

```
Query with (easy) SparseVector { indices: [0, 1000, 2000, 3000], weights: [1.0, 0.2, 0.9, 0.5] } with limit 100
Search full scan storage in 24 ms
Search mutable index in 5 ms
Search immutable index in 271 micros

Query with (hot) SparseVector { indices: [0, 1000, 2839, 3000], weights: [1.0, 0.2, 0.9, 0.5] } with limit 100
Search full scan storage in 25 ms
Search mutable index in 32 ms
Search immutable index in 343 micros

```
