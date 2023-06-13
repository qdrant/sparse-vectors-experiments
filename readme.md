# Sparse vectors experiments

## Data

35K sparse vectors (300 MB)

- download dataset: https://console.cloud.google.com/storage/browser/dataset-sparse-vectors;tab=objects?forceOnBucketsSortingFiltering=true&authuser=0&project=key-surf-337615&prefix=&forceOnObjectsSortingFiltering=false
- generated with notebook: https://colab.research.google.com/drive/1acKdYvUf1CK0jA9qECF95YNkrTHYVTtk?usp=sharing

## Statistics

```
Data size: 34880 sparse vectors
Max sparse index: 30265
Min sparse index: 100
Max sparse value: 3.4626007
Min sparse value: 0.00000023841855
Max sparse vector length: 480
Min sparse length: 32
Avg sparse length: 278.5090883027523
Index size: 26372 keys
Max posting list size for key 2839 with 34461 vector ids
Min posting list size for key 15855 with 1 vector ids
```

## Experiments

### Hot keys

Hot keys have a high impact on the search time.

Hitting one of those is basically equivalent to a full scan.

#### Fullscan
```
Top 10 results for full scan query SparseVector { indices: [0, 1000, 2000, 3000], values: [1.0, 0.2, 0.9, 0.5] } in 3851 micros
Score 0.1808575 id 21829
Score 0.15541957 id 19635
Score 0.14179742 id 1056
Score 0.13906032 id 13958
Score 0.13815996 id 34194
Score 0.13644157 id 16133
Score 0.13568835 id 19144
Score 0.13387237 id 18241
Score 0.13109322 id 16474
Score 0.1304516 id 22701
```

#### Index query (regular key)

```
Top 10 results for index query SparseVector { indices: [0, 1000, 2000, 3000], values: [1.0, 0.2, 0.9, 0.5] } in 883 micros
Score 0.1808575 id 21829
Score 0.15541957 id 19635
Score 0.14179742 id 1056
Score 0.13906032 id 13958
Score 0.13815996 id 34194
Score 0.13644157 id 16133
Score 0.13568835 id 19144
Score 0.13387237 id 18241
Score 0.13109322 id 16474
Score 0.1304516 id 22701
```

#### Index query (hot key)

```
Top 10 results for index query SparseVector { indices: [0, 1000, 2839, 3000], values: [1.0, 0.2, 0.9, 0.5] } in 3851 micros
Score 0.1808575 id 21829
Score 0.15541957 id 19635
Score 0.14179742 id 1056
Score 0.13906032 id 13958
Score 0.13815996 id 34194
Score 0.13644157 id 16133
Score 0.13568835 id 19144
Score 0.13387237 id 18241
Score 0.13109322 id 16474
Score 0.1304516 id 22701
```
