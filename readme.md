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

#### Happy path

```
Search fullscan happy path (storage)
Top 10 results for full scan query SparseVector { indices: [0, 1000, 2000, 3000], weights: [1.0, 0.2, 0.9, 0.5] } in 19417 micros
Score 2.7671282 id 17065
Score 2.0483174 id 12683
Score 2.0371644 id 14102
Score 2.0196939 id 4549
Score 1.9788228 id 16912
Score 1.9322071 id 1126
Score 1.9067127 id 34838
Score 1.8462889 id 1520
Score 1.8256714 id 34005
Score 1.8095042 id 19780
```

```
Search happy path (mutable index)
Top 10 results for index query SparseVector { indices: [0, 1000, 2000, 3000], weights: [1.0, 0.2, 0.9, 0.5] } in 4072 micros
Score 2.7671282 id 17065
Score 2.0483174 id 12683
Score 2.0371644 id 14102
Score 2.0196939 id 4549
Score 1.9788228 id 16912
Score 1.9322071 id 1126
Score 1.9067127 id 34838
Score 1.8462889 id 1520
Score 1.8256714 id 34005
Score 1.8095042 id 19780
```

```
Search happy path (immutable index)
Top 10 results for index query SparseVector { indices: [0, 1000, 2000, 3000], weights: [1.0, 0.2, 0.9, 0.5] } in 291 micros
Score 2.7671282 id 17065
Score 2.0483174 id 12683
Score 2.0371644 id 14102
Score 2.0196939 id 4549
Score 1.9788228 id 16912
Score 1.9322071 id 1126
Score 1.9067127 id 34838
Score 1.8462889 id 1520
Score 1.8256714 id 34005
Score 1.8095042 id 19780
```

#### Hot key

```
Search fullscan hot key (storage)
Top 10 results for full scan query SparseVector { indices: [0, 1000, 2839, 3000], weights: [1.0, 0.2, 0.9, 0.5] } in 22195 micros
Score 2.034697 id 29677
Score 2.0140328 id 11691
Score 1.9806437 id 19080
Score 1.9476879 id 12225
Score 1.9174209 id 6556
Score 1.9105625 id 7112
Score 1.893393 id 17869
Score 1.8832698 id 10552
Score 1.8761885 id 6907
Score 1.8575685 id 17411
```

```
Search hot key (mutable index)
Top 10 results for index query SparseVector { indices: [0, 1000, 2839, 3000], weights: [1.0, 0.2, 0.9, 0.5] } in 22151 micros
Score 2.034697 id 29677
Score 2.0140328 id 11691
Score 1.9806437 id 19080
Score 1.9476879 id 12225
Score 1.9174209 id 6556
Score 1.9105625 id 7112
Score 1.893393 id 17869
Score 1.8832698 id 10552
Score 1.8761885 id 6907
Score 1.8575685 id 17411
```

```
Search hot key (immutable index)
Top 10 results for index query SparseVector { indices: [0, 1000, 2839, 3000], weights: [1.0, 0.2, 0.9, 0.5] } in 261 micros
Score 2.034697 id 29677
Score 2.0140328 id 11691
Score 1.9806437 id 19080
Score 1.9476879 id 12225
Score 1.9174209 id 6556
Score 1.9105625 id 7112
Score 1.893393 id 17869
Score 1.8832698 id 10552
Score 1.8761885 id 6907
Score 1.8575685 id 17411
```
