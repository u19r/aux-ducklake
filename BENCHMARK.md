# DuckLake catalog benchmarks

## Method

| Property         | Value                                                                                         |
| ---------------- | --------------------------------------------------------------------------------------------- |
| Host             | `r8gd.4xlarge`, 16 ARM64 vCPUs, 128 GiB RAM                                                   |
| DuckDB           | 2.0.0, `d54cbbe3df17e8e5ce68312f483b4155836112aa` on both sides                               |
| DuckLake         | 1.1, `417a9117f590e8141742ac0579d02f6a90162e37` on both sides                                 |
| FoundationDB     | 7.4.8 client/server, **`ssd-redwood-1`**, one process, single redundancy                      |
| Postgres         | 18.6; fsync, synchronous commit and full-page writes enabled                                  |
| Metadata delay   | Toxiproxy 2.13.0, **1 ms downstream, zero jitter, both databases**, toxicity 1                |
| Database storage | Instance NVMe: data, WAL, logs, descriptors, local metadata, Parquet and spills               |
| Memory           | DuckDB **48 GiB per process** on both sides; no fixed RSS rejection guard                     |
| FDB scans        | At most 1,024 concurrent prefix/range requests; ordered results and per-range row limits      |
| TPC-H            | 16 threads, one warmup and three measured trials per query/backend; alternating backend order |

## Catalog workloads

| Profile            | FDB seconds | Postgres seconds | FDB/Postgres |
| ------------------ | ----------- | ---------------- | ------------ |
| smoke              | 1.732       | 1.561            | 1.109×       |
| scan10             | 1.502       | 1.497            | 1.004×       |
| profile            | 1.796       | 1.565            | 1.148×       |
| inline             | 3.679       | 2.681            | 1.372×       |
| operational        | 9.214       | 8.608            | 1.070×       |
| operational-growth | 25.176      | 22.183           | 1.135×       |
| realistic          | 48.870      | 37.857           | 1.291×       |
| varied             | 187.414     | 178.627          | 1.049×       |

## 2 GiB mixed workload

| Phase                   | FDB seconds | Postgres seconds | FDB/Postgres |
| ----------------------- | ----------- | ---------------- | ------------ |
| preload                 | 9.285       | 9.896            | 0.938×       |
| mixed                   | 8.625       | 5.085            | 1.696×       |
| dedicated_deletes       | 7.853       | 4.491            | 1.749×       |
| dedicated_inlining      | 15.375      | 11.557           | 1.330×       |
| dedicated_compaction    | 2.070       | 3.403            | 0.608×       |
| latest_queries          | 1.444       | 0.648            | 2.229×       |
| time_travel_queries     | 1.444       | 0.648            | 2.227×       |
| parallel_latest_queries | 1.763       | 0.930            | 1.896×       |

## 5 GiB mixed workload

| Phase                   | FDB seconds | Postgres seconds | FDB/Postgres |
| ----------------------- | ----------- | ---------------- | ------------ |
| preload                 | 33.549      | 54.572           | 0.615×       |
| mixed                   | 22.677      | 23.284           | 0.974×       |
| dedicated_deletes       | 17.133      | 10.592           | 1.617×       |
| dedicated_inlining      | 32.774      | 24.783           | 1.322×       |
| dedicated_compaction    | 17.970      | 10.336           | 1.739×       |
| join_queries            | 3.162       | 2.149            | 1.472×       |
| mutation_churn          | 45.117      | 43.498           | 1.037×       |
| latest_queries          | 2.994       | 1.401            | 2.137×       |
| time_travel_queries     | 3.054       | 1.344            | 2.273×       |
| parallel_latest_queries | 5.402       | 2.537            | 2.130×       |

## TPC-H SF5 and SF50

| Scale | FDB query seconds | Postgres query seconds | Query ratio | FDB process seconds | Postgres process seconds |
| ----- | ----------------- | ---------------------- | ----------- | ------------------- | ------------------------ |
| SF5   | 3.439             | 3.010                  | 1.143×      | 8.261               | 6.277                    |
| SF50  | 15.828            | 16.117                 | 0.982×      | 23.372              | 19.937                   |

| Query | SF5 FDB ms | SF5 Postgres ms | SF50 FDB ms | SF50 Postgres ms |
| ----- | ---------- | --------------- | ----------- | ---------------- |
| Q1    | 104        | 115             | 729         | 756              |
| Q2    | 131        | 91              | 222         | 182              |
| Q3    | 157        | 134             | 777         | 815              |
| Q4    | 108        | 104             | 524         | 562              |
| Q5    | 218        | 171             | 904         | 945              |
| Q6    | 64         | 69              | 300         | 313              |
| Q7    | 202        | 171             | 796         | 793              |
| Q8    | 243        | 196             | 995         | 1042             |
| Q9    | 281        | 235             | 1583        | 1571             |
| Q10   | 211        | 188             | 894         | 914              |
| Q11   | 88         | 60              | 176         | 147              |
| Q12   | 113        | 100             | 488         | 488              |
| Q13   | 196        | 188             | 1271        | 1257             |
| Q14   | 122        | 112             | 511         | 533              |
| Q15   | 104        | 97              | 490         | 513              |
| Q16   | 140        | 115             | 311         | 284              |
| Q17   | 126        | 125             | 657         | 762              |
| Q18   | 178        | 164             | 1120        | 1125             |
| Q19   | 124        | 114             | 562         | 574              |
| Q20   | 172        | 134             | 677         | 672              |
| Q21   | 257        | 236             | 1568        | 1606             |
| Q22   | 100        | 91              | 273         | 263              |

## Robustness and validation limits

| Scenario                | FDB seconds | Postgres seconds | FDB/Postgres |
| ----------------------- | ----------- | ---------------- | ------------ |
| narrow-tiny-batches     | 16.179      | 9.756            | 1.658×       |
| mixed-balanced          | 15.362      | 11.544           | 1.331×       |
| wide-large-rows         | 10.912      | 8.122            | 1.344×       |
| many-small-tables       | 33.166      | 27.159           | 1.221×       |
| concurrent-read-write   | 16.139      | 12.034           | 1.341×       |
| inline-flush-each-batch | 6.156       | 4.350            | 1.415×       |
| inline-flush-at-end     | 8.275       | 7.261            | 1.140×       |
| inline-never-flush      | 4.406       | 3.687            | 1.195×       |
