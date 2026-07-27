# DuckLake FDB Benchmark

## Method

| Property            | Value                                      |
| ------------------- | ------------------------------------------ |
| Date                | 2026-07-27                                 |
| FDB proxy           | `127.0.0.1:14691` -> `127.0.0.1:4691`      |
| Postgres proxy      | `127.0.0.1:15432` -> `127.0.0.1:5432`      |
| Proxy latency       | 1 ms downstream, 0 ms jitter               |
| State               | empty before and after each recipe         |
| Paired SQL          | identical                                  |
| Multi-trial order   | alternating                                |
| Build profile       | release                                    |
| Runtime metrics     | disabled                                   |
| DuckDB memory limit | 1 GiB                                      |
| DuckLake commit     | `2856687c875bbee90d523fe15627f8d8fd494622` |
| DuckDB commit       | `117e1a46be1c903c5a36ee3c881c125597f93c60` |

## Full Suite

| Profile               |      FDB ms |   Postgres ms | FDB/Postgres |
| --------------------- | ----------: | ------------: | -----------: |
| smoke                 |   4,560.057 |     7,671.313 |       0.594x |
| scan10                |   3,821.652 |     7,430.280 |       0.514x |
| profile, 10,000 files |   4,175.981 |     7,492.897 |       0.557x |
| inline                |   5,952.556 |    11,773.628 |       0.506x |
| operational           |  30,618.158 |   118,152.447 |       0.259x |
| operational growth    |  91,988.772 |   403,600.153 |       0.228x |
| realistic             | 156,534.897 |   279,921.680 |       0.559x |
| varied, 5 GiB         | 723,738.947 | 1,539,848.010 |       0.470x |

## Operational Transactions

| Backend      | Samples | Create p50 ms | Commit p50 ms |    p90 ms |    p99 ms |    Max ms |
| ------------ | ------: | ------------: | ------------: | --------: | --------: | --------: |
| FoundationDB |      40 |       210.659 |       366.482 |   383.313 |   397.275 |   397.275 |
| Postgres     |      40 |       426.097 |     1,829.309 | 1,859.964 | 1,878.088 | 1,878.088 |

## Operational Growth

| Backend      | Samples | Create p50 ms | Commit p50 ms |    p90 ms |    p99 ms |    Max ms |
| ------------ | ------: | ------------: | ------------: | --------: | --------: | --------: |
| FoundationDB |     200 |       212.662 |       448.931 |   481.804 |   494.683 |   499.617 |
| Postgres     |     200 |       412.961 |     1,988.695 | 2,027.455 | 2,057.290 | 2,510.511 |

| Backend      | Trial 1 last/first decile | Trial 2 last/first decile |
| ------------ | ------------------------: | ------------------------: |
| FoundationDB |                    1.181x |                    1.170x |
| Postgres     |                    1.091x |                    1.075x |

## Full 5 GiB Varied Workload

| Property                          |  Value |
| --------------------------------- | -----: |
| Tables                            |    100 |
| Columns per table                 |     24 |
| Rows per table                    | 13,108 |
| Target logical data               |  5 GiB |
| Preload rows per batch            |  4,096 |
| Preload workers                   |      4 |
| Parallel latest-read workers      |     12 |
| Compaction tables per transaction |     10 |

| Batch                   |      FDB ms |   Postgres ms | FDB/Postgres |
| ----------------------- | ----------: | ------------: | -----------: |
| preload                 | 117,800.152 |   201,430.927 |       0.585x |
| mixed                   |  79,926.372 |   129,894.705 |       0.615x |
| dedicated deletes       |  84,682.941 |   136,216.038 |       0.622x |
| dedicated inlining      | 112,449.389 |   164,059.126 |       0.685x |
| dedicated compaction    |  81,239.513 |    66,885.877 |       1.215x |
| join queries            |  21,574.901 |    40,060.361 |       0.539x |
| mutation churn          | 175,482.500 |   726,119.720 |       0.242x |
| latest queries          |  14,412.049 |    23,469.705 |       0.614x |
| time-travel queries     |  14,405.110 |    23,541.991 |       0.612x |
| parallel latest queries |  16,538.659 |    22,825.380 |       0.725x |
| total                   | 723,738.947 | 1,539,848.010 |       0.470x |

| Peak RSS       |   FDB KiB | Postgres KiB |
| -------------- | --------: | -----------: |
| maximum        | 1,183,712 |      952,528 |
| compaction     |   510,496 |      566,848 |
| mutation churn | 1,031,040 |      952,528 |

## Robustness

| Scenario                |      FDB ms | Postgres ms | FDB/Postgres |
| ----------------------- | ----------: | ----------: | -----------: |
| narrow tiny batches     |  43,556.657 |  50,565.931 |       0.861x |
| mixed balanced          |  43,253.020 |  81,502.119 |       0.531x |
| wide large rows         |  25,261.848 |  45,170.291 |       0.559x |
| many small tables       | 119,116.447 | 226,416.722 |       0.526x |
| concurrent read/write   |  45,044.383 |  83,695.995 |       0.538x |
| inline flush each batch |  11,820.991 |  19,321.176 |       0.612x |
| inline flush at end     |  19,385.606 |  30,870.038 |       0.628x |
| inline never flush      |   7,058.505 |  17,608.544 |       0.401x |
| total                   | 314,497.457 | 555,150.816 |       0.567x |

## Scale Status

| Area                  | FDB/Postgres | Status     |
| --------------------- | -----------: | ---------- |
| latest reads          |       0.614x | FDB faster |
| time-travel reads     |       0.612x | FDB faster |
| parallel latest reads |       0.725x | FDB faster |
| compaction            |       1.215x | unresolved |
