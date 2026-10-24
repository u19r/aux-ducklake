# DuckDB 2.0 upgrade evidence

## Sources and packages

Baseline repository: `6cc0cb8`; DuckDB `117e1a46be1c903c5a36ee3c881c125597f93c60`; DuckLake `2856687c875bbee90d523fe15627f8d8fd494622`. The old shallow build prints a `v0.0.1` placeholder; the source commits identify the baseline.

New DuckDB: `d54cbbe3df17e8e5ce68312f483b4155836112aa` (v2.0.0). New DuckLake: `417a9117f590e8141742ac0579d02f6a90162e37` (1.1), selected by DuckDB’s release extension configuration. Rust bindings: `e1b0940dfddd575c43fe4e2f070d47ee53d419cd` (1.20000.0, Arrow 60). The binding revision is pinned because crates.io did not yet provide the matching crate.

Candidate consumer release: `v0.1.7`; bundle build: `v417a9117`. No tag or release was published. Analytics consumes the pinned DuckDB 2.0 binding and native bundle.

## Correctness evidence

- Catalog: **953 passed, 9 ignored** with `cargo test -p ducklake-catalog --features foundationdb`.
- Catalog Clippy: `cargo clippy -p ducklake-catalog --all-targets --features foundationdb -- -D warnings` passed.
- Workspace/all-target FoundationDB check passed; analytics workspace/all-target check passed.
- Analytics engine: **256 passed, 25 ignored** against the DuckDB 2.0 shared library.
- Shared-library and extracted archive smokes passed on macOS ARM64 and Linux ARM64. AMD64 passed the same full smoke in full-system x86 virtualization with matching UBI OpenSSL dependencies; the analytics downloader then verified its archive checksum and every extracted file against that smoked bundle, followed by an HTTP read from the extracted library. Both Linux archives passed the analytics downloader. ARM64 used clean UBI 9.8. They cover SQL/FDB catalogs, schema/default evolution with predicates, same-transaction ADD/default/INSERT, concurrent nested schemas, delete/flush history, sorted physical Parquet, encrypted analytics-style write/flush/delete/reopen, and reopen.
- Old-binary to new-binary persisted-catalog proof passed with explicit automatic migration from the previous development version.
- Six core upstream SQL scenarios passed on FDB and on a dedicated PostgreSQL instance. Fourteen of twenty combined FDB core/nested helper tests passed. Five helpers directly inspect or mutate SQL metadata tables, which are private mirrors for Aux; one expects a different conflict error string. The native nested/concurrency proof checks the authoritative backend directly.
- Isolated live runtime suite: 25 passed, 4 failed. All four failures also occurred with the original baseline binary (static file/change-feed ID assumptions); these fixtures were not rewritten.
- Release workflow negative tests rejected a tag/commit mismatch and replacement of published assets; a matching draft upload passed.

At the time of this comparison, the complete repository release gate had not passed because of baseline live-fixture failures and formatting failures. These historical results do not establish acceptance of later source or dependency changes.

## Performance method

All measurements use release builds on this shared Apple Silicon host. Each old/new comparison uses the same local FDB 7.4.6 server. The final native reruns use a separate dedicated instance because the shared default endpoint became unavailable; earlier shell measurements used the default instance. Compilation was idle during measurements; other host workloads were active. Raw runs record host load. Results are comparisons on this host, not production Linux latency guarantees.

After the final Rust parser cleanup, the native suites were rerun against the final packaged runtime; earlier shell measurements remain separately recorded. The first suite uses three alternating rounds (old/new, new/old, old/new), fresh processes and unique prefixes. Native tests use four DuckDB threads, a 1 GiB limit, actual 1,000-file metadata/filter workloads and 256-file dynamic joins, plus repeated inline commits, update/delete, flush and historical assertions. SQL fixture hashes match. Inline shell cases preload fifty tables; growth cases execute two trials of one hundred commits per run (600 measured commits per version overall). The profile argument `10000` means rows, not files.

The original growth limit of 1.5 already failed on the baseline (2.118). Across the six measured trials per version, first-decile commit means had medians of 94.08 ms old and 84.68 ms new; last-decile means were 157.16 ms old and 155.67 ms new. Median within-trial growth was 1.629× old and 1.778× new, while the median worst trial in each run was 1.739× old and 2.387× new. Absolute later-commit latency was similar, but the growth-ratio gate remains unproven. Both measurement drivers used the same higher collection limit so measurements could finish; that is not a growth-gate pass. Baseline attempts at repeated 1,000-file dynamic joins exceeded FDB’s read-version lifetime. Therefore the 1,000-file result covers metadata/filters, while join evidence is limited to 256 files.

One CLI profile sample overlapped a diagnostic, was preserved as excluded, and was rerun alone. An exploratory performance warm-up overlap was also stopped and excluded, as detailed in the Linux focused check below. Accepted final comparisons ran with our compilers, other benchmarks, and database diagnostics idle.

### Native workload medians

Values are milliseconds, using the median of each run’s repeated-query median, then the median across three runs.

| Operation | Old | New | New/old |
| --- | ---: | ---: | ---: |
| 1,000-file count | 91.94 | 51.05 | 0.555× |
| 1,000-file partition filter | 74.50 | 56.30 | 0.756× |
| 1,000-file column filter | 108.88 | 59.21 | 0.544× |
| 256-file dynamic join | 714.10 | 472.93 | 0.662× |
| Ten-table inline commit | 65.04 | 90.31 | 1.389× |
| Ten-table counts | 83.83 | 106.82 | 1.274× |
| Fifty-table inline flush | 2311.41 | 2384.85 | 1.032× |
| Historical count | 33.85 | 17.98 | 0.531× |
| Operational commit p50 | 130.08 | 123.20 | 0.947× |
| Operational commit p99 | 206.39 | 192.40 | 0.932× |

### Direct-library mixed SQL workload

The CLI process profile varied substantially (old 1.41–1.97 s; new 1.54–8.07 s across the accepted runs). A statement-timed reproduction was faster on the new build (1.27 s versus 2.26 s), so it did not reproduce a persistent SQL execution regression. The shell inline flush/read total was 24.5% slower in the original three pairs, while isolated statement timings showed faster actual flushes. These CLI/lifecycle tails remain uncertain on the shared host.

To test the integration boundary used by analytics, the same maintained file/inline SQL was run directly through the shared-library C API with four concurrent reader databases. Five fresh-process pairs alternated order, checked the same row/history results, and had no compiler or diagnostic overlap.

| Phase | Old ms | New ms | New/old |
| --- | ---: | ---: | ---: |
| Total mixed workload | 1388.12 | 916.91 | 0.661× |
| Initial attach | 28.48 | 28.65 | 1.006× |
| File-backed writes/deletes/compaction | 606.13 | 486.16 | 0.802× |
| Inline writes/deletes/flush | 527.77 | 296.87 | 0.563× |
| Four concurrent readers | 66.32 | 62.59 | 0.944× |

The table retains attach, file-backed and small-transaction costs even where they increase. The mixed integration workload improves in these runs. Individual many-table and cold-start measurements remain higher and variable; see the focused Linux checks below. Cold CLI/lifecycle tail parity and physical production Linux performance remain unproven.

### Linux ARM64 comparison

These measurements use native ARM64 execution in the dedicated six-CPU Linux VM on the same shared host, with FDB 7.4.6 inside the container. The baseline was rebuilt from repository `6cc0cb8`; the published v0.1.6 bundle failed before the workload with a catalog-ID decode error and is retained as a failed attempt, not used as a baseline. Both rebuilt baseline and candidate use Clang 21.

Three alternating native rounds and five mixed C API pairs passed every row/history assertion. The initial native results include substantial adverse samples; they are retained below. Values are medians of run medians, in milliseconds.

| Initial native operation | Old | New | New/old |
| --- | ---: | ---: | ---: |
| 1,000-file count | 90.13 | 73.20 | 0.812× |
| 1,000-file partition filter | 137.55 | 159.09 | 1.157× |
| 1,000-file column filter | 123.79 | 158.37 | 1.279× |
| 256-file dynamic join | 1099.08 | 1122.20 | 1.021× |
| Ten-table inline commit | 91.23 | 149.34 | 1.637× |
| Ten-table count after commit | 111.45 | 217.71 | 1.953× |
| Fifty-table inline flush | 2917.22 | 2354.84 | 0.807× |

| Mixed C API phase | Old ms | New ms | New/old |
| --- | ---: | ---: | ---: |
| Total mixed workload | 3259.71 | 2378.91 | 0.730× |
| File-backed writes/deletes/compaction | 1276.98 | 1233.46 | 0.966× |
| Inline writes/deletes/flush | 1389.13 | 720.99 | 0.519× |
| Four concurrent readers | 238.08 | 143.36 | 0.602× |

#### Focused write/read check

The large synthetic ten-table deltas did not reproduce in warmed reads: three alternating pairs with 256 unrelated files differed by about 2%, with faster commits. A coupled write/read experiment exposed large startup/order transients. Therefore the final repetition uses explicit provider warm-up (two old/new pairs, retained separately), then **five alternating measured pairs**. Each fresh process creates 256 three-column partitioned fact files, the dimension file, and fifty inline tables, using the original inline threshold of 100. It performs twenty ten-table commits, checking the ten counts immediately after every commit. Each version therefore has 100 measured commits and 100 checked post-write queries.

| Final coupled operation | Old mean ms | New mean ms | Total new/old | Median paired ratio |
| --- | ---: | ---: | ---: | ---: |
| Ten-table commit | 88.78 | 102.39 | 1.153× | 0.979× |
| Immediate ten-table counts | 117.15 | 131.41 | 1.122× | 0.979× |

The final series retains all five measured pairs: total commit time increased about 15% and immediate count time about 12%; the median of the five paired ratios was near parity. Totals divide the same 100 operations per version; paired ratios compare each adjacent old/new pair before taking the median. This difference reflects substantial run-to-run variation. The initial adverse timings remain evidence; the focused checks do not erase them or prove production latency parity. Source review found that old and new count paths both use the same typed file-list operation and parser; the new hook adds partial-snapshot exactness checks. Fresh mutation validation remains necessary for concurrency correctness. No speculative cache or weakened snapshot fence was added to improve these numbers.

An exploratory final warm-up was accidentally started before the preceding exploratory run exited. It was stopped, recorded as excluded, and restarted after both processes ended. The final exact series above has no such overlap; its provider warm-up was planned before collecting the five measured pairs. Raw runs and the exclusion record are retained.

## Changes and retained limits

The rebase preserves batched stats reads, metadata caching, pruning, byte guards, row identities, commit fencing and ID remapping. It removes superseded flush sort/partition/delete-position implementations and the obsolete Postgres pointer patch. New typed hooks replace new upstream SQL-only metadata paths. Inline statistics now describe their writing schema; the global owner applies ADD-time defaults and keeps unknown file/nested statistics conservative, fixing an independently reproduced baseline pruning bug. Mutation validation uses fresh catalog reads and invalidates the caller’s stale snapshot even after failure.

Version-1 schema decoding remains for existing persisted rows. Whole-view comments remain supported; new view-column tags are explicitly rejected. Two baseline-equivalent gaps remain documented with correct-result non-gating fixtures: DELETE plus inline flush in the same transaction can break historical reads, and changing partition specifications before flushing pending inline rows can fail flush. See the release runbook.

## Local evidence

Raw source hashes, timing samples, load records and diagnostics: `run-artifacts/duckdb-2-upgrade/performance/`. Linux builds use Clang 21 and checksum-pinned curl 8.23.0, because HTTPFS 2.0 needs newer APIs than UBI 9 provides. The packaged curl SONAME links and clean-runtime HTTP reads verify this dependency boundary. Native smoke, archive smoke, catalog tests, Clippy, old/new persistence and platform build logs: `run-artifacts/duckdb-2-upgrade/`. These local artifacts are intentionally ignored by Git.

The performance manifests record exact native library/runtime hashes and fixture hashes. ARM64 build/package gates passed. AMD64 compilation and an independent full-system package smoke passed; the Rosetta package smoke did not pass. Linux archives also passed the analytics downloader and HTTP proof. AMD64 compilation ran under Rosetta; its incorrect table-description result under Rosetta was not accepted. The unchanged package reports the correct columns and passes the complete smoke in a full-system QEMU x86 VM. This establishes independent functional proof, not physical AMD64 performance evidence. The slow VM also exposed an acknowledged-configuration timeout in the smoke harness; readiness now verifies a database read after failed configuration acknowledgement. Archive SHA-256 values:

- `aux-ducklake-fdb-v417a9117-linux-amd64.tar.gz` — SHA-256 `2a2054b2d8e6a83a1ebfd52069d7fb4f3dd9411659a57b7146cb332385372ea3`
- `aux-ducklake-fdb-v417a9117-linux-arm64.tar.gz` — SHA-256 `bbd66ac69faae62f5964ac62f8e10ef4535a6c406965e35e321dd137bb35bb10`
- `aux-ducklake-fdb-v417a9117-macos-arm64.tar.gz` — SHA-256 `d2d036d25d08142fa76829b5021af363c30ca55e2178a2db73326a034c22bdf9`
