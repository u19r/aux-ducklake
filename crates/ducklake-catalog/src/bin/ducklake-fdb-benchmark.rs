#[cfg(not(feature = "foundationdb"))]
fn main() {
    eprintln!("ducklake-fdb-benchmark requires `--features foundationdb`");
    std::process::exit(2);
}

#[cfg(feature = "foundationdb")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    fdb_benchmark::run()
}

#[cfg(feature = "foundationdb")]
mod fdb_benchmark {
    mod args;
    mod fixture_batches;
    mod json_artifact;

    use std::{
        env, fs,
        sync::{Arc, Barrier},
        thread,
        time::{Instant, SystemTime, UNIX_EPOCH},
    };

    use args::{Args, require_live_fdb};
    use ducklake_catalog::{
        AppendCommitResult, CatalogError, CatalogId, CatalogOrderId, ColumnId, CommitAttemptId,
        DataFileId, DataFileRow, FdbOrderedCatalogKv, SchemaId, TableColumnRow, TableId, TableRow,
        latest_snapshot, list_current_data_files, list_data_file_changes, list_data_files_at,
        shutdown_foundationdb_if_booted,
    };
    use fixture_batches::production_shape_batches;
    use json_artifact::{Artifact, Batch, BatchResult};

    const CATALOG: CatalogId = CatalogId(90);
    const SCAN_TABLE: TableId = TableId(10);
    const CONCURRENT_TABLE: TableId = TableId(20);
    const IDEMPOTENT_TABLE: TableId = TableId(30);
    const CONFLICT_TABLE: TableId = TableId(40);

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        require_live_fdb()?;
        let args = Args::parse()?;
        let started = Instant::now();
        let prefix = unique_prefix(&args.profile);
        let kv = Arc::new(open_fdb(prefix.clone())?);

        let initial = timed("initialize_catalog", || {
            let snapshot = kv.initialize_catalog_if_absent_versionstamped(CATALOG)?;
            Ok(BatchResult::new()
                .label("initial_sequence", snapshot.sequence)
                .operation("fdb_transactions", 1))
        })?;

        let create_tables = timed("create_tables", || {
            for (table, name) in [
                (SCAN_TABLE, "scan_probe"),
                (CONCURRENT_TABLE, "concurrent_probe"),
                (IDEMPOTENT_TABLE, "idempotent_probe"),
                (CONFLICT_TABLE, "conflict_probe"),
            ] {
                let latest = latest_snapshot(kv.as_ref(), CATALOG)?.ok_or_else(missing_snapshot)?;
                kv.create_table_versionstamped(
                    CATALOG,
                    TableRow::with_catalog_metadata(
                        table,
                        SchemaId(0),
                        format!("fdb-benchmark-{}", table.0),
                        name,
                        format!("main/{name}"),
                        vec![TableColumnRow::new(
                            ColumnId(1),
                            "id",
                            "INTEGER",
                            true,
                            None,
                        )],
                        latest.order,
                    ),
                    None,
                )?;
            }
            Ok(BatchResult::new()
                .label("created_tables", 4)
                .operation("fdb_transactions", 4))
        })?;

        let append = append_scan_files(kv.as_ref(), &args)?;
        let current_scan = timed("current_scan", || {
            let files = list_current_data_files(kv.as_ref(), CATALOG, SCAN_TABLE)?;
            Ok(BatchResult::new()
                .label(
                    "fdb_current_scan",
                    format!("{},{}", files.len(), sum_records(&files)),
                )
                .operation("current_scan_calls", 1)
                .operation("current_scan_files", files.len()))
        })?;
        let time_travel = time_travel_scan(kv.as_ref(), args.batch_size)?;
        let change_feed = timed("change_feed", || {
            let latest = latest_snapshot(kv.as_ref(), CATALOG)?.ok_or_else(missing_snapshot)?;
            let changes = list_data_file_changes(
                kv.as_ref(),
                CATALOG,
                SCAN_TABLE,
                append.base_order,
                latest.order,
            )?;
            Ok(BatchResult::new()
                .label(
                    "fdb_change_feed",
                    format!("{},{}", changes.len(), changes.len()),
                )
                .operation("change_feed_calls", 1)
                .operation("change_feed_rows", changes.len()))
        })?;
        let concurrent = concurrent_appends(Arc::clone(&kv), &args)?;
        let idempotency = commit_idempotency(kv.as_ref())?;
        let conflict = conflict_rejection(kv.as_ref())?;

        let mut batches = vec![
            initial,
            create_tables,
            append.batch,
            current_scan,
            time_travel,
            change_feed,
            concurrent,
            idempotency,
            conflict,
        ];
        batches.extend(production_shape_batches(kv.as_ref(), CATALOG, &args)?);
        let artifact = Artifact {
            profile: args.profile.clone(),
            generated_at_micros: now_micros()?,
            elapsed_ms: elapsed_ms(started),
            key_prefix: prefix,
            fixture: args.fixture(),
            batches,
        };
        fs::create_dir_all(args.output.parent().ok_or("output path has no parent")?)?;
        fs::write(&args.output, artifact.to_json())?;
        println!(
            "ducklake_fdb_feature_parity_benchmark_artifact={}",
            args.output.display()
        );
        drop(kv);
        shutdown_foundationdb_if_booted();
        Ok(())
    }

    fn append_scan_files(
        kv: &FdbOrderedCatalogKv,
        args: &Args,
    ) -> Result<AppendScanResult, Box<dyn std::error::Error>> {
        let base_order = latest_snapshot(kv, CATALOG)?
            .ok_or_else(missing_snapshot)?
            .order;
        let mut first_batch_order = None;
        let started = Instant::now();
        let mut max_batch_bytes = 0usize;
        let mut next_id = 1_u64;
        let mut committed = 0usize;

        while committed < args.scan_files {
            let batch_len = args.batch_size.min(args.scan_files - committed);
            let mut rows = Vec::with_capacity(batch_len);
            for _ in 0..batch_len {
                rows.push(data_file(
                    next_id,
                    SCAN_TABLE,
                    format!("scan-{next_id}.parquet"),
                    1,
                ));
                next_id = next_id.saturating_add(1);
            }
            max_batch_bytes = max_batch_bytes.max(rows.iter().map(|row| row.encode().len()).sum());
            let appended = kv.append_data_files_versionstamped(CATALOG, rows)?;
            if first_batch_order.is_none() {
                first_batch_order = appended.first().map(|row| row.validity.begin_order);
            }
            committed += appended.len();
        }

        Ok(AppendScanResult {
            base_order,
            batch: Batch {
                name: "append_scan_files".to_owned(),
                duration_ms: elapsed_ms(started),
                labels: vec![("fdb_append_scan_files".to_owned(), committed.to_string())],
                operation_counts: vec![
                    (
                        "fdb_transactions".to_owned(),
                        ceil_div(args.scan_files, args.batch_size) as u64,
                    ),
                    ("appended_files".to_owned(), committed as u64),
                ],
                transaction_estimates: vec![
                    (
                        "max_files_per_transaction".to_owned(),
                        args.batch_size.to_string(),
                    ),
                    (
                        "max_encoded_row_bytes_per_transaction".to_owned(),
                        max_batch_bytes.to_string(),
                    ),
                    (
                        "first_batch_order".to_owned(),
                        first_batch_order.ok_or_else(missing_snapshot)?.to_string(),
                    ),
                ],
            },
        })
    }

    fn time_travel_scan(
        kv: &FdbOrderedCatalogKv,
        expected_files: usize,
    ) -> Result<Batch, Box<dyn std::error::Error>> {
        timed("time_travel_scan", || {
            let changes = list_data_file_changes(
                kv,
                CATALOG,
                SCAN_TABLE,
                CatalogOrderId::uuid_v7(0),
                latest_snapshot(kv, CATALOG)?
                    .ok_or_else(missing_snapshot)?
                    .order,
            )?;
            let first_order = changes
                .iter()
                .map(|change| change.order)
                .min()
                .ok_or_else(missing_snapshot)?;
            let files = list_data_files_at(kv, CATALOG, SCAN_TABLE, first_order)?;
            Ok(BatchResult::new()
                .label(
                    "fdb_time_travel_scan",
                    format!("{},{}", files.len(), sum_records(&files)),
                )
                .operation("time_travel_scan_calls", 1)
                .operation("time_travel_scan_files", files.len())
                .estimate("expected_first_batch_files", expected_files))
        })
    }

    fn concurrent_appends(
        kv: Arc<FdbOrderedCatalogKv>,
        args: &Args,
    ) -> Result<Batch, Box<dyn std::error::Error>> {
        let started = Instant::now();
        let latest = latest_snapshot(kv.as_ref(), CATALOG)?
            .ok_or_else(missing_snapshot)?
            .order;
        let barrier = Arc::new(Barrier::new(args.concurrent_writers + 1));
        let mut handles = Vec::with_capacity(args.concurrent_writers);
        for writer_index in 0..args.concurrent_writers {
            let writer_kv = Arc::clone(&kv);
            let writer_barrier = Arc::clone(&barrier);
            let writer_files = args.writer_files;
            handles.push(thread::spawn(move || {
                writer_barrier.wait();
                let start = 1_000_000 + (writer_index as u64 * 10_000);
                let rows = (0..writer_files)
                    .map(|offset| {
                        data_file(
                            start + offset as u64,
                            CONCURRENT_TABLE,
                            format!("writer-{writer_index}-{offset}.parquet"),
                            1,
                        )
                    })
                    .collect::<Vec<_>>();
                writer_kv.append_data_files_versionstamped(CATALOG, rows)
            }));
        }
        barrier.wait();
        let mut appended = 0usize;
        for handle in handles {
            let rows = handle
                .join()
                .map_err(|_| "concurrent FDB writer panicked")??;
            appended += rows.len();
        }
        let files = list_current_data_files(kv.as_ref(), CATALOG, CONCURRENT_TABLE)?;
        let visible_new_files = files
            .iter()
            .filter(|file| file.validity.begin_order > latest)
            .count();
        if visible_new_files != appended {
            return Err(format!(
                "concurrent FDB append mismatch: appended {appended}, visible {visible_new_files}"
            )
            .into());
        }
        Ok(Batch {
            name: "concurrent_appends".to_owned(),
            duration_ms: elapsed_ms(started),
            labels: vec![(
                "fdb_concurrent_append".to_owned(),
                format!("{},{}", args.concurrent_writers, appended),
            )],
            operation_counts: vec![
                (
                    "fdb_transactions".to_owned(),
                    args.concurrent_writers as u64,
                ),
                (
                    "concurrent_writers".to_owned(),
                    args.concurrent_writers as u64,
                ),
                ("appended_files".to_owned(), appended as u64),
            ],
            transaction_estimates: vec![(
                "files_per_writer".to_owned(),
                args.writer_files.to_string(),
            )],
        })
    }

    fn commit_idempotency(kv: &FdbOrderedCatalogKv) -> Result<Batch, Box<dyn std::error::Error>> {
        timed("commit_idempotency", || {
            let latest = latest_snapshot(kv, CATALOG)?.ok_or_else(missing_snapshot)?;
            let row = data_file(2_000_001, IDEMPOTENT_TABLE, "idempotent.parquet", 1);
            let first = kv.commit_append_data_file_versionstamped(
                CATALOG,
                CommitAttemptId(2_000_001),
                latest.order,
                latest.order,
                row.clone(),
            )?;
            let second = kv.commit_append_data_file_versionstamped(
                CATALOG,
                CommitAttemptId(2_000_001),
                latest.order,
                latest.order,
                row,
            )?;
            let label = match (first, second) {
                (AppendCommitResult::Committed(_), AppendCommitResult::AlreadyCommitted { .. }) => {
                    "committed,already_committed"
                }
                _ => return Err("unexpected idempotent append result".into()),
            };
            Ok(BatchResult::new()
                .label("fdb_commit_idempotency", label)
                .operation("fdb_transactions", 2)
                .operation("commit_attempts", 2))
        })
    }

    fn conflict_rejection(kv: &FdbOrderedCatalogKv) -> Result<Batch, Box<dyn std::error::Error>> {
        timed("conflict_rejection", || {
            let base = kv
                .append_data_files_versionstamped(
                    CATALOG,
                    vec![data_file(
                        3_000_001,
                        CONFLICT_TABLE,
                        "conflict-base.parquet",
                        1,
                    )],
                )?
                .into_iter()
                .next()
                .ok_or("missing conflict base file")?;
            let expired = kv.expire_data_file_versionstamped(CATALOG, base.data_file_id)?;
            let through = expired.validity.end_order.ok_or("missing expire order")?;
            let error = kv
                .commit_append_data_file_versionstamped(
                    CATALOG,
                    CommitAttemptId(3_000_001),
                    base.validity.begin_order,
                    through,
                    data_file(3_000_002, CONFLICT_TABLE, "should-conflict.parquet", 1),
                )
                .err()
                .ok_or("conflicting append unexpectedly succeeded")?;
            match error {
                CatalogError::LogicalConflict { .. } => Ok(BatchResult::new()
                    .label("fdb_conflict", "logical_conflict")
                    .operation("conflict_attempts", 1)
                    .operation("rejected_commits", 1)),
                other => Err(format!("unexpected conflict error: {other}").into()),
            }
        })
    }

    fn timed(
        name: &str,
        run: impl FnOnce() -> Result<BatchResult, Box<dyn std::error::Error>>,
    ) -> Result<Batch, Box<dyn std::error::Error>> {
        let started = Instant::now();
        let result = run()?;
        Ok(Batch {
            name: name.to_owned(),
            duration_ms: elapsed_ms(started),
            labels: result.labels,
            operation_counts: result.operation_counts,
            transaction_estimates: result.transaction_estimates,
        })
    }

    fn data_file(id: u64, table: TableId, path: impl Into<String>, records: u64) -> DataFileRow {
        DataFileRow::new(
            DataFileId(id),
            table,
            path,
            records,
            records.saturating_mul(10),
            CatalogOrderId::uuid_v7(0),
        )
    }

    fn open_fdb(prefix: Vec<u8>) -> Result<FdbOrderedCatalogKv, CatalogError> {
        let cluster_file = env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE").ok();
        FdbOrderedCatalogKv::open_with_prefix(cluster_file.as_deref(), prefix)
    }

    fn unique_prefix(profile: &str) -> Vec<u8> {
        format!(
            "aux-ducklake-benchmark/{}/{}/{}",
            profile,
            std::process::id(),
            now_micros().unwrap_or(0)
        )
        .into_bytes()
    }

    fn missing_snapshot() -> CatalogError {
        CatalogError::NotFound("snapshot")
    }

    fn sum_records(files: &[DataFileRow]) -> u64 {
        files.iter().map(|file| file.record_count).sum()
    }

    fn elapsed_ms(started: Instant) -> f64 {
        started.elapsed().as_secs_f64() * 1000.0
    }

    fn now_micros() -> Result<u128, std::time::SystemTimeError> {
        Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())
    }

    fn ceil_div(left: usize, right: usize) -> usize {
        left.saturating_add(right.saturating_sub(1)) / right
    }

    struct AppendScanResult {
        base_order: CatalogOrderId,
        batch: Batch,
    }
}
