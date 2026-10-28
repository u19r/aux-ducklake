use super::{
    accumulator::{consume_inline_payload_lines, reconstruct_inline_current_rows_streaming},
    capture_cache::{ByteBoundedCache, CaptureFlightRegistry},
    history::InlineHistoryRequest,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use super::capture::{
    CaptureKey, CapturedInlineRows, CapturedInlineRowsBySchema, MAX_CAPTURE_CACHE_BYTES,
    capture_bounds_for_bytes, capture_weight, captured_inline_current_rows_or_latest,
};
use crate::{
    CatalogCacheNamespace, CatalogError, CatalogId, CatalogOrderId, DataFileId, DeleteFileId,
    FakeOrderedCatalogKv, InlineRowChangeKind, KvBatch, RawSnapshotSequence, SchemaId, TableId,
    ValidityWindow,
    inline_data::{
        INLINE_PAYLOAD_LIMIT_BYTES, InlineCurrentRow, InlineTableChunkRow, inline_table_chunk_key,
    },
    keys::{data_file_end_key, delete_file_end_key, inline_table_end_key},
};

#[test]
fn concurrent_capture_misses_build_once_and_share_arc() {
    const WORKERS: usize = 8;
    let registry = Arc::new(CaptureFlightRegistry::<u64, usize>::new());
    let entered = Arc::new(Barrier::new(WORKERS + 1));
    let started = Arc::new(Barrier::new(WORKERS + 1));
    let builders = Arc::new(AtomicUsize::new(0));
    let build_gate = Arc::new(Barrier::new(2));
    let pointers = Arc::new(Mutex::new(Vec::with_capacity(WORKERS)));
    let mut workers = Vec::with_capacity(WORKERS);

    for _ in 0..WORKERS {
        let registry = Arc::clone(&registry);
        let entered = Arc::clone(&entered);
        let started = Arc::clone(&started);
        let builders = Arc::clone(&builders);
        let build_gate = Arc::clone(&build_gate);
        let pointers = Arc::clone(&pointers);
        workers.push(thread::spawn(move || {
            entered.wait();
            let (flight, is_builder) = registry.start(7).unwrap();
            started.wait();
            let capture = if is_builder {
                builders.fetch_add(1, Ordering::SeqCst);
                build_gate.wait();
                let capture = Arc::new(42usize);
                registry.complete(&7, &flight, Ok(Arc::clone(&capture)))?
            } else {
                flight.wait()?
            };
            pointers
                .lock()
                .unwrap()
                .push(Arc::as_ptr(&capture) as usize);
            assert_eq!(*capture, 42);
            Ok::<(), CatalogError>(())
        }));
    }

    entered.wait();
    started.wait();
    build_gate.wait();
    for worker in workers {
        worker.join().unwrap().unwrap();
    }

    assert_eq!(builders.load(Ordering::SeqCst), 1);
    let pointers = pointers.lock().unwrap();
    assert_eq!(pointers.len(), WORKERS);
    assert!(pointers.windows(2).all(|pair| pair[0] == pair[1]));
}

#[test]
fn delayed_capture_miss_rechecks_cache_after_flight_completion() {
    let registry = Arc::new(CaptureFlightRegistry::<u64, usize>::new());
    let cache = Arc::new(ByteBoundedCache::new(10));
    let observed_miss = Arc::new(Barrier::new(2));
    let release_delayed_worker = Arc::new(Barrier::new(2));
    let builders = Arc::new(AtomicUsize::new(0));
    let key = 13_u64;

    let delayed_registry = Arc::clone(&registry);
    let delayed_cache = Arc::clone(&cache);
    let delayed_observed_miss = Arc::clone(&observed_miss);
    let delayed_release = Arc::clone(&release_delayed_worker);
    let delayed_builders = Arc::clone(&builders);
    let delayed_worker = thread::spawn(move || {
        assert!(delayed_cache.get(&key).is_none());
        delayed_observed_miss.wait();
        delayed_release.wait();
        delayed_registry
            .get_or_build(key, || {
                if let Some(capture) = delayed_cache.get(&key) {
                    return Ok(capture);
                }
                delayed_builders.fetch_add(1, Ordering::SeqCst);
                let capture = Arc::new(42usize);
                assert!(delayed_cache.insert_shared(key, Arc::clone(&capture), 1, true));
                Ok(capture)
            })
            .unwrap()
    });

    observed_miss.wait();
    let first = registry
        .get_or_build(key, || {
            builders.fetch_add(1, Ordering::SeqCst);
            let capture = Arc::new(42usize);
            assert!(cache.insert_shared(key, Arc::clone(&capture), 1, true));
            Ok(capture)
        })
        .unwrap();
    release_delayed_worker.wait();

    let delayed = delayed_worker.join().unwrap();
    assert_eq!(builders.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(&first, &delayed));
}

#[test]
fn failed_capture_wakes_waiters_and_allows_retry() {
    const WORKERS: usize = 8;
    let registry = Arc::new(CaptureFlightRegistry::<u64, usize>::new());
    let entered = Arc::new(Barrier::new(WORKERS + 1));
    let started = Arc::new(Barrier::new(WORKERS + 1));
    let builders = Arc::new(AtomicUsize::new(0));
    let build_gate = Arc::new(Barrier::new(2));
    let errors = Arc::new(Mutex::new(Vec::with_capacity(WORKERS)));
    let mut workers = Vec::with_capacity(WORKERS);

    for _ in 0..WORKERS {
        let registry = Arc::clone(&registry);
        let entered = Arc::clone(&entered);
        let started = Arc::clone(&started);
        let builders = Arc::clone(&builders);
        let build_gate = Arc::clone(&build_gate);
        let errors = Arc::clone(&errors);
        workers.push(thread::spawn(move || {
            entered.wait();
            let (flight, is_builder) = registry.start(9).unwrap();
            started.wait();
            let result = if is_builder {
                builders.fetch_add(1, Ordering::SeqCst);
                build_gate.wait();
                registry.complete(
                    &9,
                    &flight,
                    Err(CatalogError::Backend("capture failed".to_owned())),
                )
            } else {
                flight.wait()
            };
            errors.lock().unwrap().push(
                result
                    .map(|capture| *capture)
                    .map_err(|error| error.to_string()),
            );
        }));
    }

    entered.wait();
    started.wait();
    build_gate.wait();
    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(builders.load(Ordering::SeqCst), 1);
    let errors = errors.lock().unwrap();
    assert_eq!(errors.len(), WORKERS);
    assert!(errors.iter().all(|result| {
        matches!(result, Err(message) if message == "backend failed: capture failed")
    }));
    drop(errors);

    let retry = registry.get_or_build(9, || Ok(Arc::new(73usize))).unwrap();
    assert_eq!(*retry, 73);
}

#[test]
fn different_capture_keys_build_concurrently() {
    let registry = Arc::new(CaptureFlightRegistry::<u64, usize>::new());
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for key in [11_u64, 12_u64] {
        let registry = Arc::clone(&registry);
        let started_tx = started_tx.clone();
        let release = Arc::clone(&release);
        workers.push(thread::spawn(move || {
            registry.get_or_build(key, || {
                started_tx.send(key).unwrap();
                while !release.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                Ok(Arc::new(key as usize))
            })
        }));
    }
    drop(started_tx);

    let first = started_rx.recv_timeout(Duration::from_secs(1));
    let second = started_rx.recv_timeout(Duration::from_secs(1));
    release.store(true, Ordering::Release);
    for worker in workers {
        worker.join().unwrap().unwrap();
    }
    assert!(first.is_ok(), "first capture builder did not start");
    assert!(
        second.is_ok(),
        "second capture builder waited on another key"
    );
}

#[test]
fn byte_bound_evicts_least_recently_used_entries() {
    let cache = ByteBoundedCache::new(10);
    assert!(cache.insert(1, "first", 4, false));
    assert!(cache.insert(2, "second", 4, false));
    assert_eq!(cache.get(&1).as_deref(), Some(&"first"));

    assert!(cache.insert(3, "third", 4, false));

    assert_eq!(cache.get(&1).as_deref(), Some(&"first"));
    assert_eq!(cache.get(&2), None);
    assert_eq!(cache.get(&3).as_deref(), Some(&"third"));
    assert!(cache.total_bytes() <= 10);
}

#[test]
fn byte_bound_does_not_cache_an_entry_larger_than_the_total_bound() {
    let cache = ByteBoundedCache::new(10);

    assert!(!cache.insert(1, "too-large", 11, false));

    assert_eq!(cache.get(&1), None);
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.total_bytes(), 0);
}

#[test]
fn retaining_context_entries_releases_their_weight() {
    let cache = ByteBoundedCache::new(10);
    assert!(cache.insert(1, "first", 6, false));
    assert!(cache.insert(2, "second", 4, false));

    cache.retain(|key| *key != 1);

    assert_eq!(cache.get(&1), None);
    assert_eq!(cache.get(&2).as_deref(), Some(&"second"));
    assert_eq!(cache.total_bytes(), 4);
}

#[test]
fn canonical_payload_bound_supports_one_hundred_inline_rows() {
    let mut rows = Vec::with_capacity(100);
    for row_id in 0_u64..100 {
        rows.push((
            row_id,
            InlineCurrentRow::new(
                CatalogOrderId::uuid_v7(row_id as u128),
                RawSnapshotSequence(row_id),
                vec![b'x'; INLINE_PAYLOAD_LIMIT_BYTES],
            ),
        ));
    }
    let mut by_schema = BTreeMap::new();
    by_schema.insert(
        SchemaId(0),
        CapturedInlineRows {
            rows,
            next_row_id: 100,
            ..CapturedInlineRows::default()
        }
        .into(),
    );
    let mut capture = CapturedInlineRowsBySchema { rows: by_schema };
    let row_weight = capture_weight(&capture).unwrap();
    let derived = Arc::get_mut(capture.rows.get_mut(&SchemaId(0)).unwrap()).unwrap();
    derived.global_stats = String::with_capacity(1024);
    derived.aggregate_stats = String::with_capacity(2048);
    let derived_bytes = derived.global_stats.capacity() + derived.aggregate_stats.capacity();
    let weight = capture_weight(&capture).unwrap();

    assert_eq!(weight, row_weight + derived_bytes);
    assert!(weight >= 100 * INLINE_PAYLOAD_LIMIT_BYTES);
    assert!(weight < MAX_CAPTURE_CACHE_BYTES);
}

#[test]
fn canonical_payload_bound_supports_one_hundred_rows_per_schema_version() {
    let mut by_schema = BTreeMap::new();
    for schema_id in [SchemaId(0), SchemaId(1)] {
        let mut rows = Vec::with_capacity(100);
        for row_id in 0_u64..100 {
            rows.push((
                row_id,
                InlineCurrentRow::new(
                    CatalogOrderId::uuid_v7((schema_id.0 as u128) << 64 | row_id as u128),
                    RawSnapshotSequence(row_id),
                    vec![b'x'; INLINE_PAYLOAD_LIMIT_BYTES],
                ),
            ));
        }
        by_schema.insert(
            schema_id,
            CapturedInlineRows {
                rows,
                next_row_id: 100,
                ..CapturedInlineRows::default()
            }
            .into(),
        );
    }
    let weight = capture_weight(&CapturedInlineRowsBySchema { rows: by_schema }).unwrap();

    assert!(weight >= 2 * 100 * INLINE_PAYLOAD_LIMIT_BYTES);
    assert!(weight < MAX_CAPTURE_CACHE_BYTES);
}

#[test]
fn routing_row_limit_does_not_reject_a_valid_overshoot_batch() {
    let routing_row_limit = 10;
    let begin_order = CatalogOrderId::uuid_v7(10);
    let mut payload = Vec::new();
    for row_id in 0_u64..11 {
        payload.extend_from_slice(format!("row\t{row_id}\tvalue\n").as_bytes());
    }

    let captured = reconstruct_inline_current_rows_streaming(
        |accumulator| consume_inline_payload_lines(begin_order, &payload, accumulator),
        begin_order,
        &BTreeMap::from([(begin_order, 101)]),
        &BTreeMap::new(),
        capture_bounds_for_bytes(MAX_CAPTURE_CACHE_BYTES).0,
        MAX_CAPTURE_CACHE_BYTES,
        MAX_CAPTURE_CACHE_BYTES,
    )
    .expect("the routing threshold must not be a capture row-count bound");

    assert!(routing_row_limit < captured.rows.len());
    assert_eq!(captured.rows.len(), 11);
}

#[test]
fn streaming_reconstruction_folds_large_delete_history_into_bounded_state() {
    const CHURN: u64 = 10_000;
    let snapshot_order = CatalogOrderId::uuid_v7((CHURN * 2 + 1) as u128);
    let final_order = snapshot_order;
    let final_row_id = 77_777;
    let captured = reconstruct_inline_current_rows_streaming(
        |accumulator| {
            for index in 0..CHURN {
                let begin_order = CatalogOrderId::uuid_v7(index as u128 * 2 + 1);
                let delete_order = CatalogOrderId::uuid_v7(index as u128 * 2 + 2);
                accumulator.insert_payload_row(
                    begin_order,
                    index,
                    format!("row\t{index}\ttransient-{index}").into_bytes(),
                )?;
                accumulator.remove_deleted(index, delete_order);
            }
            accumulator.insert_payload_row(final_order, final_row_id, b"row\t77777\tfinal".to_vec())
        },
        snapshot_order,
        &BTreeMap::from([(final_order, 42)]),
        &BTreeMap::new(),
        1,
        1024,
        MAX_CAPTURE_CACHE_BYTES,
    )
    .expect("row churn should fold without retaining deleted history");

    assert_eq!(captured.next_row_id, final_row_id + 1);
    assert_eq!(captured.rows.len(), 1);
    assert_eq!(captured.rows[0].0, final_row_id);
    assert_eq!(captured.rows[0].1.begin_sequence, RawSnapshotSequence(42));
    assert_eq!(captured.rows[0].1.payload, b"row\t77777\tfinal");
}

#[test]
fn streaming_reconstruction_folds_flush_history_before_the_next_payload_group() {
    const CHURN: u64 = 2_000;
    let catalog = CatalogId(90);
    let table_id = TableId(91);
    let schema_id = SchemaId(92);
    let snapshot_order = CatalogOrderId::uuid_v7((CHURN * 2 + 1) as u128);
    let final_row_id = 88_888;
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    for index in 0..CHURN {
        let begin_order = CatalogOrderId::uuid_v7(index as u128 * 2 + 1);
        // A single flush end order closes every payload group in the batch.
        // The capture must consult those markers before retaining the groups;
        // applying them in a later pass would exceed the one-row bound.
        let end_order = snapshot_order;
        let chunk = InlineTableChunkRow::new(
            table_id,
            schema_id,
            ValidityWindow::new(begin_order, None),
            0,
            1,
            format!("row\t{index}\tflushed-{index}\n").into_bytes(),
        );
        batch.put(
            inline_table_chunk_key(catalog, table_id, schema_id, begin_order, 0),
            chunk.encode(),
        );
        batch.put(
            inline_table_end_key(catalog, table_id, end_order, schema_id, begin_order),
            begin_order.as_bytes().to_vec(),
        );
    }
    let final_begin_order = snapshot_order;
    let final_chunk = InlineTableChunkRow::new(
        table_id,
        schema_id,
        ValidityWindow::new(final_begin_order, None),
        0,
        1,
        format!("row\t{final_row_id}\tfinal\n").into_bytes(),
    );
    batch.put(
        inline_table_chunk_key(catalog, table_id, schema_id, final_begin_order, 0),
        final_chunk.encode(),
    );
    // EndOrder is a shared table range. These ordinary sibling markers sort
    // before the inline marker and must be skipped, not treated as EOF.
    let sibling_end_order = CatalogOrderId::uuid_v7(0);
    batch.put(
        data_file_end_key(catalog, table_id, sibling_end_order, DataFileId(1)),
        Vec::new(),
    );
    batch.put(
        delete_file_end_key(catalog, table_id, sibling_end_order, DeleteFileId(1)),
        Vec::new(),
    );
    kv.commit(batch).unwrap();

    let captured = reconstruct_inline_current_rows_streaming(
        |accumulator| {
            InlineHistoryRequest {
                catalog,
                table: table_id,
                schema: schema_id,
                snapshot: snapshot_order,
            }
            .stream(&kv, accumulator)
        },
        snapshot_order,
        &BTreeMap::from([(final_begin_order, 73)]),
        &BTreeMap::new(),
        1,
        MAX_CAPTURE_CACHE_BYTES,
        MAX_CAPTURE_CACHE_BYTES,
    )
    .expect("flush markers must release each history group before the next one");

    assert_eq!(captured.rows.len(), 1);
    assert_eq!(captured.rows[0].0, final_row_id);
    assert_eq!(captured.rows[0].1.begin_sequence, RawSnapshotSequence(73));
    assert_eq!(captured.rows[0].1.payload, b"row\t88888\tfinal");
}

#[test]
fn reconstructs_old_logical_snapshot_without_historical_fdb_version() {
    let catalog = CatalogId(1);
    let table_id = TableId(1);
    let schema_id = SchemaId(2);
    let first_order = CatalogOrderId::uuid_v7(1);
    let second_order = CatalogOrderId::uuid_v7(2);
    let sequences = BTreeMap::from([(first_order, 101), (second_order, 102)]);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    for (order, row_id, value) in [(first_order, 42, "value"), (second_order, 43, "later")] {
        let chunk = InlineTableChunkRow::new(
            table_id,
            schema_id,
            ValidityWindow::new(order, None),
            0,
            1,
            format!("row\t{row_id}\t{value}\n").into_bytes(),
        );
        batch.put(
            inline_table_chunk_key(catalog, table_id, schema_id, order, 0),
            chunk.encode(),
        );
    }
    batch.put(
        crate::keys::table_schema_kind_inline_row_change_key(
            catalog,
            table_id,
            schema_id,
            InlineRowChangeKind::Deleted,
            second_order,
            42,
        ),
        Vec::new(),
    );
    kv.commit(batch).unwrap();

    let captured = reconstruct_inline_current_rows_streaming(
        |accumulator| {
            InlineHistoryRequest {
                catalog,
                table: table_id,
                schema: schema_id,
                snapshot: first_order,
            }
            .stream(&kv, accumulator)
        },
        first_order,
        &sequences,
        &BTreeMap::new(),
        100,
        MAX_CAPTURE_CACHE_BYTES,
        MAX_CAPTURE_CACHE_BYTES,
    )
    .unwrap();

    assert_eq!(captured.next_row_id, 43);
    assert_eq!(captured.rows.len(), 1);
    assert_eq!(captured.rows[0].0, 42);
    assert_eq!(captured.rows[0].1.begin_sequence, RawSnapshotSequence(101));
    assert_eq!(captured.rows[0].1.payload, b"row\t42\tvalue");
}

#[test]
fn reconstructs_canonical_payload_with_current_row_sequence_when_snapshot_index_misses() {
    let begin_order = CatalogOrderId::uuid_v7(11);
    let current_rows = BTreeMap::from([(42, (begin_order, RawSnapshotSequence(101)))]);

    let captured = reconstruct_inline_current_rows_streaming(
        |accumulator| consume_inline_payload_lines(begin_order, b"row\t42\tvalue\n", accumulator),
        begin_order,
        &BTreeMap::new(),
        &current_rows,
        100,
        MAX_CAPTURE_CACHE_BYTES,
        MAX_CAPTURE_CACHE_BYTES,
    )
    .unwrap();

    assert_eq!(captured.rows.len(), 1);
    assert_eq!(captured.rows[0].1.begin_order, begin_order);
    assert_eq!(captured.rows[0].1.begin_sequence, RawSnapshotSequence(101));
}

#[test]
fn reconstruct_rejects_payload_without_any_canonical_sequence() {
    let begin_order = CatalogOrderId::uuid_v7(12);
    let result = reconstruct_inline_current_rows_streaming(
        |accumulator| consume_inline_payload_lines(begin_order, b"row\t42\tvalue\n", accumulator),
        begin_order,
        &BTreeMap::new(),
        &BTreeMap::new(),
        100,
        MAX_CAPTURE_CACHE_BYTES,
        MAX_CAPTURE_CACHE_BYTES,
    );

    assert!(matches!(
        result,
        Err(CatalogError::Decode(message))
            if message == format!(
                "inline payload begin order {begin_order} has no canonical snapshot sequence"
            )
    ));
}

#[test]
fn pinned_context_capture_survives_pressure_and_fails_closed() {
    let cache = ByteBoundedCache::new(10);
    assert!(cache.insert(1, (), 6, true));
    assert!(cache.insert(2, (), 4, false));

    // An active capture is pinned until EndRuntimeReadContext. The unpinned entry may
    // be evicted, but a new active capture is rejected rather than evicting
    // the pinned value.
    assert!(!cache.insert(3, (), 5, true));
    assert!(cache.get(&1).is_some());
    assert_eq!(cache.get(&2), None);
    assert_eq!(cache.get(&3), None);
    assert_eq!(cache.total_bytes(), 6);
}

#[test]
fn active_context_cache_eviction_fails_closed_without_latest_fallback() {
    let cache = ByteBoundedCache::new(1);
    assert!(cache.insert(1, (), 1, false));
    assert!(cache.insert(2, (), 1, false));
    assert_eq!(cache.get(&1), None);

    let mut fallback_called = false;
    let mut request = crate::store::begin_runtime_read_request(Some(91));
    let result = captured_inline_current_rows_or_latest(None, true, || {
        fallback_called = true;
        Ok(Some((Vec::<(u64, InlineCurrentRow)>::new(), 0)))
    });
    assert!(matches!(
        result,
        Err(CatalogError::Backend(message))
            if message == "inline read snapshot capture is unavailable for the active runtime read context"
    ));
    assert!(!fallback_called);
    request.end_context();
}

#[test]
fn no_context_cache_miss_keeps_latest_fallback() {
    let mut fallback_called = false;
    let result = captured_inline_current_rows_or_latest(None, false, || {
        fallback_called = true;
        Ok(Some((Vec::<(u64, InlineCurrentRow)>::new(), 17)))
    })
    .unwrap();

    assert!(fallback_called);
    assert_eq!(result.map(|(_, next_row_id)| next_row_id), Some(17));
}

#[test]
fn context_cleanup_requires_namespace_catalog_and_context_identity() {
    let namespace = CatalogCacheNamespace::foundationdb(1, 2).with_read_context(Some(9));
    let other_namespace = CatalogCacheNamespace::foundationdb(3, 4).with_read_context(Some(9));
    let other_catalog = CatalogId(8);
    let key = CaptureKey {
        namespace,
        catalog: CatalogId(7),
        read_context_id: 9,
        table_id: TableId(1),
        snapshot_order: CatalogOrderId::uuid_v7(1),
    };

    assert!(key.belongs_to(namespace, CatalogId(7), 9));
    assert!(!key.belongs_to(other_namespace, CatalogId(7), 9));
    assert!(!key.belongs_to(namespace, other_catalog, 9));
    assert!(!key.belongs_to(namespace, CatalogId(7), 10));
}

#[derive(Clone)]
struct HistoryCapturePause {
    entered: mpsc::SyncSender<()>,
    resume: Arc<Mutex<mpsc::Receiver<()>>>,
}

static HISTORY_CAPTURE_PAUSES: Mutex<BTreeMap<CatalogCacheNamespace, HistoryCapturePause>> =
    Mutex::new(BTreeMap::new());

pub(super) fn pause_history_capture(namespace: CatalogCacheNamespace) {
    let pause = HISTORY_CAPTURE_PAUSES
        .lock()
        .unwrap()
        .get(&namespace)
        .cloned();
    if let Some(pause) = pause {
        pause.entered.send(()).unwrap();
        pause
            .resume
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
    }
}

#[test]
fn given_live_cleanup_during_historical_capture_when_first_read_runs_then_rows_remain_pinned() {
    if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
        return;
    }
    use crate::{
        ColumnId, FdbOrderedCatalogKv, InlinedTableRow, MutableCatalogKv, OrderedCatalogKv,
        TableColumnRow, TableRow,
    };
    let catalog = CatalogId(987_063);
    let table_id = TableId(1);
    let schema = SchemaId(1);
    let context = 70_103;
    let prefix = format!(
        "aux-ducklake-test/capture-cleanup/{}/{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let mut table = TableRow::with_catalog_metadata(
        table_id,
        SchemaId(0),
        "capture-race",
        "capture_race",
        "main/capture_race",
        vec![TableColumnRow::new(
            ColumnId(1),
            "value",
            "INTEGER",
            false,
            None,
        )],
        CatalogOrderId::uuid_v7(0),
    );
    table = kv
        .create_table_versionstamped(catalog, table, None)
        .unwrap();
    table
        .inlined_data_tables
        .push(InlinedTableRow::new("ducklake_inlined_data_1_1", 1));
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        table.clone(),
        schema,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let snapshot = {
        let _request = crate::store::begin_runtime_read_request(Some(context));
        crate::latest_snapshot(&kv, catalog).unwrap().unwrap()
    };
    kv.commit_delete_inline_table_rows_versionstamped(catalog, table_id, schema, &[0], None)
        .unwrap();
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        table,
        schema,
        b"row\t0\ti:2\n".to_vec(),
    )
    .unwrap();
    let namespace = kv
        .catalog_cache_namespace()
        .with_read_context(Some(context));
    // Exercise historical reconstruction after a physical lease renewal.
    crate::runtime_read_versions::end_context(namespace, context);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    HISTORY_CAPTURE_PAUSES.lock().unwrap().insert(
        namespace,
        HistoryCapturePause {
            entered: entered_tx,
            resume: Arc::new(Mutex::new(resume_rx)),
        },
    );
    let reader = kv.clone();
    let worker = thread::spawn(move || {
        let _request = crate::store::begin_runtime_read_request(Some(context));
        let payload = crate::runtime_inline_rows::read_foundationdb_inline_rows_payload(
            &reader,
            catalog,
            crate::runtime_inline_rows::ReadInlineRowsPayload {
                table_name: "ducklake_inlined_data_1_1".to_owned(),
                snapshot: Some(crate::runtime_snapshot_range::ReadSnapshot::new(
                    crate::DuckLakeSnapshotId(snapshot.sequence.0),
                )),
                include_flushed: false,
                include_deleted: false,
            },
        );
        super::capture::end_runtime_read_context(&reader, catalog, context);
        payload
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut cleanup = KvBatch::new();
    for prefix in [
        crate::inline_data::inline_table_prefix(catalog, table_id, schema),
        crate::keys::inline_current_row_prefix(catalog, table_id, schema),
    ] {
        for item in kv
            .scan_prefix(&prefix, crate::RangeDirection::Forward, usize::MAX)
            .unwrap()
        {
            cleanup.delete(item.key);
        }
    }
    kv.commit(cleanup).unwrap();
    resume_tx.send(()).unwrap();
    let payload = String::from_utf8(worker.join().unwrap().unwrap()).unwrap();
    HISTORY_CAPTURE_PAUSES.lock().unwrap().remove(&namespace);
    assert_eq!(payload.matches("row_change\t").count(), 1, "{payload}");
    assert!(payload.contains("\ti:1\n"), "{payload}");
    assert!(!payload.contains("\ti:2\n"), "{payload}");
    let database = crate::fdb_runtime::shared_foundationdb_database(None).unwrap();
    crate::clear_foundationdb_prefix(&database, kv.key_prefix()).unwrap();
}
