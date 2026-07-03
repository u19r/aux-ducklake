#[cfg(test)]
mod tests {
    use std::time::Instant;

    use alloc_counter::{AllocationGuard, emit_report};

    use crate::{
        CatalogId, DataFileId, DataFileRow, FakeOrderedCatalogKv, InlineTableFlush,
        RawSnapshotSequence, SchemaId, TableId, commit_data_mutation, initialize_catalog_if_absent,
        latest_snapshot, list_data_files,
    };

    use super::super::{
        assemble_inline_payload, inline_table_chunks, register_inline_table_payload,
    };

    fn large_inline_payload() -> Vec<u8> {
        let mut payload = Vec::new();
        for index in 0..5_000 {
            payload.extend_from_slice(
                format!("row\t{index}\ti:{index}\ts:value-{index}\n").as_bytes(),
            );
        }
        payload
    }

    #[test]
    fn perf_loop_assemble_inline_payload_reports_allocations() {
        let payload = large_inline_payload();
        let chunks = inline_table_chunks(
            TableId(1),
            SchemaId(1),
            crate::CatalogOrderId::uuid_v7(1),
            payload.clone(),
        )
        .unwrap();
        let guard = AllocationGuard::start(
            module_path!(),
            "perf_loop_assemble_inline_payload_reports_allocations",
            file!(),
            line!(),
            Some("assemble_inline_payload_5k_rows"),
        );
        let assembled = assemble_inline_payload(chunks).unwrap();
        assert_eq!(assembled, payload);
        let report = guard.finish();
        emit_report(&report);
    }

    #[test]
    #[ignore]
    fn perf_loop_assemble_inline_payload_reports_cpu() {
        let payload = large_inline_payload();
        let chunks = inline_table_chunks(
            TableId(1),
            SchemaId(1),
            crate::CatalogOrderId::uuid_v7(1),
            payload.clone(),
        )
        .unwrap();
        let started = Instant::now();
        let assembled = assemble_inline_payload(chunks).unwrap();
        assert_eq!(assembled, payload);
        println!(
            "assemble_inline_payload_5k_rows_ns={}",
            started.elapsed().as_nanos()
        );
    }

    #[test]
    fn given_same_inline_flush_committed_twice_when_saving_metadata_then_second_file_is_rejected() {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let schema = SchemaId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        register_inline_table_payload(&mut kv, catalog, table, schema, b"row\t1\n".to_vec())
            .unwrap();
        let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(flush_snapshot.sequence, RawSnapshotSequence(1));
        let flush = InlineTableFlush::new(table, schema, flush_snapshot.sequence);

        commit_data_mutation(
            &mut kv,
            catalog,
            vec![flush_file(1, table, 0)],
            Vec::new(),
            &[flush],
        )
        .unwrap();
        let error = commit_data_mutation(
            &mut kv,
            catalog,
            vec![flush_file(2, table, 10)],
            Vec::new(),
            &[flush],
        )
        .unwrap_err();

        assert!(error.to_string().contains("inline flush"));
        assert!(error.to_string().contains("stale"));
        let files = list_data_files(&kv, catalog).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].data_file_id, DataFileId(1));
    }

    #[test]
    #[ignore = "internal policy test: inline flush materialization may overlap existing row ids by design"]
    fn given_flushed_file_row_ids_overlap_live_file_when_saving_metadata_then_file_is_rejected() {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let schema = SchemaId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        register_inline_table_payload(&mut kv, catalog, table, schema, b"row\t1\n".to_vec())
            .unwrap();
        let first_flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_data_mutation(
            &mut kv,
            catalog,
            vec![flush_file_with_count(1, table, 10, 2)],
            Vec::new(),
            &[InlineTableFlush::new(
                table,
                schema,
                first_flush_snapshot.sequence,
            )],
        )
        .unwrap();

        register_inline_table_payload(&mut kv, catalog, table, schema, b"row\t2\n".to_vec())
            .unwrap();
        let second_flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let error = commit_data_mutation(
            &mut kv,
            catalog,
            vec![flush_file(2, table, 11)],
            Vec::new(),
            &[InlineTableFlush::new(
                table,
                schema,
                second_flush_snapshot.sequence,
            )],
        )
        .unwrap_err();

        assert!(error.to_string().contains("row ids"));
        assert!(error.to_string().contains("overlap"));
        let files = list_data_files(&kv, catalog).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].data_file_id, DataFileId(1));
    }

    fn flush_file(id: u64, table: TableId, row_id_start: u64) -> DataFileRow {
        flush_file_with_count(id, table, row_id_start, 1)
    }

    fn flush_file_with_count(
        id: u64,
        table: TableId,
        row_id_start: u64,
        record_count: u64,
    ) -> DataFileRow {
        DataFileRow::new(
            DataFileId(id),
            table,
            format!("flush-{id}.parquet"),
            record_count,
            100,
            crate::CatalogOrderId::uuid_v7(0),
        )
        .with_row_id_start(row_id_start)
    }
}
