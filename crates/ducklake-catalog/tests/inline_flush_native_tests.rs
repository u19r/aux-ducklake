#![cfg(feature = "foundationdb")]

use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
#[ignore = "requires freshly built native DuckLake artifacts and an owned FoundationDB cluster"]
fn given_updated_and_deleted_rows_when_flushed_and_compacted_then_aux_matches_duckdb() {
    assert_eq!(std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref(), Ok("1"));
    let cli =
        std::env::var("AUX_DUCKLAKE_TEST_NATIVE_CLI").expect("set the reviewed DuckDB CLI path");
    assert!(std::path::Path::new(&cli).is_file());
    let runtime =
        std::env::var("AUX_DUCKLAKE_RUNTIME_LIBRARY").expect("set the reviewed Rust runtime path");
    assert!(std::path::Path::new(&runtime).is_file());
    for backend in ["aux_catalog", "duckdb"] {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().display();
        let metadata_type = if backend == "duckdb" {
            ""
        } else {
            ", META_TYPE 'aux_catalog'"
        };
        let sql = format!(
            r#"
.bail on
SET autoinstall_known_extensions = false;
LOAD ducklake;
ATTACH 'ducklake:{root}/metadata.duckdb' AS dlake (
 DATA_PATH '{root}/data', ENCRYPTED, DATA_INLINING_ROW_LIMIT 100{metadata_type});
CREATE TABLE dlake.t (table_name VARCHAR, tenant_id VARCHAR, __id VARCHAR,
 shard BIGINT, email VARCHAR, seq BIGINT, item JSON);
ALTER TABLE dlake.t SET PARTITIONED BY (bucket(8, shard));
ALTER TABLE dlake.t SET SORTED BY (email ASC, seq DESC);
INSERT INTO dlake.t SELECT 't','perf','id'||i, i%8, 'u'||i||'@x', i,
 json_object('a',i) FROM range(0,300) r(i);
CALL ducklake_flush_inlined_data('dlake');
CREATE TEMP TABLE stage AS SELECT 't' AS table_name,'perf' AS tenant_id,
 'id'||i AS __id, i%8 AS shard, 'u'||i||'@x' AS email, i+10000 AS seq,
 json_object('a',i+10000) AS item FROM range(0,150) r(i);
MERGE INTO dlake.t USING stage ON dlake.t.__id = stage.__id
 WHEN MATCHED THEN UPDATE SET seq = stage.seq, item = stage.item;
DELETE FROM dlake.t WHERE seq % 3 = 0;
SELECT 'review_before=' || count(*) FROM dlake.t;
CALL ducklake_flush_inlined_data('dlake');
SELECT 'review_flush=' || count(*) FROM dlake.t;
CALL ducklake_rewrite_data_files('dlake', 't', delete_threshold => 0.1, max_compacted_files => 32);
SELECT 'review_rewrite=' || count(*) FROM dlake.t;
CALL ducklake_merge_adjacent_files('dlake', 't', max_compacted_files => 32);
SELECT 'review_merge=' || count(*) FROM dlake.t;
"#
        );
        let mut child = Command::new(&cli)
            .args(["-unsigned", "-batch", "-csv"])
            .env("AUX_DUCKLAKE_CATALOG_BACKEND", "foundationdb")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(sql.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{backend}: {stdout}\n{stderr}");
        for stage in ["before", "flush", "rewrite", "merge"] {
            assert!(
                stdout.contains(&format!("review_{stage}=200")),
                "{backend}: {stdout}\n{stderr}"
            );
        }
        println!("NATIVE_INLINE_SQL backend={backend} before=200 flush=200 rewrite=200 merge=200");
    }
}
