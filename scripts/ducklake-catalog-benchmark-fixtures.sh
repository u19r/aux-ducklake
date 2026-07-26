#!/usr/bin/env bash

realistic_table_name() {
    printf 'bench_%03d' "$1"
}

default_varied_tables_per_transaction() {
    local table_count="$1"
    if ((table_count < 10)); then
        printf '%s\n' "$table_count"
    else
        printf '10\n'
    fi
}

realistic_row_sql() {
    local table_index="$1" start_id="$2" end_id="$3"
    local table_name
    table_name="$(realistic_table_name "$table_index")"
    local payload_bytes="${realistic_row_bytes:-4096}"
    local payload_repeats=$(((payload_bytes + 31) / 32))
    cat <<SQL
INSERT INTO dl.main.$table_name
SELECT
    i::INTEGER,
    $table_index::INTEGER,
    CASE WHEN i % 5 = 0 THEN 'a' WHEN i % 5 = 1 THEN 'b' WHEN i % 5 = 2 THEN 'c' WHEN i % 5 = 3 THEN 'd' ELSE 'e' END,
    (i * 10)::BIGINT,
$(realistic_row_tail_sql "$payload_bytes" "$payload_repeats")
FROM range($start_id, $end_id) t(i);
SQL
}

realistic_row_tail_sql() {
    local payload_bytes="$1" payload_repeats="$2" column
    case "${varied_schema:-legacy}" in
        narrow)
            cat <<SQL
    i % 2 = 0,
    DATE '2020-01-01' + (i % 365)::INTEGER,
    i::DOUBLE / 7.0,
    left(repeat(md5((($table_index::BIGINT * 1000000000::BIGINT) + i::BIGINT)::VARCHAR), $payload_repeats), $payload_bytes)
SQL
            ;;
        mixed)
            cat <<SQL
    i % 2 = 0,
    DATE '2020-01-01' + (i % 365)::INTEGER,
    TIMESTAMP '2020-01-01 00:00:00' + i * INTERVAL 1 SECOND,
    (i::DECIMAL(18, 3) / 7)::DECIMAL(18, 3),
    i::DOUBLE / 11.0,
    md5((($table_index::BIGINT * 1000000000::BIGINT) + i::BIGINT)::VARCHAR)::UUID,
    'note_' || (i % 97)::VARCHAR,
    left(repeat(md5((($table_index::BIGINT * 1000000000::BIGINT) + i::BIGINT)::VARCHAR), $payload_repeats), $payload_bytes)
SQL
            ;;
        wide)
            for ((column = 4; column < 47; column++)); do
                printf '    i + %s,\n' "$column"
            done
            printf '    left(repeat(md5(((%s::BIGINT * 1000000000::BIGINT) + i::BIGINT)::VARCHAR), %s), %s)\n' "$table_index" "$payload_repeats" "$payload_bytes"
            ;;
        legacy)
            for ((column = 1; column <= 19; column++)); do
                printf '    i + %s,\n' "$column"
            done
            printf '    left(repeat(md5(((%s::BIGINT * 1000000000::BIGINT) + i::BIGINT)::VARCHAR), %s), %s)\n' "$table_index" "$payload_repeats" "$payload_bytes"
            ;;
    esac
}

realistic_schema_sql() {
    local count="$1" table table_name
    for ((table = 0; table < count; table++)); do
        table_name="$(realistic_table_name "$table")"
        cat <<SQL
CREATE TABLE dl.main.$table_name(
    id INTEGER,
    table_index INTEGER,
    bucket VARCHAR,
    amount BIGINT,
$(realistic_schema_tail_sql)
);
SQL
        if [[ "$profile" == "varied" ]]; then
            cat <<SQL
ALTER TABLE dl.main.$table_name SET PARTITIONED BY (bucket);
ALTER TABLE dl.main.$table_name SET SORTED BY (id ASC NULLS FIRST);
SQL
        fi
    done
}

realistic_schema_tail_sql() {
    local column
    case "${varied_schema:-legacy}" in
        narrow)
            cat <<'SQL'
    is_active BOOLEAN,
    event_date DATE,
    score DOUBLE,
    payload VARCHAR
SQL
            ;;
        mixed)
            cat <<'SQL'
    is_active BOOLEAN,
    event_date DATE,
    event_at TIMESTAMP,
    price DECIMAL(18, 3),
    score DOUBLE,
    external_id UUID,
    note VARCHAR,
    payload VARCHAR
SQL
            ;;
        wide)
            for ((column = 4; column < 47; column++)); do
                printf '    c%02d BIGINT,\n' "$column"
            done
            printf '    payload VARCHAR\n'
            ;;
        legacy)
            for ((column = 3; column <= 21; column++)); do
                printf '    c%02d BIGINT,\n' "$column"
            done
            printf '    payload VARCHAR\n'
            ;;
    esac
}

realistic_schema_column_count() {
    case "${varied_schema:-legacy}" in
        narrow) printf '8\n' ;;
        mixed) printf '12\n' ;;
        wide) printf '48\n' ;;
        legacy) printf '24\n' ;;
    esac
}

realistic_preload_sql() {
    local count="$1" rows="$2" table start end chunk
    realistic_schema_sql "$count"
    for ((table = 0; table < count; table++)); do
        start=1
        while [[ "$start" -le "$rows" ]]; do
            chunk=$((5 + ((table + start) % 16)))
            end=$((start + chunk))
            if [[ "$end" -gt $((rows + 1)) ]]; then
                end=$((rows + 1))
            fi
            realistic_row_sql "$table" "$start" "$end"
            start="$end"
        done
    done
    cat <<SQL
SELECT 'realistic_preload=' || $count || ',' || $rows || ',' || ($count * $rows) || ',' || ($count * $rows * $realistic_row_bytes);
SQL
}

realistic_preload_worker_sql() {
    local count="$1" rows="$2" worker="$3" workers="$4" batch_rows="$5"
    local table start end
    for ((table = worker; table < count; table += workers)); do
        start=1
        while [[ "$start" -le "$rows" ]]; do
            end=$((start + batch_rows))
            if [[ "$end" -gt $((rows + 1)) ]]; then
                end=$((rows + 1))
            fi
            realistic_row_sql "$table" "$start" "$end"
            start="$end"
        done
    done
    cat <<SQL
SELECT 'realistic_preload_worker=' || $worker || ',' || $workers || ',' || $batch_rows;
SQL
}

realistic_sum_subqueries() {
    local count="$1" table table_name sep=""
    for ((table = 0; table < count; table++)); do
        table_name="$(realistic_table_name "$table")"
        printf '%sSELECT count(*) row_count, coalesce(sum(id), 0) id_sum FROM dl.main.%s' "$sep" "$table_name"
        sep=$'\nUNION ALL\n'
    done
}

realistic_latest_query_sql() {
    local count="$1"
    cat <<SQL
SELECT 'realistic_latest=' || sum(row_count) || ',' || sum(id_sum)
FROM (
$(realistic_sum_subqueries "$count")
);
SQL
}

realistic_time_travel_query_sql() {
    local count="$1" snapshot_var="$2" table table_name sep=""
    printf "SELECT 'realistic_time_travel=' || sum(row_count) || ',' || sum(id_sum)\nFROM (\n"
    for ((table = 0; table < count; table++)); do
        table_name="$(realistic_table_name "$table")"
        printf "%sSELECT count(*) row_count, coalesce(sum(id), 0) id_sum FROM dl.main.%s AT (VERSION => getvariable('%s')::BIGINT)" "$sep" "$table_name" "$snapshot_var"
        sep=$'\nUNION ALL\n'
    done
    printf "\n);\n"
}

varied_join_query_sql() {
    local count="$1" snapshot_var="${2:-}" group_count table_a table_b table_c table_d sep=""
    group_count=$((count / 4))
    if [[ "$group_count" -lt 1 ]]; then
        group_count=1
    fi
    if [[ -n "$snapshot_var" ]]; then
        printf "SELECT 'varied_join_time_travel=' || sum(join_count) || ',' || coalesce(sum(join_amount), 0)\nFROM (\n"
    else
        printf "SELECT 'varied_join_latest=' || sum(join_count) || ',' || coalesce(sum(join_amount), 0)\nFROM (\n"
    fi
    for ((group = 0; group < group_count; group++)); do
        table_a="$(realistic_table_name "$((group * 4))")"
        table_b="$(realistic_table_name "$(((group * 4 + 1) % count))")"
        table_c="$(realistic_table_name "$(((group * 4 + 2) % count))")"
        table_d="$(realistic_table_name "$(((group * 4 + 3) % count))")"
        if [[ -n "$snapshot_var" ]]; then
            printf "%sSELECT count(*) join_count, coalesce(sum(a.amount + b.amount + c.amount + d.amount), 0) join_amount\nFROM (SELECT * FROM dl.main.%s AT (VERSION => getvariable('%s')::BIGINT)) a\nJOIN (SELECT * FROM dl.main.%s AT (VERSION => getvariable('%s')::BIGINT)) b ON b.id = a.id AND b.bucket = a.bucket\nJOIN (SELECT * FROM dl.main.%s AT (VERSION => getvariable('%s')::BIGINT)) c ON c.id = a.id\nJOIN (SELECT * FROM dl.main.%s AT (VERSION => getvariable('%s')::BIGINT)) d ON d.table_index <> a.table_index AND d.id = b.id\nWHERE a.id %% 97 = %s AND b.id %% 89 = %s" "$sep" "$table_a" "$snapshot_var" "$table_b" "$snapshot_var" "$table_c" "$snapshot_var" "$table_d" "$snapshot_var" "$((group % 97))" "$((group % 89))"
        else
            printf "%sSELECT count(*) join_count, coalesce(sum(a.amount + b.amount + c.amount + d.amount), 0) join_amount\nFROM dl.main.%s a\nJOIN dl.main.%s b ON b.id = a.id AND b.bucket = a.bucket\nJOIN dl.main.%s c ON c.id = a.id\nJOIN dl.main.%s d ON d.table_index <> a.table_index AND d.id = b.id\nWHERE a.id %% 97 = %s AND b.id %% 89 = %s" "$sep" "$table_a" "$table_b" "$table_c" "$table_d" "$((group % 97))" "$((group % 89))"
        fi
        sep=$'\nUNION ALL\n'
    done
    printf "\n);\n"
}

realistic_mixed_sql() {
    local count="$1" rows="$2" table table_name start_id
    cat <<SQL
SET VARIABLE realistic_before_mixed = (SELECT id FROM ducklake_current_snapshot('dl'));
SQL
    for ((table = 0; table < count; table++)); do
        table_name="$(realistic_table_name "$table")"
        start_id=$((rows + 1))
        realistic_row_sql "$table" "$start_id" "$((start_id + 5))"
        cat <<SQL
DELETE FROM dl.main.$table_name WHERE id IN (1, 2);
SELECT count(*), coalesce(sum(id), 0) FROM dl.main.$table_name WHERE bucket IN ('a', 'c');
SQL
    done
    realistic_latest_query_sql "$count"
    realistic_time_travel_query_sql "$count" "realistic_before_mixed"
}

realistic_delete_sql() {
    local count="$1" table table_name
    cat <<SQL
SET VARIABLE realistic_before_deletes = (SELECT id FROM ducklake_current_snapshot('dl'));
SQL
    for ((table = 0; table < count; table++)); do
        table_name="$(realistic_table_name "$table")"
        cat <<SQL
DELETE FROM dl.main.$table_name WHERE id = 3;
DELETE FROM dl.main.$table_name WHERE id = 4;
SQL
    done
    realistic_latest_query_sql "$count"
    realistic_time_travel_query_sql "$count" "realistic_before_deletes"
}

varied_churn_sql() {
    local count="$1" rows="$2" rounds="$3" mode="${4:-all}" tables_per_transaction="${5:-10}"
    local round table table_name start_id delete_id update_id span
    local has_mutations=0
    if [[ "$mode" == "all" || "$mode" == "mutate" || "$mode" == "insert" || "$mode" == "update" || "$mode" == "delete" ]]; then
        has_mutations=1
    fi
    if [[ "$mode" == "all" || "$mode" == "time_travel" || "$mode" == "join_time_travel" ]]; then
        cat <<SQL
SET VARIABLE varied_before_churn = (SELECT id FROM ducklake_current_snapshot('dl'));
SQL
    fi
    for ((round = 0; round < rounds; round++)); do
        for ((table = 0; table < count; table++)); do
            if ((has_mutations == 1 && table % tables_per_transaction == 0)); then
                printf 'BEGIN TRANSACTION;\n'
            fi
            table_name="$(realistic_table_name "$table")"
            span=$((5 + ((round + table) % 16)))
            start_id=$((rows + 1000 + round * 100000 + table * 100))
            delete_id=$((5 + ((round + table) % 31)))
            update_id=$((40 + ((round * 7 + table) % 53)))
            if [[ "$mode" == "all" || "$mode" == "mutate" || "$mode" == "insert" ]]; then
                realistic_row_sql "$table" "$start_id" "$((start_id + span))"
            fi
            if [[ "$mode" == "all" || "$mode" == "mutate" || "$mode" == "update" ]]; then
                cat <<SQL
UPDATE dl.main.$table_name SET amount = amount + $((round + 1)), bucket = CASE WHEN bucket = 'a' THEN 'b' ELSE bucket END WHERE id = $update_id;
SQL
            fi
            if [[ "$mode" == "all" || "$mode" == "mutate" || "$mode" == "delete" ]]; then
                cat <<SQL
DELETE FROM dl.main.$table_name WHERE id IN ($delete_id, $((delete_id + 1)));
SQL
            fi
            if ((has_mutations == 1 && ((table + 1) % tables_per_transaction == 0 || table + 1 == count))); then
                printf 'COMMIT;\n'
            fi
        done
        if [[ "$mode" == "all" ]]; then
            varied_join_query_sql "$count"
            cat <<SQL
SELECT 'varied_memory_usage_bytes_round_$round=' || coalesce(sum(memory_usage_bytes), 0)
FROM duckdb_memory();
SELECT 'varied_object_cache_bytes_round_$round=' || coalesce(sum(memory_usage_bytes), 0)
FROM duckdb_memory()
WHERE tag = 'OBJECT_CACHE';
SELECT 'varied_external_file_cache_blocks_round_$round=' || count(*)
FROM duckdb_external_file_cache();
SQL
        fi
    done
    if [[ "$mode" == "all" ]]; then
        realistic_latest_query_sql "$count"
        realistic_time_travel_query_sql "$count" "varied_before_churn"
        varied_join_query_sql "$count" "varied_before_churn"
    elif [[ "$mode" == "latest" ]]; then
        realistic_latest_query_sql "$count"
    elif [[ "$mode" == "time_travel" ]]; then
        realistic_time_travel_query_sql "$count" "varied_before_churn"
    elif [[ "$mode" == "join" ]]; then
        varied_join_query_sql "$count"
    elif [[ "$mode" == "join_time_travel" ]]; then
        varied_join_query_sql "$count" "varied_before_churn"
    else
        printf "SELECT 'varied_churn_component=%s';\n" "$mode"
    fi
}

realistic_inline_sql() {
    local count="$1" table table_name
    for ((table = 0; table < count; table++)); do
        table_name="inline_$(realistic_table_name "$table")"
        cat <<SQL
CREATE TABLE dl.main.$table_name(id INTEGER, table_index INTEGER, note VARCHAR);
INSERT INTO dl.main.$table_name SELECT i::INTEGER, $table::INTEGER, 'inline_' || i::VARCHAR FROM range(1, 6) t(i);
INSERT INTO dl.main.$table_name SELECT i::INTEGER, $table::INTEGER, 'inline_' || i::VARCHAR FROM range(6, 18) t(i);
DELETE FROM dl.main.$table_name WHERE id IN (2, 3);
CALL ducklake_flush_inlined_data('dl', table_name => '$table_name');
SQL
    done
    cat <<SQL
SELECT 'realistic_inline_tables=' || $count;
SQL
}

inline_micro_table_sql() {
    local table="$1" first_rows="$2" second_rows="$3" delete_rows="$4"
    local table_name
    table_name="inline_$(realistic_table_name "$table")"
    cat <<SQL
CREATE TABLE dl.main.$table_name(id INTEGER, table_index INTEGER, note VARCHAR);
INSERT INTO dl.main.$table_name
SELECT i::INTEGER, $table::INTEGER, 'inline_' || i::VARCHAR
FROM range(1, $((first_rows + 1))) t(i);
SQL
    if [[ "$inline_flush_interval" == "each_batch" ]]; then
        printf "CALL ducklake_flush_inlined_data('dl', table_name => '%s');\n" "$table_name"
    fi
    cat <<SQL
INSERT INTO dl.main.$table_name
SELECT i::INTEGER, $table::INTEGER, 'inline_' || i::VARCHAR
FROM range($((first_rows + 1)), $((first_rows + second_rows + 1))) t(i);
SQL
    if [[ "$inline_flush_interval" == "each_batch" ]]; then
        printf "CALL ducklake_flush_inlined_data('dl', table_name => '%s');\n" "$table_name"
    fi
    cat <<SQL
DELETE FROM dl.main.$table_name WHERE id <= $delete_rows;
SQL
    if [[ "$inline_flush_interval" == "end" ]]; then
        printf "CALL ducklake_flush_inlined_data('dl', table_name => '%s');\n" "$table_name"
    fi
    cat <<SQL
SELECT 'inline_micro_table=' || '$table_name' || ',' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.$table_name;
SQL
}

varied_concurrent_writer_sql() {
    local table_count="$1" rows="$2" writer="$3"
    local table=$((writer % table_count))
    local table_name start_id
    table_name="$(realistic_table_name "$table")"
    start_id=$((rows + 1000000 + writer * 100))
    realistic_row_sql "$table" "$start_id" "$((start_id + 8))"
    cat <<SQL
UPDATE dl.main.$table_name SET amount = amount + 1 WHERE id = $((50 + writer));
DELETE FROM dl.main.$table_name WHERE id = $((80 + writer));
SELECT 'concurrent_writer=' || $writer || ',' || count(*) FROM dl.main.$table_name;
SQL
}

inline_micro_step_sql() {
    local step="$1" table="$2" first_rows="$3" second_rows="$4" delete_rows="$5"
    local table_name
    table_name="inline_$(realistic_table_name "$table")"
    case "$step" in
        create)
            cat <<SQL
CREATE TABLE dl.main.$table_name(id INTEGER, table_index INTEGER, note VARCHAR);
SELECT 'inline_micro_step=' || '$table_name' || ',create';
SQL
            ;;
        insert_first)
            cat <<SQL
INSERT INTO dl.main.$table_name
SELECT i::INTEGER, $table::INTEGER, 'inline_' || i::VARCHAR
FROM range(1, $((first_rows + 1))) t(i);
SELECT 'inline_micro_step=' || '$table_name' || ',insert_first';
SQL
            ;;
        insert_second)
            cat <<SQL
INSERT INTO dl.main.$table_name
SELECT i::INTEGER, $table::INTEGER, 'inline_' || i::VARCHAR
FROM range($((first_rows + 1)), $((first_rows + second_rows + 1))) t(i);
SELECT 'inline_micro_step=' || '$table_name' || ',insert_second';
SQL
            ;;
        delete)
            cat <<SQL
DELETE FROM dl.main.$table_name WHERE id <= $delete_rows;
SELECT 'inline_micro_step=' || '$table_name' || ',delete';
SQL
            ;;
        flush_read)
            cat <<SQL
CALL ducklake_flush_inlined_data('dl', table_name => '$table_name');
SELECT 'inline_micro_table=' || '$table_name' || ',' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.$table_name;
SQL
            ;;
        *) fail "unknown inline micro step $step" ;;
    esac
}

realistic_compaction_sql() {
    local count="$1"
    local batch_size="${2:-$(default_varied_tables_per_transaction "$count")}"
    local table table_name
    for ((table = 0; table < count; table++)); do
        if ((table % batch_size == 0)); then
            printf 'BEGIN TRANSACTION;\n'
        fi
        table_name="$(realistic_table_name "$table")"
        printf "CALL ducklake_merge_adjacent_files('dl', '%s');\n" "$table_name"
        if (((table + 1) % batch_size == 0 || table + 1 == count)); then
            printf 'COMMIT;\n'
        fi
    done
    realistic_latest_query_sql "$count"
}
