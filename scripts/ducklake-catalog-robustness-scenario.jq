$fdb[0] as $f | $postgres[0] as $p
| {
    name: $name,
    profile: $profile,
    execution_order: (if $backends == "both" then $execution_order else $backends end),
    comparison: {
        status: (if (($backends == "both" and $f.elapsed_ms and $p.elapsed_ms)
                     or ($backends == "fdb" and $f.elapsed_ms)
                     or ($backends == "postgres" and $p.elapsed_ms)) then "complete" else "failed" end),
        fdb_elapsed_ms: $f.elapsed_ms,
        postgres_elapsed_ms: $p.elapsed_ms,
        fdb_postgres_ratio: (if ($f.elapsed_ms and $p.elapsed_ms) then ($f.elapsed_ms / $p.elapsed_ms) else null end),
        fdb_status: (if $f == null then "skipped" else ($f.status // "complete") end),
        postgres_status: (if $p == null then "skipped" else ($p.status // "complete") end)
    },
    fdb: $f,
    postgres: $p
}
