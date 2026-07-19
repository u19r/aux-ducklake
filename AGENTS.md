# aux-ducklake

Follow the mandatory cross-repository contract in `../aux-infra/docs/code-style.md`.

## Boundaries

- `ducklake-catalog` owns DuckLake catalog metadata, the FoundationDB backend, and the C-compatible
  runtime bridge. The simulation crates own deterministic FoundationDB model, runner, and workload
  behavior.
- Shared, staging, production, benchmark, and test catalogs must set an explicit
  `AUX_DUCKLAKE_FDB_PREFIX`; do not share the default prefix.
- Change the pinned upstream integration through `patches/ducklake/` and the fetch/build scripts.
  Do not hand-edit the fetched upstream checkout.

## Specialized commands

- Focused catalog: `just ducklake-catalog-check`, `just ducklake-catalog-test`
- Release proof: `just ducklake-fdb-release-gate`

## Exact exceptions

- `third_party/ducklake/` is a generated vendored checkout and follows its upstream instructions;
  it is not a local style precedent.
- The `[lib]` name `ducklake_fdb_sim_workload` in
  `crates/ducklake-fdb-sim-workload/Cargo.toml` is retained for the simulation `cdylib` ABI.
