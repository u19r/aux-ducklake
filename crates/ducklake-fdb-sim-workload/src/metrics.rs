use foundationdb_simulation::Metric;

pub(crate) fn metric(key: &'static str, value: u64) -> Metric<'static> {
    Metric {
        key,
        val: value as f64,
        avg: false,
        fmt: None,
    }
}
