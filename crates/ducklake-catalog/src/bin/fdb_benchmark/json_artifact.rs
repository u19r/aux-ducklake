pub(crate) struct BatchResult {
    pub(crate) labels: Vec<(String, String)>,
    pub(crate) operation_counts: Vec<(String, u64)>,
    pub(crate) transaction_estimates: Vec<(String, String)>,
}

impl BatchResult {
    pub(crate) fn new() -> Self {
        Self {
            labels: Vec::new(),
            operation_counts: Vec::new(),
            transaction_estimates: Vec::new(),
        }
    }

    pub(crate) fn label(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.labels.push((key.into(), value.to_string()));
        self
    }

    pub(crate) fn operation(mut self, key: impl Into<String>, value: impl TryInto<u64>) -> Self {
        let value = value.try_into().unwrap_or(u64::MAX);
        self.operation_counts.push((key.into(), value));
        self
    }

    pub(crate) fn estimate(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.transaction_estimates
            .push((key.into(), value.to_string()));
        self
    }
}

pub(crate) struct Batch {
    pub(crate) name: String,
    pub(crate) duration_ms: f64,
    pub(crate) labels: Vec<(String, String)>,
    pub(crate) operation_counts: Vec<(String, u64)>,
    pub(crate) transaction_estimates: Vec<(String, String)>,
}

pub(crate) struct Artifact {
    pub(crate) profile: String,
    pub(crate) generated_at_micros: u128,
    pub(crate) elapsed_ms: f64,
    pub(crate) key_prefix: Vec<u8>,
    pub(crate) fixture: Vec<(String, String)>,
    pub(crate) batches: Vec<Batch>,
}

impl Artifact {
    pub(crate) fn to_json(&self) -> String {
        let mut out = String::new();
        out.push_str("{\n");
        field(
            &mut out,
            1,
            "artifact",
            "ducklake-fdb-feature-parity-fdb-benchmark",
            true,
        );
        field(&mut out, 1, "profile", &self.profile, true);
        number_field(
            &mut out,
            1,
            "generated_at_micros",
            self.generated_at_micros,
            true,
        );
        float_field(&mut out, 1, "elapsed_ms", self.elapsed_ms, true);
        field(
            &mut out,
            1,
            "key_prefix",
            &String::from_utf8_lossy(&self.key_prefix),
            true,
        );
        map_field(&mut out, 1, "fixture", &self.fixture, true);
        out.push_str("  \"batches\": [\n");
        for (index, batch) in self.batches.iter().enumerate() {
            batch_json(&mut out, batch, index + 1 != self.batches.len());
        }
        out.push_str("  ]\n}\n");
        out
    }
}

fn batch_json(out: &mut String, batch: &Batch, trailing_comma: bool) {
    out.push_str("    {\n");
    field(out, 3, "name", &batch.name, true);
    float_field(out, 3, "duration_ms", batch.duration_ms, true);
    map_field(out, 3, "labels", &batch.labels, true);
    u64_map_field(out, 3, "operation_counts", &batch.operation_counts, true);
    map_field(
        out,
        3,
        "transaction_estimates",
        &batch.transaction_estimates,
        false,
    );
    out.push_str("    }");
    if trailing_comma {
        out.push(',');
    }
    out.push('\n');
}

fn field(out: &mut String, indent: usize, key: &str, value: &str, comma: bool) {
    line(
        out,
        indent,
        &format!("\"{}\": \"{}\"", escape(key), escape(value)),
        comma,
    );
}

fn number_field(out: &mut String, indent: usize, key: &str, value: u128, comma: bool) {
    line(
        out,
        indent,
        &format!("\"{}\": {}", escape(key), value),
        comma,
    );
}

fn float_field(out: &mut String, indent: usize, key: &str, value: f64, comma: bool) {
    line(
        out,
        indent,
        &format!("\"{}\": {:.3}", escape(key), value),
        comma,
    );
}

fn map_field(out: &mut String, indent: usize, key: &str, values: &[(String, String)], comma: bool) {
    line(out, indent, &format!("\"{}\": {{", escape(key)), false);
    for (index, (map_key, value)) in values.iter().enumerate() {
        field(out, indent + 1, map_key, value, index + 1 != values.len());
    }
    line(out, indent, "}", comma);
}

fn u64_map_field(
    out: &mut String,
    indent: usize,
    key: &str,
    values: &[(String, u64)],
    comma: bool,
) {
    line(out, indent, &format!("\"{}\": {{", escape(key)), false);
    for (index, (map_key, value)) in values.iter().enumerate() {
        line(
            out,
            indent + 1,
            &format!("\"{}\": {}", escape(map_key), value),
            index + 1 != values.len(),
        );
    }
    line(out, indent, "}", comma);
}

fn line(out: &mut String, indent: usize, text: &str, comma: bool) {
    out.push_str(&"  ".repeat(indent));
    out.push_str(text);
    if comma {
        out.push(',');
    }
    out.push('\n');
}

fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            other => escaped.push(other),
        }
    }
    escaped
}
