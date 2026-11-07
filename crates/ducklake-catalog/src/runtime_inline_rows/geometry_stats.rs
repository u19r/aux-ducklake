use std::{collections::BTreeSet, fmt::Write};

#[derive(Clone, Debug)]
pub(super) struct InlineGeometryStats {
    xmin: Option<f64>,
    xmax: Option<f64>,
    ymin: Option<f64>,
    ymax: Option<f64>,
    zmin: Option<f64>,
    zmax: Option<f64>,
    mmin: Option<f64>,
    mmax: Option<f64>,
    types: BTreeSet<String>,
}

impl InlineGeometryStats {
    pub(super) fn parse_wkt(value: &str) -> Option<Self> {
        let trimmed = value.trim();
        let first_paren = trimmed.find('(')?;
        let header = trimmed[..first_paren].trim();
        let mut parts = header.split_whitespace();
        let geometry_type = parts.next()?.to_ascii_lowercase();
        let dimension = parts.next().unwrap_or("").to_ascii_lowercase();
        let coordinate_width = match dimension.as_str() {
            "z" | "m" => 3,
            "zm" => 4,
            _ => 2,
        };
        let mut stats = Self {
            xmin: None,
            xmax: None,
            ymin: None,
            ymax: None,
            zmin: None,
            zmax: None,
            mmin: None,
            mmax: None,
            types: [geometry_type_with_dimension(&geometry_type, &dimension)]
                .into_iter()
                .collect(),
        };
        let numbers = wkt_numbers(&trimmed[first_paren..]);
        if numbers.len() < coordinate_width {
            return None;
        }
        for coordinate in numbers.chunks(coordinate_width) {
            if coordinate.len() < coordinate_width {
                break;
            }
            stats.observe_xy(coordinate[0], coordinate[1]);
            match dimension.as_str() {
                "z" => stats.observe_z(coordinate[2]),
                "m" => stats.observe_m(coordinate[2]),
                "zm" => {
                    stats.observe_z(coordinate[2]);
                    stats.observe_m(coordinate[3]);
                }
                _ => {}
            }
        }
        Some(stats)
    }

    pub(super) fn merge(&mut self, incoming: Self) {
        self.xmin = min_optional(self.xmin, incoming.xmin);
        self.xmax = max_optional(self.xmax, incoming.xmax);
        self.ymin = min_optional(self.ymin, incoming.ymin);
        self.ymax = max_optional(self.ymax, incoming.ymax);
        self.zmin = min_optional(self.zmin, incoming.zmin);
        self.zmax = max_optional(self.zmax, incoming.zmax);
        self.mmin = min_optional(self.mmin, incoming.mmin);
        self.mmax = max_optional(self.mmax, incoming.mmax);
        self.types.extend(incoming.types);
    }

    fn observe_xy(&mut self, x: f64, y: f64) {
        self.xmin = min_optional(self.xmin, Some(x));
        self.xmax = max_optional(self.xmax, Some(x));
        self.ymin = min_optional(self.ymin, Some(y));
        self.ymax = max_optional(self.ymax, Some(y));
    }

    fn observe_z(&mut self, z: f64) {
        self.zmin = min_optional(self.zmin, Some(z));
        self.zmax = max_optional(self.zmax, Some(z));
    }

    fn observe_m(&mut self, m: f64) {
        self.mmin = min_optional(self.mmin, Some(m));
        self.mmax = max_optional(self.mmax, Some(m));
    }

    fn to_json(&self) -> String {
        let mut out = String::from("{\"bbox\": {");
        write!(
            out,
            "\"xmin\": {}, \"xmax\": {}, \"ymin\": {}, \"ymax\": {}, \"zmin\": {}, \"zmax\": {}, \"mmin\": {}, \"mmax\": {}",
            json_number_or_null(self.xmin),
            json_number_or_null(self.xmax),
            json_number_or_null(self.ymin),
            json_number_or_null(self.ymax),
            json_number_or_null(self.zmin),
            json_number_or_null(self.zmax),
            json_number_or_null(self.mmin),
            json_number_or_null(self.mmax)
        )
        .expect("writing geometry stats JSON to string cannot fail");
        out.push_str("}, \"types\": [");
        for (index, geometry_type) in self.types.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push('"');
            out.push_str(geometry_type);
            out.push('"');
        }
        out.push_str("]}");
        out
    }
}

impl std::fmt::Display for InlineGeometryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_json())
    }
}

pub(super) fn geometry_type_with_dimension(geometry_type: &str, dimension: &str) -> String {
    match dimension {
        "z" | "m" | "zm" => format!("{geometry_type}_{dimension}"),
        _ => geometry_type.to_owned(),
    }
}

pub(super) fn wkt_numbers(value: &str) -> Vec<f64> {
    let mut numbers = Vec::new();
    let mut start = None;
    let mut previous = '\0';
    for (index, ch) in value.char_indices() {
        let exponent_sign = matches!(previous, 'e' | 'E') && matches!(ch, '-' | '+');
        if ch.is_ascii_digit() || matches!(ch, '-' | '+' | '.') || exponent_sign {
            start.get_or_insert(index);
            previous = ch;
            continue;
        }
        if matches!(ch, 'e' | 'E') && start.is_some() {
            previous = ch;
            continue;
        }
        if let Some(number_start) = start.take()
            && let Ok(number) = value[number_start..index].parse::<f64>()
        {
            numbers.push(number);
        }
        previous = ch;
    }
    if let Some(number_start) = start
        && let Ok(number) = value[number_start..].parse::<f64>()
    {
        numbers.push(number);
    }
    numbers
}

pub(super) fn min_optional(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

pub(super) fn max_optional(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

pub(super) fn json_number_or_null(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_owned())
}
