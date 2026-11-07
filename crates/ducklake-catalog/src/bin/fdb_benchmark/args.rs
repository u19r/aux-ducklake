use std::{env, path::PathBuf};

#[derive(Clone)]
pub(crate) struct Args {
    pub(crate) profile: String,
    pub(crate) scan_files: usize,
    pub(crate) batch_size: usize,
    pub(crate) concurrent_writers: usize,
    pub(crate) writer_files: usize,
    pub(crate) high_tables: usize,
    pub(crate) high_rows: usize,
    pub(crate) output: PathBuf,
}

impl Args {
    pub(crate) fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut args = Self::for_profile("smoke")?;
        let mut iter = env::args().skip(1);
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--profile" => {
                    let profile = iter.next().ok_or("--profile requires a value")?;
                    args = Self::for_profile(&profile)?;
                }
                "--scan-files" => args.scan_files = parse_usize(&arg, iter.next())?,
                "--batch-size" => args.batch_size = parse_usize(&arg, iter.next())?,
                "--concurrent-writers" => args.concurrent_writers = parse_usize(&arg, iter.next())?,
                "--writer-files" => args.writer_files = parse_usize(&arg, iter.next())?,
                "--high-tables" => args.high_tables = parse_usize(&arg, iter.next())?,
                "--high-rows" => args.high_rows = parse_usize(&arg, iter.next())?,
                "--output" => {
                    args.output = PathBuf::from(iter.next().ok_or("--output requires a value")?)
                }
                other => return Err(format!("unknown argument {other}").into()),
            }
        }
        Ok(args)
    }

    pub(crate) fn fixture(&self) -> Vec<(String, String)> {
        vec![
            ("backend".to_owned(), "foundationdb".to_owned()),
            ("scan_files".to_owned(), self.scan_files.to_string()),
            ("batch_size".to_owned(), self.batch_size.to_string()),
            (
                "concurrent_writers".to_owned(),
                self.concurrent_writers.to_string(),
            ),
            ("writer_files".to_owned(), self.writer_files.to_string()),
            ("high_tables".to_owned(), self.high_tables.to_string()),
            ("high_rows".to_owned(), self.high_rows.to_string()),
        ]
    }

    fn for_profile(profile: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let base = PathBuf::from("docs/benchmarks/ducklake-fdb-feature-parity");
        match profile {
            "smoke" => Ok(Self {
                profile: profile.to_owned(),
                scan_files: 100,
                batch_size: 25,
                concurrent_writers: 2,
                writer_files: 2,
                high_tables: 10,
                high_rows: 10,
                output: base.join("fdb-smoke-latest.json"),
            }),
            "tiny" => Ok(Self {
                profile: profile.to_owned(),
                scan_files: 12,
                batch_size: 4,
                concurrent_writers: 2,
                writer_files: 2,
                high_tables: 5,
                high_rows: 5,
                output: base.join("fdb-profile-tiny-latest.json"),
            }),
            "full" => Ok(Self {
                profile: profile.to_owned(),
                scan_files: 10_000,
                batch_size: 250,
                concurrent_writers: 4,
                writer_files: 25,
                high_tables: 100,
                high_rows: 100,
                output: base.join("fdb-profile-latest.json"),
            }),
            other => Err(format!("unknown profile {other}").into()),
        }
    }
}

pub(crate) fn require_live_fdb() -> Result<(), Box<dyn std::error::Error>> {
    if env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() == Ok("1") {
        return Ok(());
    }
    Err("set AUX_DUCKLAKE_FDB_LIVE=1 to run FoundationDB benchmarks".into())
}

fn parse_usize(flag: &str, value: Option<String>) -> Result<usize, Box<dyn std::error::Error>> {
    let value = value.ok_or_else(|| format!("{flag} requires a value"))?;
    let parsed = value.parse::<usize>()?;
    if parsed == 0 {
        return Err(format!("{flag} must be positive").into());
    }
    Ok(parsed)
}
