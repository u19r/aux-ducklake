#[cfg(not(feature = "foundationdb"))]
fn main() {
    eprintln!("ducklake-fdb-prefix-clear requires `--features foundationdb`");
    std::process::exit(2);
}

#[cfg(feature = "foundationdb")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    prefix_clear::run()
}

#[cfg(feature = "foundationdb")]
mod prefix_clear {
    use std::env;

    use ducklake_catalog::clear_foundationdb_prefix;
    use foundationdb::Database;

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args = Args::parse()?;
        let _network = foundationdb::boot()?;
        let db = Database::new(args.cluster_file.as_deref())?;
        clear_foundationdb_prefix(&db, args.prefix.as_bytes())?;
        println!("cleared_prefix={}", args.prefix);
        Ok(())
    }

    struct Args {
        prefix: String,
        cluster_file: Option<String>,
    }

    impl Args {
        fn parse() -> Result<Self, String> {
            let mut prefix = None;
            let mut cluster_file = env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE").ok();
            let mut args = env::args().skip(1);
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--prefix" => prefix = args.next(),
                    "--cluster-file" => cluster_file = args.next(),
                    "--help" | "-h" => return Err(usage()),
                    other => return Err(format!("unknown argument {other}\n{}", usage())),
                }
            }
            Ok(Self {
                prefix: prefix.ok_or_else(usage)?,
                cluster_file,
            })
        }
    }

    fn usage() -> String {
        "usage: ducklake-fdb-prefix-clear --prefix <trailing-slash-prefix> [--cluster-file <path>]"
            .to_owned()
    }
}
