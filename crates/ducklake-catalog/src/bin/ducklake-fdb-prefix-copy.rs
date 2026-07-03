#[cfg(not(feature = "foundationdb"))]
fn main() {
    eprintln!("ducklake-fdb-prefix-copy requires `--features foundationdb`");
    std::process::exit(2);
}

#[cfg(feature = "foundationdb")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    prefix_copy::run()
}

#[cfg(feature = "foundationdb")]
mod prefix_copy {
    use std::env;

    use ducklake_catalog::keys::prefix_end;
    use foundationdb::{Database, RangeOption, options::TransactionOption};
    use futures::{TryStreamExt, executor::block_on};

    const DEFAULT_BATCH_KEYS: usize = 100;
    const DEFAULT_BATCH_BYTES: usize = 512 * 1024;

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args = Args::parse()?;
        validate_prefixes(&args.source_prefix, &args.destination_prefix)?;
        let _network = unsafe { foundationdb::boot() };
        let db = Database::new(args.cluster_file.as_deref())?;

        if args.clear_destination {
            clear_destination(&db, &args.destination_prefix)?;
        }
        if args.sample_keys > 0 {
            print_sample_keys(&db, &args.source_prefix, args.sample_keys)?;
        }
        let copied = copy_prefix(&db, &args)?;
        println!("source_prefix={}", args.source_prefix);
        println!("destination_prefix={}", args.destination_prefix);
        println!("copied_key_count={}", copied.keys);
        println!("copied_value_bytes={}", copied.value_bytes);
        println!("copy_batches={}", copied.batches);
        Ok(())
    }

    struct Args {
        source_prefix: String,
        destination_prefix: String,
        cluster_file: Option<String>,
        clear_destination: bool,
        batch_keys: usize,
        batch_bytes: usize,
        sample_keys: usize,
    }

    struct CopyStats {
        keys: usize,
        value_bytes: usize,
        batches: usize,
    }

    impl Args {
        fn parse() -> Result<Self, String> {
            let mut source_prefix = None;
            let mut destination_prefix = None;
            let mut cluster_file = env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE").ok();
            let mut clear_destination = false;
            let mut batch_keys = DEFAULT_BATCH_KEYS;
            let mut batch_bytes = DEFAULT_BATCH_BYTES;
            let mut sample_keys = 0;

            let mut args = env::args().skip(1);
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--source-prefix" => source_prefix = args.next(),
                    "--destination-prefix" => destination_prefix = args.next(),
                    "--cluster-file" => cluster_file = args.next(),
                    "--clear-destination" => clear_destination = true,
                    "--batch-keys" => batch_keys = parse_usize(args.next(), "--batch-keys")?,
                    "--batch-bytes" => batch_bytes = parse_usize(args.next(), "--batch-bytes")?,
                    "--sample-keys" => sample_keys = parse_usize(args.next(), "--sample-keys")?,
                    "--help" | "-h" => return Err(usage()),
                    other => return Err(format!("unknown argument {other}\n{}", usage())),
                }
            }

            Ok(Self {
                source_prefix: source_prefix.ok_or_else(usage)?,
                destination_prefix: destination_prefix.ok_or_else(usage)?,
                cluster_file,
                clear_destination,
                batch_keys,
                batch_bytes,
                sample_keys,
            })
        }
    }

    fn print_sample_keys(
        db: &Database,
        source_prefix: &str,
        limit: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let source = source_prefix.as_bytes();
        for (index, (key, value)) in read_batch(db, source, &prefix_end(source), limit)?
            .into_iter()
            .enumerate()
        {
            let suffix = key.strip_prefix(source).unwrap_or(&key);
            println!("sample_key\t{}\t{}\t{}", index, hex(suffix), value.len());
        }
        Ok(())
    }

    fn copy_prefix(db: &Database, args: &Args) -> Result<CopyStats, Box<dyn std::error::Error>> {
        let source = args.source_prefix.as_bytes();
        let destination = args.destination_prefix.as_bytes();
        let source_end = prefix_end(source);
        let mut cursor = source.to_vec();
        let mut stats = CopyStats {
            keys: 0,
            value_bytes: 0,
            batches: 0,
        };

        loop {
            let rows = read_batch(db, &cursor, &source_end, args.batch_keys)?;
            if rows.is_empty() {
                return Ok(stats);
            }
            cursor = next_after(rows.last().expect("non-empty batch").0.as_slice());
            for chunk in copy_chunks(rows, args.batch_bytes) {
                write_batch(db, source, destination, &chunk)?;
                stats.keys = stats.keys.saturating_add(chunk.len());
                stats.value_bytes = stats
                    .value_bytes
                    .saturating_add(chunk.iter().map(|(_, value)| value.len()).sum::<usize>());
                stats.batches = stats.batches.saturating_add(1);
            }
        }
    }

    fn read_batch(
        db: &Database,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Box<dyn std::error::Error>> {
        let trx = db.create_trx()?;
        trx.set_option(TransactionOption::Timeout(5_000))?;
        let mut range = RangeOption::from(start.to_vec()..end.to_vec());
        range.limit = Some(limit.max(1));
        let rows = block_on(
            trx.get_ranges_keyvalues(range, false)
                .try_collect::<Vec<_>>(),
        )?;
        Ok(rows
            .into_iter()
            .map(|row| (row.key().to_vec(), row.value().to_vec()))
            .collect())
    }

    fn write_batch(
        db: &Database,
        source_prefix: &[u8],
        destination_prefix: &[u8],
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let trx = db.create_trx()?;
        trx.set_option(TransactionOption::Timeout(5_000))?;
        for (source_key, value) in rows {
            let suffix = source_key
                .strip_prefix(source_prefix)
                .ok_or("source key escaped source prefix")?;
            let mut destination_key = Vec::with_capacity(destination_prefix.len() + suffix.len());
            destination_key.extend_from_slice(destination_prefix);
            destination_key.extend_from_slice(suffix);
            trx.set(&destination_key, value);
        }
        block_on(trx.commit())?;
        Ok(())
    }

    fn clear_destination(
        db: &Database,
        destination_prefix: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let trx = db.create_trx()?;
        trx.set_option(TransactionOption::Timeout(5_000))?;
        trx.clear_range(
            destination_prefix.as_bytes(),
            &prefix_end(destination_prefix.as_bytes()),
        );
        block_on(trx.commit())?;
        Ok(())
    }

    fn copy_chunks(
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        batch_bytes: usize,
    ) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0usize;
        let max_bytes = batch_bytes.max(1);
        for row in rows {
            let row_bytes = row.0.len().saturating_add(row.1.len());
            if !current.is_empty() && current_bytes.saturating_add(row_bytes) > max_bytes {
                chunks.push(current);
                current = Vec::new();
                current_bytes = 0;
            }
            current_bytes = current_bytes.saturating_add(row_bytes);
            current.push(row);
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        chunks
    }

    fn validate_prefixes(source: &str, destination: &str) -> Result<(), String> {
        if source.is_empty() || destination.is_empty() {
            return Err("source and destination prefixes must be non-empty".to_owned());
        }
        if source == destination {
            return Err("source and destination prefixes must differ".to_owned());
        }
        if source.as_bytes().starts_with(destination.as_bytes())
            || destination.as_bytes().starts_with(source.as_bytes())
        {
            return Err("source and destination prefixes must not overlap".to_owned());
        }
        Ok(())
    }

    fn next_after(key: &[u8]) -> Vec<u8> {
        let mut next = key.to_vec();
        next.push(0);
        next
    }

    fn parse_usize(value: Option<String>, flag: &str) -> Result<usize, String> {
        value
            .ok_or_else(|| format!("{flag} requires a value"))?
            .parse()
            .map_err(|error| format!("{flag} must be a positive integer: {error}"))
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn usage() -> String {
        "usage: ducklake-fdb-prefix-copy --source-prefix <prefix> --destination-prefix <prefix> [--cluster-file <path>] [--clear-destination] [--batch-keys <n>] [--batch-bytes <n>] [--sample-keys <n>]".to_owned()
    }
}
