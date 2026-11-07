use std::{
    collections::BTreeSet,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

const DEFAULT_ARTIFACT_ROOT: &str = "run-artifacts/fdb-sim";
const DEFAULT_BUGGIFY: &str = "on";
const DEFAULT_FDBSERVER: &str = "fdbserver";
const FDBSERVER_FALLBACKS: &[&str] = &[
    "/usr/local/libexec/fdbserver",
    "/opt/homebrew/libexec/fdbserver",
];
const DEFAULT_LIBRARY_NAME: &str = "ducklake_fdb_sim_workload";
const DEFAULT_LIBRARY_PATH: &str = "target/release";
const DEFAULT_PROFILE: &str = "smoke";
const DEFAULT_TEST_FILE: &str = "crates/ducklake-fdb-sim-workload/simulation/catalog_smoke.toml";
const DEFAULT_WORKLOAD: &str = "catalog_smoke";

pub fn run<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    let args = CliArgs::parse(args.into_iter())?;
    match args.command.as_str() {
        "run" => run_simulation(args),
        "help" | "--help" | "-h" => Err(help_text()),
        other => Err(format!("unknown command '{other}'\n{}", help_text())),
    }
}

struct CliArgs {
    command: String,
    workload: String,
    profile: String,
    seed: u64,
    buggify: String,
    fdbserver: String,
    test_file: PathBuf,
    artifact_root: PathBuf,
    library_path: String,
    library_name: String,
}

impl CliArgs {
    fn parse<I>(mut args: I) -> Result<Self, String>
    where
        I: Iterator<Item = String>,
    {
        let command = args.next().unwrap_or_else(|| "run".to_owned());
        let mut parsed = Self {
            command,
            workload: DEFAULT_WORKLOAD.to_owned(),
            profile: DEFAULT_PROFILE.to_owned(),
            seed: 1,
            buggify: DEFAULT_BUGGIFY.to_owned(),
            fdbserver: DEFAULT_FDBSERVER.to_owned(),
            test_file: PathBuf::from(DEFAULT_TEST_FILE),
            artifact_root: PathBuf::from(DEFAULT_ARTIFACT_ROOT),
            library_path: DEFAULT_LIBRARY_PATH.to_owned(),
            library_name: DEFAULT_LIBRARY_NAME.to_owned(),
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--workload" => parsed.workload = next_arg(&mut args, "--workload")?,
                "--profile" => parsed.profile = next_arg(&mut args, "--profile")?,
                "--seed" => parsed.seed = parse_seed(&next_arg(&mut args, "--seed")?)?,
                "--buggify" => parsed.buggify = next_arg(&mut args, "--buggify")?,
                "--fdbserver" => parsed.fdbserver = next_arg(&mut args, "--fdbserver")?,
                "--test-file" => {
                    parsed.test_file = PathBuf::from(next_arg(&mut args, "--test-file")?)
                }
                "--artifact-root" => {
                    parsed.artifact_root = PathBuf::from(next_arg(&mut args, "--artifact-root")?);
                }
                "--library-path" => parsed.library_path = next_arg(&mut args, "--library-path")?,
                "--library-name" => parsed.library_name = next_arg(&mut args, "--library-name")?,
                "--help" | "-h" => return Err(help_text()),
                other => return Err(format!("unknown argument '{other}'\n{}", help_text())),
            }
        }
        Ok(parsed)
    }

    fn rerun_command(&self) -> String {
        format!(
            "cargo run -p ducklake-fdb-sim-runner -- run --workload {} --profile {} --seed {} \
             --buggify {} --fdbserver {} --test-file {} --artifact-root {} --library-path {} \
             --library-name {}",
            shell_word(&self.workload),
            shell_word(&self.profile),
            self.seed,
            shell_word(&self.buggify),
            shell_word(&self.fdbserver),
            shell_word(&self.test_file.display().to_string()),
            shell_word(&self.artifact_root.display().to_string()),
            shell_word(&self.library_path),
            shell_word(&self.library_name),
        )
    }
}

fn run_simulation(args: CliArgs) -> Result<(), String> {
    let artifact_dir = artifact_dir(&args)?;
    fs::create_dir_all(&artifact_dir).map_err(|err| {
        format!(
            "failed to create artifact directory {}: {err}",
            artifact_dir.display()
        )
    })?;
    let test_file = artifact_dir.join("simulation.toml");
    materialize_test_file(&args, &test_file)?;
    fs::write(
        artifact_dir.join("rerun.sh"),
        format!("{}\n", args.rerun_command()),
    )
    .map_err(|err| format!("failed to write rerun command: {err}"))?;
    fs::write(
        artifact_dir.join("run-metadata.txt"),
        metadata_text(&args, &artifact_dir),
    )
    .map_err(|err| format!("failed to write run metadata: {err}"))?;
    println!(
        "ducklake fdb simulation artifact: {}",
        artifact_dir.display()
    );
    std::io::stdout()
        .flush()
        .map_err(|err| format!("failed to flush artifact path: {err}"))?;

    let output_path = artifact_dir.join("fdbserver-output.log");
    let output = fs::File::create(&output_path)
        .map_err(|err| format!("failed to create {}: {err}", output_path.display()))?;
    let output_err = output
        .try_clone()
        .map_err(|err| format!("failed to clone simulation log handle: {err}"))?;
    let traces_before = trace_files()?;
    let simfdb_existed_before = Path::new("simfdb").exists();
    let fdbserver = resolve_fdbserver(&args.fdbserver);
    let status = Command::new(&fdbserver)
        .arg("-r")
        .arg("simulation")
        .arg("-f")
        .arg(&test_file)
        .arg("--seed")
        .arg(args.seed.to_string())
        .arg("--buggify")
        .arg(&args.buggify)
        .stdout(Stdio::from(output))
        .stderr(Stdio::from(output_err))
        .status()
        .map_err(|err| format!("failed to run {fdbserver}: {err}"))?;

    write_metric_lines(&output_path, &artifact_dir.join("metrics.log"))?;
    copy_new_trace_files(&artifact_dir, &traces_before)?;
    cleanup_simulation_scratch(&traces_before, simfdb_existed_before)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "simulation failed with status {status}; see {}",
            output_path.display()
        ))
    }
}

fn artifact_dir(args: &CliArgs) -> Result<PathBuf, String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system clock is before UNIX_EPOCH: {err}"))?
        .as_secs();
    Ok(args
        .artifact_root
        .join(&args.profile)
        .join(format!("{}-seed-{}-{timestamp}", args.workload, args.seed)))
}

fn materialize_test_file(args: &CliArgs, destination: &Path) -> Result<(), String> {
    let raw = fs::read_to_string(&args.test_file)
        .map_err(|err| format!("failed to read {}: {err}", args.test_file.display()))?;
    let rendered = raw
        .replace("__WORKLOAD__", &args.workload)
        .replace("__PROFILE__", &args.profile)
        .replace(
            "__ACTIVE_CLIENT_COUNT__",
            &active_client_count_for_profile(&args.profile).to_string(),
        )
        .replace("__LIBRARY_PATH__", &args.library_path)
        .replace("__LIBRARY_NAME__", &args.library_name)
        .replace(
            "__ARTIFACT_ROOT__",
            &args.artifact_root.display().to_string(),
        );
    fs::write(destination, rendered)
        .map_err(|err| format!("failed to write {}: {err}", destination.display()))
}

fn active_client_count_for_profile(profile: &str) -> i32 {
    match profile {
        "multi-client" | "buggify" => 3,
        _ => 1,
    }
}

fn write_metric_lines(output_path: &Path, metrics_path: &Path) -> Result<(), String> {
    let output = fs::read_to_string(output_path)
        .map_err(|err| format!("failed to read {}: {err}", output_path.display()))?;
    let metrics = output
        .lines()
        .filter(|line| line.starts_with("Metric "))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(metrics_path, format!("{metrics}\n"))
        .map_err(|err| format!("failed to write {}: {err}", metrics_path.display()))
}

fn trace_files() -> Result<BTreeSet<PathBuf>, String> {
    let entries = fs::read_dir(".").map_err(|err| format!("failed to list trace files: {err}"))?;
    let mut paths = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to inspect trace file: {err}"))?;
        let path = entry.path();
        if is_trace_xml(&path) {
            paths.insert(path);
        }
    }
    Ok(paths)
}

fn copy_new_trace_files(
    artifact_dir: &Path,
    traces_before: &BTreeSet<PathBuf>,
) -> Result<(), String> {
    let traces_after = trace_files()?;
    let trace_dir = artifact_dir.join("traces");
    for path in traces_after.difference(traces_before) {
        fs::create_dir_all(&trace_dir).map_err(|err| {
            format!(
                "failed to create trace artifact directory {}: {err}",
                trace_dir.display()
            )
        })?;
        let Some(file_name) = path.file_name() else {
            continue;
        };
        fs::copy(path, trace_dir.join(file_name)).map_err(|err| {
            format!(
                "failed to copy trace file {} into {}: {err}",
                path.display(),
                trace_dir.display()
            )
        })?;
    }
    Ok(())
}

fn cleanup_simulation_scratch(
    traces_before: &BTreeSet<PathBuf>,
    simfdb_existed_before: bool,
) -> Result<(), String> {
    let traces_after = trace_files()?;
    for path in traces_after.difference(traces_before) {
        fs::remove_file(path)
            .map_err(|err| format!("failed to remove scratch trace {}: {err}", path.display()))?;
    }
    if !simfdb_existed_before && Path::new("simfdb").exists() {
        fs::remove_dir_all("simfdb")
            .map_err(|err| format!("failed to remove simulator scratch directory: {err}"))?;
    }
    Ok(())
}

fn is_trace_xml(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("trace.") && name.ends_with(".xml"))
}

fn metadata_text(args: &CliArgs, artifact_dir: &Path) -> String {
    format!(
        "workload={}\nprofile={}\nseed={}\nbuggify={}\nfdbserver={}\ntest_file={}\nartifact_dir={}\nlibrary_path={}\nlibrary_name={}\nrerun={}\n",
        args.workload,
        args.profile,
        args.seed,
        args.buggify,
        args.fdbserver,
        args.test_file.display(),
        artifact_dir.display(),
        args.library_path,
        args.library_name,
        args.rerun_command(),
    )
}

fn resolve_fdbserver(configured: &str) -> String {
    if configured != DEFAULT_FDBSERVER || command_exists(configured) {
        return configured.to_owned();
    }
    FDBSERVER_FALLBACKS
        .iter()
        .copied()
        .find(|path| Path::new(path).is_file())
        .unwrap_or(configured)
        .to_owned()
}

fn command_exists(command: &str) -> bool {
    if command.contains('/') {
        return Path::new(command).is_file();
    }
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&paths).any(|path| path.join(command).is_file())
}

fn next_arg<I>(args: &mut I, flag: &str) -> Result<String, String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_seed(value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|err| format!("--seed must be an unsigned integer: {err}"))
}

fn shell_word(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-'))
    {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn help_text() -> String {
    "usage: ducklake-fdb-sim-runner run [--workload catalog_smoke] [--profile smoke] \
     [--seed 1] [--buggify on] [--fdbserver fdbserver] [--test-file path] \
     [--artifact-root path] [--library-path path] [--library-name name]"
        .to_owned()
}
