use std::process::ExitCode;

fn main() -> ExitCode {
    match ducklake_fdb_sim_runner::run(std::env::args().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ducklake-fdb-sim-runner: {error}");
            ExitCode::FAILURE
        }
    }
}
