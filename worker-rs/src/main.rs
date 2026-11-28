/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! `worker-rs`: a benchmark worker in Rust that the Java coordinator drives over the worker HTTP
//! API (ADR 0021). It serves HTTP/1.1 and h2c on one port, like the Java `BenchmarkWorker`.

mod api;
mod stats;
mod synthetic;
mod worker;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use api::BearerToken;

const TOKEN_ENV: &str = "OMB_WORKER_TOKEN";

const USAGE: &str = "Usage: worker-rs [options]
  -p, --port <port>      HTTP port to listen on, 0 for any free port (default 8080)
  -b, --bind <address>   Address to listen on; use 127.0.0.1 to accept only local connections
                         (default 0.0.0.0)
  --token-file <path>    File holding the shared bearer token that every request must send;
                         overrides the OMB_WORKER_TOKEN environment variable. Required unless
                         --bind is a loopback address or --insecure-no-auth is set
  --insecure-no-auth     Allow an unauthenticated API on a non-loopback --bind address
  -h, --help             Show this message";

#[derive(Debug, PartialEq, Eq)]
struct Arguments {
    port: u16,
    bind: String,
    token_file: Option<PathBuf>,
    insecure_no_auth: bool,
}

fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Arguments>, String> {
    let mut arguments = Arguments { port: 8080, bind: "0.0.0.0".into(), token_file: None, insecure_no_auth: false };
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "-p" | "--port" => {
                let port = value(&arg)?;
                arguments.port = port.parse().map_err(|_| format!("Invalid port {port}"))?;
            }
            "-b" | "--bind" => arguments.bind = value(&arg)?,
            "--token-file" => arguments.token_file = Some(value(&arg)?.into()),
            "--insecure-no-auth" => arguments.insecure_no_auth = true,
            "--tls-cert" | "--tls-key" => return Err("worker-rs does not serve TLS yet; see ADR 0021".into()),
            other => return Err(format!("Unknown option {other}")),
        }
    }
    Ok(Some(arguments))
}

/// The token from `--token-file` if given, else from `OMB_WORKER_TOKEN`; trimmed, and a blank
/// environment variable means none, as in the Java worker.
fn resolve_token(token_file: Option<&PathBuf>, environment: Option<String>) -> Result<Option<String>, String> {
    if let Some(path) = token_file {
        let token =
            std::fs::read_to_string(path).map_err(|e| format!("Cannot read token file {}: {e}", path.display()))?;
        let token = token.trim();
        if token.is_empty() {
            return Err(format!("Token file {} is empty", path.display()));
        }
        return Ok(Some(token.to_owned()));
    }
    Ok(environment.map(|t| t.trim().to_owned()).filter(|t| !t.is_empty()))
}

async fn run(arguments: Arguments) -> Result<(), String> {
    let token = resolve_token(arguments.token_file.as_ref(), std::env::var(TOKEN_ENV).ok())?;
    let address: SocketAddr = tokio::net::lookup_host((arguments.bind.as_str(), arguments.port))
        .await
        .map_err(|e| format!("Cannot resolve --bind {}: {e}", arguments.bind))?
        .next()
        .ok_or_else(|| format!("Cannot resolve --bind {}", arguments.bind))?;
    if token.is_none() && !arguments.insecure_no_auth && !address.ip().is_loopback() {
        return Err(format!(
            "No worker token configured and --bind {} is not a loopback address. Set {TOKEN_ENV} or \
             --token-file, bind to 127.0.0.1, or pass --insecure-no-auth to run the API unauthenticated",
            arguments.bind
        ));
    }
    if token.is_none() {
        eprintln!(
            "No worker token configured (set {TOKEN_ENV} or --token-file); anyone who can reach port {} can control \
             this worker",
            arguments.port
        );
    }
    let worker = Arc::new(worker::Worker::default());
    let router = api::router(worker.clone(), token.as_deref().map(BearerToken::new));
    let listener =
        tokio::net::TcpListener::bind(address).await.map_err(|e| format!("Cannot listen on {address}: {e}"))?;
    let local = listener.local_addr().map_err(|e| e.to_string())?;
    // Read by scripts and the contract test to find a --port 0 worker
    println!("worker-rs listening on {local}");
    axum::serve(listener, router).with_graceful_shutdown(shutdown_signal()).await.map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || worker.stop_all()).await.ok();
    Ok(())
}

// SIGINT, or SIGTERM as Kubernetes and process supervisors send it
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let Ok(mut terminate) = signal(SignalKind::terminate()) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

fn main() -> ExitCode {
    let arguments = match parse(std::env::args().skip(1)) {
        Ok(Some(arguments)) => arguments,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("{error}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    match runtime.block_on(run(arguments)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Result<Option<Arguments>, String> {
        parse(values.iter().map(|v| (*v).to_owned()))
    }

    #[test]
    fn parses_the_java_worker_options() {
        let parsed =
            args(&["-p", "0", "--bind", "127.0.0.1", "--token-file", "t", "--insecure-no-auth"]).unwrap().unwrap();
        assert_eq!(
            parsed,
            Arguments { port: 0, bind: "127.0.0.1".into(), token_file: Some("t".into()), insecure_no_auth: true }
        );
        assert_eq!(args(&["--help"]).unwrap(), None);
        assert!(args(&["--port"]).is_err());
        assert!(args(&["--port", "x"]).is_err());
        assert!(args(&["--tls-cert", "c"]).unwrap_err().contains("TLS"));
        assert!(args(&["--frobnicate"]).is_err());
    }

    #[test]
    fn resolves_the_token_like_the_java_worker() {
        assert_eq!(resolve_token(None, Some("  abc \n".into())).unwrap(), Some("abc".into()));
        assert_eq!(resolve_token(None, Some("   ".into())).unwrap(), None);
        assert_eq!(resolve_token(None, None).unwrap(), None);
        let dir = std::env::temp_dir().join(format!("worker-rs-token-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("token");
        std::fs::write(&file, "from-file\n").unwrap();
        assert_eq!(resolve_token(Some(&file), Some("env".into())).unwrap(), Some("from-file".into()));
        std::fs::write(&file, " \n").unwrap();
        assert!(resolve_token(Some(&file), None).is_err());
        assert!(resolve_token(Some(&dir.join("missing")), None).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
