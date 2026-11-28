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

//! The worker HTTP API, route for route the Java `WorkerHandler`, with its security defaults
//! (ADR 0012): the bearer token is checked before anything else, browser requests are refused,
//! and POSTs must be JSON or YAML.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::worker::{self, Worker};

pub const HEALTH: &str = "/health";
const BEARER: &str = "Bearer ";
// Payload templates travel base64-encoded in start-load
const BODY_LIMIT: usize = 256 * 1024 * 1024;

/// Checks `Authorization: Bearer <token>` in constant time for tokens of the same length.
#[derive(Clone)]
pub struct BearerToken(Arc<[u8]>);

impl BearerToken {
    pub fn new(token: &str) -> Self {
        BearerToken(token.as_bytes().into())
    }

    fn matches(&self, authorization: Option<&str>) -> bool {
        let Some(value) = authorization else {
            return false;
        };
        if value.len() < BEARER.len() || !value[..BEARER.len()].eq_ignore_ascii_case(BEARER) {
            return false;
        }
        let presented = &value.as_bytes()[BEARER.len()..];
        presented.len() == self.0.len() && presented.iter().zip(self.0.iter()).fold(0, |d, (a, b)| d | (a ^ b)) == 0
    }
}

#[derive(Clone)]
struct App {
    worker: Arc<Worker>,
    token: Option<BearerToken>,
}

pub fn router(worker: Arc<Worker>, token: Option<BearerToken>) -> Router {
    let app = App { worker, token };
    Router::new()
        .route("/initialize-driver", post(initialize_driver))
        .route("/set-run-id", post(ok))
        .route("/set-phase", post(set_phase))
        .route("/create-topics", post(create_topics))
        .route("/create-producers", post(create_producers))
        .route("/probe-producers", post(probe_producers))
        .route("/create-consumers", post(create_consumers))
        .route("/pause-consumers", post(pause_consumers))
        .route("/resume-consumers", post(resume_consumers))
        .route("/start-load", post(start_load))
        .route("/adjust-publish-rate", post(adjust_publish_rate))
        .route("/stop-all", post(stop_all))
        .route("/period-stats", get(period_stats))
        .route("/cumulative-latencies", get(cumulative_latencies))
        .route("/counters-stats", get(counters_stats))
        .route("/reset-stats", post(reset_stats))
        .route(HEALTH, get(health))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(middleware::from_fn_with_state(app.clone(), guard))
        .with_state(app)
}

fn reply(status: StatusCode, message: &'static str) -> Response {
    (status, message).into_response()
}

// Runs before every route, in the order of the Java before-handlers
async fn guard(State(app): State<App>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let authorization = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    if let Some(token) = &app.token {
        if request.uri().path() != HEALTH && !token.matches(authorization) {
            let mut response = reply(StatusCode::UNAUTHORIZED, "Missing or invalid bearer token");
            response.headers_mut().insert(header::WWW_AUTHENTICATE, "Bearer".parse().expect("header"));
            return response;
        }
    }
    if headers.contains_key(header::ORIGIN) {
        return reply(StatusCode::FORBIDDEN, "Requests with an Origin header are not accepted");
    }
    // Without a token, a page could rebind its own host name to the worker's address
    if app.token.is_none() && !is_address_host(host(&request)) {
        return reply(StatusCode::FORBIDDEN, "Without a worker token, the Host must be an IP address or localhost");
    }
    if request.method() == Method::POST && !accepted_type(headers) {
        return reply(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "POST requests must use Content-Type application/json or application/yaml",
        );
    }
    next.run(request).await
}

// HTTP/2 requests carry the host in the URI authority instead of a Host header
fn host(request: &Request) -> Option<&str> {
    request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| request.uri().authority().map(|a| a.as_str()))
}

fn is_address_host(host: Option<&str>) -> bool {
    let Some(host) = host else {
        return true; // HTTP/1.0 clients; browsers always send Host
    };
    if let Some(rest) = host.strip_prefix('[') {
        return rest.find(']').is_some_and(|end| end > 0); // bracketed IPv6 literal
    }
    let name = host.split(':').next().unwrap_or_default();
    name.eq_ignore_ascii_case("localhost") || is_ipv4(name)
}

// Four groups of 1 to 3 digits, as the Java worker's pattern
fn is_ipv4(name: &str) -> bool {
    let groups: Vec<&str> = name.split('.').collect();
    groups.len() == 4 && groups.iter().all(|g| (1..=3).contains(&g.len()) && g.bytes().all(|b| b.is_ascii_digit()))
}

fn accepted_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let media = value.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    media == "application/json" || media == "application/yaml"
}

impl IntoResponse for worker::Error {
    fn into_response(self) -> Response {
        match self {
            worker::Error::Client(message) => (StatusCode::BAD_REQUEST, message).into_response(),
            worker::Error::Server(message) => (StatusCode::INTERNAL_SERVER_ERROR, message).into_response(),
        }
    }
}

type Reply = Result<Response, worker::Error>;

fn parse<T: DeserializeOwned>(body: &Bytes) -> Result<T, worker::Error> {
    serde_json::from_slice(body).map_err(|e| worker::Error::Client(format!("Invalid request body: {e}")))
}

fn json<T: Serialize>(value: &T) -> Reply {
    let body = serde_json::to_vec(value).map_err(|e| worker::Error::Server(e.to_string()))?;
    Ok(([(header::CONTENT_TYPE, "application/json")], body).into_response())
}

// Worker calls can block (a stop joins the send threads), so they run off the async threads
async fn blocking<F>(app: App, call: F) -> Reply
where
    F: FnOnce(&Worker) -> Reply + std::marker::Send + 'static,
{
    tokio::task::spawn_blocking(move || call(&app.worker))
        .await
        .unwrap_or_else(|e| Err(worker::Error::Server(format!("Worker call failed: {e}"))))
}

fn done(result: worker::Result<()>) -> Reply {
    result.map(|()| StatusCode::OK.into_response())
}

async fn ok() -> StatusCode {
    StatusCode::OK
}

async fn initialize_driver(State(app): State<App>, body: Bytes) -> Reply {
    blocking(app, move |w| done(w.initialize_driver(&body))).await
}

async fn set_phase(State(app): State<App>, body: Bytes) -> Reply {
    let phase = parse(&body)?;
    app.worker.set_phase(phase);
    Ok(StatusCode::OK.into_response())
}

async fn create_topics(State(app): State<App>, body: Bytes) -> Reply {
    let info: worker::TopicsInfo = parse(&body)?;
    blocking(app, move |w| json(&w.create_topics(&info)?)).await
}

async fn create_producers(State(app): State<App>, body: Bytes) -> Reply {
    let topics: Vec<String> = parse(&body)?;
    blocking(app, move |w| done(w.create_producers(&topics))).await
}

async fn create_consumers(State(app): State<App>, body: Bytes) -> Reply {
    let assignment: worker::ConsumerAssignment = parse(&body)?;
    blocking(app, move |w| done(w.create_consumers(&assignment))).await
}

async fn probe_producers(State(app): State<App>) -> Reply {
    blocking(app, |w| done(w.probe_producers())).await
}

async fn pause_consumers(State(app): State<App>) -> Reply {
    done(app.worker.pause_consumers())
}

async fn resume_consumers(State(app): State<App>) -> Reply {
    done(app.worker.resume_consumers())
}

async fn start_load(State(app): State<App>, body: Bytes) -> Reply {
    let assignment: worker::ProducerWorkAssignment = parse(&body)?;
    blocking(app, move |w| done(w.start_load(&assignment))).await
}

async fn adjust_publish_rate(State(app): State<App>, body: Bytes) -> Reply {
    let rate: f64 = parse(&body)?;
    done(app.worker.adjust_publish_rate(rate))
}

async fn stop_all(State(app): State<App>) -> Reply {
    blocking(app, |w| done(w.stop_all())).await
}

async fn period_stats(State(app): State<App>) -> Reply {
    blocking(app, |w| json(&w.period_stats()?)).await
}

async fn cumulative_latencies(State(app): State<App>) -> Reply {
    blocking(app, |w| json(&w.cumulative_latencies()?)).await
}

async fn counters_stats(State(app): State<App>) -> Reply {
    json(&app.worker.counters_stats()?)
}

async fn reset_stats(State(app): State<App>) -> Reply {
    blocking(app, |w| done(w.reset_stats())).await
}

async fn health(State(app): State<App>, headers: HeaderMap) -> Reply {
    let mut health = app.worker.health();
    // The error text can name brokers and configuration, so only token holders see it
    let authorization = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    if app.token.as_ref().is_some_and(|t| !t.matches(authorization)) {
        health.last_error = None;
    }
    let status = if health.ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    let mut response = json(&health)?;
    *response.status_mut() = status;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn call(router: &Router, request: Request) -> (StatusCode, String) {
        let response = router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    fn post(path: &str, content_type: &str, auth: Option<&str>, body: &str) -> Request {
        let mut builder =
            Request::post(path).header(header::HOST, "10.0.0.1:8080").header(header::CONTENT_TYPE, content_type);
        if let Some(auth) = auth {
            builder = builder.header(header::AUTHORIZATION, auth);
        }
        builder.body(Body::from(body.to_owned())).unwrap()
    }

    fn get_request(path: &str, host: &str, auth: Option<&str>) -> Request {
        let mut builder = Request::get(path).header(header::HOST, host);
        if let Some(auth) = auth {
            builder = builder.header(header::AUTHORIZATION, auth);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn requires_the_token_everywhere_but_health() {
        let router = router(Arc::default(), Some(BearerToken::new("secret")));
        let (status, _) = call(&router, get_request("/counters-stats", "worker.example", None)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = call(&router, get_request("/counters-stats", "worker.example", Some("Bearer wrong!"))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // A token holder may use any host name
        let (status, body) =
            call(&router, get_request("/counters-stats", "worker.example", Some("bearer secret"))).await;
        assert_eq!((status, body.as_str()), (StatusCode::INTERNAL_SERVER_ERROR, "No driver is initialized"));
        let (status, body) = call(&router, get_request(HEALTH, "worker.example", None)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"driver\":\"NOT_STARTED\"") && body.contains("\"runtime\":\"rust\""), "{body}");
        // An unauthenticated caller learns nothing, not even the accepted types
        let (status, _) = call(&router, post("/stop-all", "text/plain", None, "")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn refuses_browser_requests_and_other_content_types() {
        let router = router(Arc::default(), None);
        let mut request = get_request(HEALTH, "127.0.0.1:8080", None);
        request.headers_mut().insert(header::ORIGIN, "https://evil.example".parse().unwrap());
        assert_eq!(call(&router, request).await.0, StatusCode::FORBIDDEN);
        assert_eq!(call(&router, get_request(HEALTH, "rebound.example:8080", None)).await.0, StatusCode::FORBIDDEN);
        assert_eq!(call(&router, get_request(HEALTH, "localhost:8080", None)).await.0, StatusCode::OK);
        assert_eq!(call(&router, get_request(HEALTH, "[::1]:8080", None)).await.0, StatusCode::OK);
        assert_eq!(
            call(&router, post("/stop-all", "text/plain", None, "")).await.0,
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert_eq!(
            call(&router, post("/stop-all", "application/json; charset=utf-8", None, "")).await.0,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn answers_bad_requests_with_400() {
        let router = router(Arc::default(), None);
        let (status, _) = call(&router, post("/set-phase", "application/json", None, "\"LUNCH\"")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) =
            call(&router, post("/initialize-driver", "application/yaml", None, "driverClass: x.Kafka\n")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let yaml = "name: s\ndriverClass: synthetic\n";
        assert_eq!(call(&router, post("/initialize-driver", "application/yaml", None, yaml)).await.0, StatusCode::OK);
        let (status, body) = call(
            &router,
            post("/create-topics", "application/json", None, r#"{"numberOfTopics":2,"numberOfPartitionsPerTopic":1}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(serde_json::from_str::<Vec<String>>(&body).unwrap().len(), 2);
        let (status, body) = call(&router, get_request("/period-stats", "127.0.0.1", None)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"publishLatency\":\"HIST"), "{body}");
        assert_eq!(call(&router, post("/stop-all", "application/json", None, "")).await.0, StatusCode::OK);
    }

    #[test]
    fn matches_tokens_exactly() {
        let token = BearerToken::new("abc");
        assert!(token.matches(Some("Bearer abc")));
        assert!(token.matches(Some("BEARER abc")));
        assert!(!token.matches(Some("Bearer abcd")));
        assert!(!token.matches(Some("Bearer")));
        assert!(!token.matches(Some("Basic abc")));
        assert!(!token.matches(None));
    }
}
