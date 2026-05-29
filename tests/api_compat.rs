use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use pi_server::config::ServerConfig;
use pi_server::opencode_routes::OPENCODE_ROUTES;
use pi_server::server::app;
use regex::Regex;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

#[tokio::test]
async fn doc_route_exposes_the_registered_opencode_surface() {
    let harness = Harness::new();
    let app = app(harness.config());

    let response = app
        .oneshot(request(Method::GET, "/doc", Body::empty()))
        .await
        .expect("GET /doc");
    assert_eq!(response.status(), StatusCode::OK);

    let body = response_json(response).await;
    let paths = body
        .get("paths")
        .and_then(Value::as_object)
        .expect("OpenAPI paths object");

    let documented = paths
        .iter()
        .flat_map(|(path, item)| {
            item.as_object().into_iter().flat_map(move |methods| {
                methods
                    .keys()
                    .map(move |method| (method.to_uppercase(), path.clone()))
            })
        })
        .collect::<BTreeSet<_>>();
    let expected = route_set();

    assert_eq!(documented, expected);
}

#[test]
fn route_matrix_has_no_duplicates() {
    let routes = route_set();
    assert_eq!(routes.len(), OPENCODE_ROUTES.len());
}

#[test]
fn route_matrix_matches_local_opencode_source_when_available() {
    let Some(opencode_root) = opencode_root() else {
        eprintln!("Skipping source compatibility check: opencode source tree not found");
        return;
    };

    let parsed = parse_opencode_routes(&opencode_root);
    let expected = route_set();

    assert_eq!(parsed, expected);
}

#[tokio::test]
async fn bootstrap_routes_return_attach_compatible_shapes() {
    let harness = Harness::new();
    let app = app(harness.config());

    let config_providers = get_json(app.clone(), "/config/providers").await;
    assert!(config_providers["providers"].is_array());
    assert_eq!(config_providers["providers"][0]["id"], "pi");
    assert_eq!(config_providers["default"]["pi"], "default");
    assert_eq!(
        config_providers["providers"][0]["models"]["default"]["providerID"],
        "pi"
    );
    assert_eq!(
        config_providers["providers"][0]["models"]["default"]["cost"]["input"],
        0
    );

    let provider_list = get_json(app.clone(), "/provider").await;
    assert!(provider_list["all"].is_array());
    assert_eq!(provider_list["all"][0]["id"], "pi");
    assert_eq!(provider_list["default"]["pi"], "default");
    assert_eq!(provider_list["connected"][0], "pi");

    let agents = get_json(app, "/agent").await;
    assert_eq!(agents[0]["name"], "build");
    assert!(agents[0]["permission"].is_array());
    assert!(agents[0]["options"].is_object());
}

#[tokio::test]
async fn session_prompt_is_backed_by_a_pi_rpc_process() {
    let harness = Harness::new();
    let app = app(harness.config());

    let session = create_session(app.clone()).await;
    let session_id = session
        .get("id")
        .and_then(Value::as_str)
        .expect("session id");

    let response = app
        .clone()
        .oneshot(request(
            Method::POST,
            &format!("/session/{session_id}/message"),
            Body::from(
                json!({
                    "parts": [{ "type": "text", "text": "hello" }]
                })
                .to_string(),
            ),
        ))
        .await
        .expect("POST prompt");
    assert_eq!(response.status(), StatusCode::OK);

    let body = response_json(response).await;
    assert_eq!(body["info"]["role"], "assistant");
    assert_eq!(body["parts"][0]["text"], "echo: hello");

    let response = app
        .oneshot(request(
            Method::GET,
            &format!("/session/{session_id}/message"),
            Body::empty(),
        ))
        .await
        .expect("GET messages");
    assert_eq!(response.status(), StatusCode::OK);
    let messages = response_json(response).await;
    assert_eq!(messages.as_array().expect("messages array").len(), 2);
}

#[tokio::test]
async fn prompts_on_multiple_sessions_can_run_concurrently() {
    let harness = Harness::new();
    let app = app(harness.config());

    let mut sessions = Vec::new();
    for _ in 0..4 {
        sessions.push(create_session(app.clone()).await);
    }

    let prompts = sessions.into_iter().enumerate().map(|(index, session)| {
        let app = app.clone();
        async move {
            let session_id = session
                .get("id")
                .and_then(Value::as_str)
                .expect("session id");
            let response = app
                .oneshot(request(
                    Method::POST,
                    &format!("/session/{session_id}/message"),
                    Body::from(
                        json!({
                            "parts": [{ "type": "text", "text": format!("hello {index}") }]
                        })
                        .to_string(),
                    ),
                ))
                .await
                .expect("POST prompt");
            assert_eq!(response.status(), StatusCode::OK);
            response_json(response).await
        }
    });

    let results = futures::future::join_all(prompts).await;
    let texts = results
        .iter()
        .map(|body| body["parts"][0]["text"].as_str().unwrap().to_string())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        texts,
        (0..4)
            .map(|index| format!("echo: hello {index}"))
            .collect::<BTreeSet<_>>()
    );
}

struct Harness {
    _tmp: TempDir,
    fake_pi: PathBuf,
    workdir: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let fake_pi = tmp.path().join("fake-pi");
        write_fake_pi(&fake_pi);
        let workdir = tmp.path().join("workdir");
        fs::create_dir(&workdir).expect("workdir");
        Self {
            _tmp: tmp,
            fake_pi,
            workdir,
        }
    }

    fn config(&self) -> ServerConfig {
        ServerConfig {
            hostname: "127.0.0.1".parse().unwrap(),
            port: 0,
            pi_bin: self.fake_pi.clone(),
            directory: self.workdir.clone(),
        }
    }
}

async fn create_session(app: axum::Router) -> Value {
    let response = app
        .oneshot(request(Method::POST, "/session", Body::empty()))
        .await
        .expect("POST /session");
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

async fn get_json(app: axum::Router, uri: &str) -> Value {
    let response = app
        .oneshot(request(Method::GET, uri, Body::empty()))
        .await
        .expect("GET route");
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

fn request(method: Method, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body)
        .expect("request")
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("json response")
}

fn route_set() -> BTreeSet<(String, String)> {
    OPENCODE_ROUTES
        .iter()
        .map(|route| (route.method.to_string(), route.opencode_path.to_string()))
        .collect()
}

fn opencode_root() -> Option<PathBuf> {
    let path = std::env::var_os("OPENCODE_SOURCE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/Users/mikesoylu/Projects/opencode"));
    path.exists().then_some(path)
}

fn parse_opencode_routes(root: &Path) -> BTreeSet<(String, String)> {
    let group_root = root.join("packages/opencode/src/server/routes/instance/httpapi/groups");
    let files = collect_ts_files(&group_root);
    let endpoint_re =
        Regex::new(r#"HttpApiEndpoint\.(get|post|put|delete|patch)\(\s*"[^"]+"\s*,\s*([^,\n]+)"#)
            .unwrap();
    let mut routes = BTreeSet::new();
    routes.insert(("GET".to_string(), "/doc".to_string()));

    for file in files {
        let source = fs::read_to_string(&file).expect("read opencode route group");
        let paths = parse_path_constants(&source);
        for capture in endpoint_re.captures_iter(&source) {
            let method = capture[1].to_uppercase();
            let expression = capture[2].trim();
            let path = resolve_path_expression(expression, &paths).unwrap_or_else(|| {
                panic!(
                    "unresolved route expression {expression} in {}",
                    file.display()
                )
            });
            routes.insert((method, normalize_opencode_path(&path)));
        }
    }

    routes
}

fn collect_ts_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(path).expect("read group dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("ts") {
                files.push(path);
            }
        }
    }
    files
}

fn parse_path_constants(source: &str) -> BTreeMap<String, String> {
    let root_re = Regex::new(r#"const root = "([^"]+)""#).unwrap();
    let root = root_re
        .captures(source)
        .map(|capture| capture[1].to_string())
        .unwrap_or_default();
    let object_re = Regex::new(r#"(?s)export const (\w+Paths) = \{(.*?)\} as const"#).unwrap();
    let entry_re = Regex::new(r#"(?m)^\s*(\w+):\s*([^,\n]+)"#).unwrap();

    let mut paths = BTreeMap::new();
    if !root.is_empty() {
        paths.insert("root".to_string(), root.clone());
    }
    for object in object_re.captures_iter(source) {
        let name = &object[1];
        let body = &object[2];
        for entry in entry_re.captures_iter(body) {
            let key = &entry[1];
            let value = evaluate_path_literal(entry[2].trim(), &root)
                .unwrap_or_else(|| panic!("unsupported path literal {}", &entry[2]));
            paths.insert(format!("{name}.{key}"), value);
        }
    }
    paths
}

fn resolve_path_expression(expression: &str, paths: &BTreeMap<String, String>) -> Option<String> {
    let root = paths.get("root").map(String::as_str).unwrap_or_default();
    evaluate_path_literal(expression, root).or_else(|| paths.get(expression).cloned())
}

fn evaluate_path_literal(expression: &str, root: &str) -> Option<String> {
    let expression = expression.trim();
    if expression == "root" && !root.is_empty() {
        return Some(root.to_string());
    }
    if let Some(stripped) = expression
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
    {
        return Some(stripped.to_string());
    }
    if let Some(stripped) = expression
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
    {
        return Some(stripped.replace("${root}", root));
    }
    None
}

fn normalize_opencode_path(path: &str) -> String {
    let param_re = Regex::new(r#":([A-Za-z][A-Za-z0-9_]*)"#).unwrap();
    param_re.replace_all(path, "{$1}").to_string()
}

fn write_fake_pi(path: &Path) {
    let mut file = fs::File::create(path).expect("create fake pi");
    file.write_all(
        br#"#!/usr/bin/env python3
import json
import sys

for line in sys.stdin:
    try:
        request = json.loads(line)
    except Exception:
        continue
    request_id = request.get("id")
    command = request.get("type")
    if command == "prompt":
        message = request.get("message", "")
        print(json.dumps({"type": "response", "id": request_id, "command": "prompt", "success": True}), flush=True)
        print(json.dumps({
            "type": "agent_end",
            "messages": [{
                "role": "assistant",
                "content": [{"type": "text", "text": "echo: " + message}],
                "api": "test",
                "provider": "test",
                "model": "test",
                "usage": {},
                "stopReason": "stop",
                "timestamp": 0
            }],
            "error": None
        }), flush=True)
    elif command == "get_messages":
        print(json.dumps({"type": "response", "id": request_id, "command": "get_messages", "success": True, "data": {"messages": []}}), flush=True)
    else:
        print(json.dumps({"type": "response", "id": request_id, "command": command, "success": True}), flush=True)
"#,
    )
    .expect("write fake pi");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path).expect("fake pi metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod fake pi");
    }
}
