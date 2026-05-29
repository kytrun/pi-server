use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Json, Router};
use futures::Stream;
use ignore::WalkBuilder;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::config::ServerConfig;
use crate::error::{Error, Result};
use crate::ids;
use crate::models::{
    CreateSessionPayload, FileNode, MessageWithParts, PromptPayload, SessionInfo,
    assistant_message, now_ms, path_to_string, user_message,
};
use crate::opencode_routes::OPENCODE_ROUTES;
use crate::pi_rpc::PiRpcClient;

#[derive(Debug, Clone)]
pub struct AppState {
    config: ServerConfig,
    sessions: Arc<RwLock<HashMap<String, Arc<Mutex<SessionRecord>>>>>,
    global_events: broadcast::Sender<Value>,
}

#[derive(Debug)]
struct SessionRecord {
    info: SessionInfo,
    rpc: Arc<PiRpcClient>,
    messages: Vec<MessageWithParts>,
}

impl AppState {
    pub fn new(config: ServerConfig) -> Self {
        let (global_events, _) = broadcast::channel(4096);
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            global_events,
        }
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<Mutex<SessionRecord>>> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("session not found: {session_id}")))
    }

    async fn create_session(&self, payload: Option<CreateSessionPayload>) -> Result<SessionInfo> {
        let mut info = SessionInfo::new(
            &self.config.directory,
            payload.as_ref().and_then(|p| p.title.clone()),
        );
        if let Some(payload) = payload {
            info.parent_id = payload.parent_id;
            info.agent = payload.agent.or(info.agent);
            info.model = payload.model.or(info.model);
            info.permission = payload.permission;
            info.workspace_id = payload.workspace_id;
        }

        let rpc = PiRpcClient::spawn(&self.config.pi_bin, &self.config.directory).await?;
        let record = Arc::new(Mutex::new(SessionRecord {
            info: info.clone(),
            rpc: Arc::clone(&rpc),
            messages: Vec::new(),
        }));
        self.sessions
            .write()
            .await
            .insert(info.id.clone(), Arc::clone(&record));
        self.forward_session_events(&info, rpc);
        self.publish(json!({
            "type": "session.created",
            "properties": { "info": info.clone() },
        }));
        Ok(info)
    }

    fn publish(&self, payload: Value) {
        let _ = self.global_events.send(json!({
            "directory": self.config.directory.display().to_string(),
            "payload": payload,
        }));
    }

    fn forward_session_events(&self, session: &SessionInfo, rpc: Arc<PiRpcClient>) {
        let mut rx = rpc.subscribe();
        let tx = self.global_events.clone();
        let directory = self.config.directory.display().to_string();
        let session_id = session.id.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(payload) => {
                        let _ = tx.send(json!({
                            "directory": directory,
                            "payload": {
                                "type": "pi.rpc.event",
                                "properties": {
                                    "sessionID": session_id,
                                    "event": payload,
                                }
                            }
                        }));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
}

pub fn app(config: ServerConfig) -> Router {
    app_with_state(AppState::new(config))
}

pub fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route("/doc", get(doc))
        .route("/global/health", get(global_health))
        .route("/global/event", get(global_event))
        .route("/event", get(global_event))
        .route(
            "/global/config",
            get(empty_object).patch(echo_or_empty_object),
        )
        .route("/global/dispose", post(ok_true))
        .route("/global/upgrade", post(upgrade))
        .route("/auth/:provider_id", put(ok_true).delete(ok_true))
        .route("/log", post(ok_true))
        .route("/config", get(empty_object).patch(echo_or_empty_object))
        .route("/config/providers", get(config_providers))
        .route("/provider", get(providers))
        .route("/provider/auth", get(empty_object))
        .route("/provider/:provider_id/oauth/authorize", post(empty_object))
        .route("/provider/:provider_id/oauth/callback", post(ok_true))
        .route("/api/provider", get(v2_providers))
        .route("/api/provider/:provider_id", get(v2_provider))
        .route("/api/model", get(v2_models))
        .route("/session", get(list_sessions).post(create_session))
        .route("/session/status", get(session_status))
        .route(
            "/session/:session_id",
            get(get_session)
                .patch(update_session)
                .delete(remove_session),
        )
        .route("/session/:session_id/children", get(empty_array))
        .route("/session/:session_id/todo", get(empty_array))
        .route("/session/:session_id/diff", get(empty_array))
        .route(
            "/session/:session_id/message",
            get(session_messages).post(prompt_session),
        )
        .route(
            "/session/:session_id/message/:message_id",
            get(session_message).delete(ok_true),
        )
        .route(
            "/session/:session_id/message/:message_id/part/:part_id",
            delete(ok_true).patch(echo_or_empty_object),
        )
        .route("/session/:session_id/fork", post(fork_session))
        .route("/session/:session_id/abort", post(abort_session))
        .route(
            "/session/:session_id/share",
            post(share_session).delete(unshare_session),
        )
        .route("/session/:session_id/init", post(ok_true))
        .route("/session/:session_id/summarize", post(ok_true))
        .route(
            "/session/:session_id/prompt_async",
            post(prompt_session_async),
        )
        .route("/session/:session_id/command", post(command_session))
        .route("/session/:session_id/shell", post(shell_session))
        .route("/session/:session_id/revert", post(echo_session))
        .route("/session/:session_id/unrevert", post(echo_session))
        .route(
            "/session/:session_id/permissions/:permission_id",
            post(ok_true),
        )
        .route("/api/session", get(v2_sessions))
        .route("/api/session/:session_id/message", get(v2_session_messages))
        .route("/api/session/:session_id/context", get(v2_session_context))
        .route("/api/session/:session_id/prompt", post(v2_prompt_session))
        .route("/api/session/:session_id/compact", post(no_content))
        .route("/api/session/:session_id/wait", post(no_content))
        .route("/path", get(paths))
        .route("/vcs", get(vcs_info))
        .route("/vcs/status", get(empty_array))
        .route("/vcs/diff", get(empty_array))
        .route("/vcs/diff/raw", get(empty_text))
        .route("/vcs/apply", post(vcs_apply))
        .route("/command", get(empty_array))
        .route("/agent", get(agents))
        .route("/skill", get(empty_array))
        .route("/lsp", get(empty_array))
        .route("/formatter", get(empty_array))
        .route("/instance/dispose", post(ok_true))
        .route("/find", get(find_text))
        .route("/find/file", get(find_file))
        .route("/find/symbol", get(empty_array))
        .route("/file", get(list_file))
        .route("/file/content", get(file_content))
        .route("/file/status", get(empty_array))
        .route("/mcp", get(empty_object).post(empty_object))
        .route(
            "/mcp/:name/auth",
            post(mcp_auth_start).delete(mcp_auth_remove),
        )
        .route("/mcp/:name/auth/callback", post(empty_object))
        .route("/mcp/:name/auth/authenticate", post(empty_object))
        .route("/mcp/:name/connect", post(ok_true))
        .route("/mcp/:name/disconnect", post(ok_true))
        .route("/permission", get(empty_array))
        .route("/permission/:request_id/reply", post(ok_true))
        .route("/question", get(empty_array))
        .route("/question/:request_id/reply", post(ok_true))
        .route("/question/:request_id/reject", post(ok_true))
        .route("/project", get(project_list))
        .route("/project/current", get(project_current))
        .route("/project/git/init", post(project_current))
        .route("/project/:project_id", patch(echo_or_empty_object))
        .route("/pty/shells", get(pty_shells))
        .route("/pty", get(empty_array).post(pty_create))
        .route("/pty/:pty_id", get(pty_get).put(pty_get).delete(ok_true))
        .route("/pty/:pty_id/connect-token", post(pty_token))
        .route("/pty/:pty_id/connect", get(ok_true))
        .route("/sync/start", post(ok_true))
        .route("/sync/replay", post(sync_replay))
        .route("/sync/steal", post(echo_or_empty_object))
        .route("/sync/history", post(empty_array))
        .route("/experimental/console", get(console_state))
        .route(
            "/experimental/console/orgs",
            get(|| async { Json(json!({ "orgs": [] })) }),
        )
        .route("/experimental/console/switch", post(ok_true))
        .route("/experimental/tool", get(empty_array))
        .route("/experimental/tool/ids", get(empty_array))
        .route(
            "/experimental/worktree",
            get(empty_array).post(worktree_create).delete(ok_true),
        )
        .route("/experimental/worktree/reset", post(ok_true))
        .route("/experimental/session", get(experimental_sessions))
        .route("/experimental/resource", get(empty_object))
        .route("/experimental/workspace/adapter", get(empty_array))
        .route(
            "/experimental/workspace",
            get(empty_array).post(workspace_create),
        )
        .route("/experimental/workspace/sync-list", post(no_content))
        .route("/experimental/workspace/status", get(empty_array))
        .route("/experimental/workspace/warp", post(no_content))
        .route("/experimental/workspace/:id", delete(ok_true))
        .route("/tui/append-prompt", post(ok_true))
        .route("/tui/open-help", post(ok_true))
        .route("/tui/open-sessions", post(ok_true))
        .route("/tui/open-themes", post(ok_true))
        .route("/tui/open-models", post(ok_true))
        .route("/tui/submit-prompt", post(ok_true))
        .route("/tui/clear-prompt", post(ok_true))
        .route("/tui/execute-command", post(ok_true))
        .route("/tui/show-toast", post(ok_true))
        .route("/tui/publish", post(ok_true))
        .route("/tui/select-session", post(ok_true))
        .route("/tui/control/next", get(|| async { Json(Value::Null) }))
        .route("/tui/control/response", post(ok_true))
        .fallback(not_found)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn serve(config: ServerConfig) -> anyhow::Result<SocketAddr> {
    let addr = SocketAddr::new(config.hostname, config.port);
    let listener = TcpListener::bind(addr).await?;
    let actual_addr = listener.local_addr()?;
    info!("pi-server listening on http://{actual_addr}");
    println!("pi-server listening on http://{actual_addr}");
    axum::serve(listener, app(config)).await?;
    Ok(actual_addr)
}

async fn doc() -> Json<Value> {
    let paths = OPENCODE_ROUTES
        .iter()
        .fold(serde_json::Map::new(), |mut paths, route| {
            let item = paths
                .entry(route.opencode_path.to_string())
                .or_insert_with(|| json!({}));
            item.as_object_mut().expect("path item object").insert(
                route.method.to_ascii_lowercase(),
                json!({
                    "responses": {
                        "200": { "description": "OK" }
                    }
                }),
            );
            paths
        });
    Json(json!({
        "openapi": "3.1.0",
        "info": {
            "title": "pi-server OpenCode-compatible API",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "paths": paths,
    }))
}

async fn global_health() -> Json<Value> {
    Json(json!({ "healthy": true, "version": env!("CARGO_PKG_VERSION") }))
}

async fn global_event(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = std::result::Result<Event, std::convert::Infallible>>> {
    let stream =
        BroadcastStream::new(state.global_events.subscribe()).filter_map(|item| match item {
            Ok(value) => Some(Ok(Event::default().data(value.to_string()))),
            Err(_) => None,
        });
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn create_session(State(state): State<AppState>, body: Bytes) -> Result<Json<SessionInfo>> {
    let payload = parse_optional_json::<CreateSessionPayload>(&body)?;
    state.create_session(payload).await.map(Json)
}

async fn list_sessions(State(state): State<AppState>) -> Json<Vec<SessionInfo>> {
    let sessions = state.sessions.read().await;
    let mut items = Vec::with_capacity(sessions.len());
    for record in sessions.values() {
        items.push(record.lock().await.info.clone());
    }
    items.sort_by(|a, b| b.time.updated.cmp(&a.time.updated));
    Json(items)
}

async fn experimental_sessions(State(state): State<AppState>) -> Json<Vec<Value>> {
    let sessions = list_sessions(State(state)).await.0;
    Json(
        sessions
            .into_iter()
            .map(|session| {
                let mut value = json!(session);
                if let Some(object) = value.as_object_mut() {
                    object.insert("project".to_string(), Value::Null);
                }
                value
            })
            .collect(),
    )
}

async fn v2_sessions(State(state): State<AppState>) -> Json<Value> {
    let sessions = list_sessions(State(state)).await.0;
    Json(json!({ "items": sessions, "cursor": {} }))
}

async fn get_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<SessionInfo>> {
    let record = state.get_session(&session_id).await?;
    Ok(Json(record.lock().await.info.clone()))
}

async fn update_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<SessionInfo>> {
    let patch = parse_optional_value(&body)?.unwrap_or_else(|| json!({}));
    let record = state.get_session(&session_id).await?;
    let mut record = record.lock().await;
    if let Some(title) = patch.get("title").and_then(Value::as_str) {
        record.info.title = title.to_string();
        record.info.slug = ids::slug(title);
    }
    if let Some(archived) = patch
        .get("time")
        .and_then(|time| time.get("archived"))
        .and_then(Value::as_i64)
    {
        record.info.time.archived = Some(archived);
    }
    record.info.touch();
    Ok(Json(record.info.clone()))
}

async fn remove_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<bool>> {
    let record = state
        .sessions
        .write()
        .await
        .remove(&session_id)
        .ok_or_else(|| Error::not_found(format!("session not found: {session_id}")))?;
    record.lock().await.rpc.shutdown().await;
    state.publish(json!({
        "type": "session.deleted",
        "properties": { "sessionID": session_id },
    }));
    Ok(Json(true))
}

async fn session_status(State(state): State<AppState>) -> Json<Value> {
    let sessions = state.sessions.read().await;
    let mut map = serde_json::Map::new();
    for id in sessions.keys() {
        map.insert(id.clone(), json!({ "type": "idle" }));
    }
    Json(Value::Object(map))
}

async fn prompt_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<MessageWithParts>> {
    prompt_impl(state, session_id, body, false).await.map(Json)
}

async fn v2_prompt_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<Value>> {
    let message = prompt_impl(state, session_id, body, true).await?;
    Ok(Json(json!(message)))
}

async fn command_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<MessageWithParts>> {
    let value = parse_optional_value(&body)?.unwrap_or_default();
    let command = value
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let arguments = value
        .get("arguments")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let text = if arguments.is_empty() {
        format!("/{command}")
    } else {
        format!("/{command} {arguments}")
    };
    let mut payload = value.as_object().cloned().unwrap_or_default();
    payload.insert(
        "parts".to_string(),
        json!([{ "type": "text", "text": text }]),
    );
    prompt_impl(
        state,
        session_id,
        Bytes::from(Value::Object(payload).to_string()),
        false,
    )
    .await
    .map(Json)
}

async fn shell_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<MessageWithParts>> {
    let mut value = parse_optional_value(&body)?.unwrap_or_default();
    let command = value
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    value["parts"] = json!([{ "type": "text", "text": command }]);
    prompt_impl(state, session_id, Bytes::from(value.to_string()), false)
        .await
        .map(Json)
}

async fn prompt_session_async(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<StatusCode> {
    let payload = parse_prompt_payload(&body)?;
    let text = payload.text();
    let record = state.get_session(&session_id).await?;
    let rpc = {
        let mut record = record.lock().await;
        let user = user_message(&record.info, &payload, text.clone());
        record.messages.push(user);
        record.info.touch();
        Arc::clone(&record.rpc)
    };
    rpc.prompt_async(text).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn prompt_impl(
    state: AppState,
    session_id: String,
    body: Bytes,
    _v2: bool,
) -> Result<MessageWithParts> {
    let payload = parse_prompt_payload(&body)?;
    let text = payload.text();
    if text.trim().is_empty() {
        return Err(Error::bad_request("prompt parts must include text"));
    }

    let record = state.get_session(&session_id).await?;
    let (rpc, user, directory) = {
        let mut record = record.lock().await;
        let user = user_message(&record.info, &payload, text.clone());
        let directory = PathBuf::from(record.info.directory.clone());
        record.messages.push(user.clone());
        record.info.touch();
        state.publish(json!({
            "type": "message.updated",
            "properties": { "sessionID": session_id, "info": user.info.clone() },
        }));
        (Arc::clone(&record.rpc), user, directory)
    };

    if payload.no_reply {
        return Ok(user);
    }

    let event = rpc.prompt(&text).await?;
    let assistant = {
        let mut record = record.lock().await;
        let assistant = assistant_from_agent_end(&record.info, user.info.id(), &directory, &event)
            .unwrap_or_else(|| assistant_message(&record.info, user.info.id(), "", &directory));
        record.messages.push(assistant.clone());
        record.info.touch();
        state.publish(json!({
            "type": "message.updated",
            "properties": { "sessionID": session_id, "info": assistant.info.clone() },
        }));
        assistant
    };

    Ok(assistant)
}

async fn session_messages(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<Vec<MessageWithParts>>> {
    let record = state.get_session(&session_id).await?;
    Ok(Json(record.lock().await.messages.clone()))
}

async fn v2_session_messages(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<Value>> {
    let messages = session_messages(State(state), AxumPath(session_id))
        .await?
        .0;
    Ok(Json(json!({ "items": messages, "cursor": {} })))
}

async fn v2_session_context(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<Vec<MessageWithParts>>> {
    session_messages(State(state), AxumPath(session_id)).await
}

async fn session_message(
    State(state): State<AppState>,
    AxumPath((session_id, message_id)): AxumPath<(String, String)>,
) -> Result<Json<MessageWithParts>> {
    let record = state.get_session(&session_id).await?;
    record
        .lock()
        .await
        .messages
        .iter()
        .find(|message| message.info.id() == message_id)
        .cloned()
        .map(Json)
        .ok_or_else(|| Error::not_found(format!("message not found: {message_id}")))
}

async fn fork_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<SessionInfo>> {
    let parent = state.get_session(&session_id).await?;
    let title = {
        let parent = parent.lock().await;
        Some(format!("{} (fork #1)", parent.info.title))
    };
    state
        .create_session(Some(CreateSessionPayload {
            parent_id: Some(session_id),
            title,
            agent: None,
            model: None,
            permission: None,
            workspace_id: None,
        }))
        .await
        .map(Json)
}

async fn abort_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<bool>> {
    let record = state.get_session(&session_id).await?;
    record.lock().await.rpc.abort().await?;
    Ok(Json(true))
}

async fn share_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<SessionInfo>> {
    let record = state.get_session(&session_id).await?;
    let mut record = record.lock().await;
    let host = headers
        .get("host")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    record.info.share = Some(json!({ "url": format!("http://{host}/share/{}", record.info.id) }));
    Ok(Json(record.info.clone()))
}

async fn unshare_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<SessionInfo>> {
    let record = state.get_session(&session_id).await?;
    let mut record = record.lock().await;
    record.info.share = None;
    Ok(Json(record.info.clone()))
}

async fn echo_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<SessionInfo>> {
    let record = state.get_session(&session_id).await?;
    Ok(Json(record.lock().await.info.clone()))
}

async fn paths(State(state): State<AppState>) -> Json<Value> {
    let directory = state.config.directory.display().to_string();
    let home = dirs::home_dir().map_or_else(|| ".".to_string(), path_to_string);
    Json(json!({
        "home": home,
        "state": directory,
        "config": directory,
        "worktree": directory,
        "directory": directory,
    }))
}

async fn find_file(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<String>>> {
    let needle = query
        .get("query")
        .cloned()
        .unwrap_or_default()
        .to_lowercase();
    let include_dirs = query.get("dirs").is_some_and(|value| value == "true");
    let kind = query.get("type").map(String::as_str);
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        .min(200);
    let mut results = Vec::new();
    for entry in WalkBuilder::new(&state.config.directory)
        .hidden(false)
        .build()
        .flatten()
    {
        if results.len() >= limit {
            break;
        }
        let file_type = entry.file_type();
        let is_dir = file_type.is_some_and(|ft| ft.is_dir());
        if is_dir && !include_dirs {
            continue;
        }
        if kind == Some("file") && is_dir {
            continue;
        }
        if kind == Some("directory") && !is_dir {
            continue;
        }
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.to_lowercase().contains(&needle) {
            results.push(relative_path(&state.config.directory, path));
        }
    }
    Ok(Json(results))
}

async fn find_text(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<Value>>> {
    let pattern = query.get("pattern").cloned().unwrap_or_default();
    if pattern.is_empty() {
        return Ok(Json(Vec::new()));
    }
    let regex = regex::Regex::new(&pattern).map_err(|err| Error::bad_request(err.to_string()))?;
    let mut matches = Vec::new();
    for entry in WalkBuilder::new(&state.config.directory)
        .hidden(false)
        .build()
        .flatten()
        .filter(|entry| entry.file_type().is_some_and(|ft| ft.is_file()))
    {
        if matches.len() >= 200 {
            break;
        }
        let Ok(content) = tokio::fs::read_to_string(entry.path()).await else {
            continue;
        };
        for (line_index, line) in content.lines().enumerate() {
            if regex.is_match(line) {
                matches.push(json!({
                    "path": relative_path(&state.config.directory, entry.path()),
                    "line": line_index + 1,
                    "text": line,
                }));
                break;
            }
        }
    }
    Ok(Json(matches))
}

async fn list_file(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<FileNode>>> {
    let requested = query.get("path").map_or(".", String::as_str);
    let path = resolve_in_root(&state.config.directory, requested);
    let mut entries = tokio::fs::read_dir(&path).await?;
    let mut nodes = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        nodes.push(FileNode {
            name: entry.file_name().to_string_lossy().into_owned(),
            path: relative_path(&state.config.directory, &entry.path()),
            kind: if metadata.is_dir() {
                "directory"
            } else {
                "file"
            }
            .to_string(),
        });
    }
    nodes.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(Json(nodes))
}

async fn file_content(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>> {
    let requested = query
        .get("path")
        .ok_or_else(|| Error::bad_request("missing path"))?;
    let path = resolve_in_root(&state.config.directory, requested);
    let content = tokio::fs::read_to_string(&path).await?;
    Ok(Json(json!({
        "type": "raw",
        "content": content,
    })))
}

async fn project_current(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "id": ids::project_id(),
        "name": state.config.directory.file_name().and_then(|n| n.to_str()),
        "worktree": state.config.directory.display().to_string(),
    }))
}

async fn project_list(State(state): State<AppState>) -> Json<Vec<Value>> {
    Json(vec![project_current(State(state)).await.0])
}

async fn vcs_info(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "root": state.config.directory.display().to_string(),
        "branch": null,
    }))
}

async fn vcs_apply() -> Json<Value> {
    Json(json!({ "ok": true }))
}

async fn agents() -> Json<Vec<Value>> {
    Json(vec![json!({
        "name": "build",
        "description": "Default pi coding agent",
        "mode": "primary",
        "native": true,
        "permission": [],
        "options": {},
    })])
}

async fn providers() -> Json<Value> {
    Json(json!({
        "all": [provider_info()],
        "default": { "pi": "default" },
        "connected": ["pi"],
    }))
}

async fn v2_providers() -> Json<Vec<Value>> {
    Json(vec![json!({
        "id": "pi",
        "name": "Pi",
        "models": [{ "id": "default", "name": "Default" }],
    })])
}

async fn v2_provider(AxumPath(provider_id): AxumPath<String>) -> Result<Json<Value>> {
    if provider_id != "pi" {
        return Err(Error::not_found(format!(
            "provider not found: {provider_id}"
        )));
    }
    Ok(Json(json!({
        "id": "pi",
        "name": "Pi",
        "models": [{ "id": "default", "name": "Default" }],
    })))
}

async fn v2_models() -> Json<Vec<Value>> {
    Json(vec![json!({
        "id": "default",
        "name": "Default",
        "providerID": "pi",
    })])
}

async fn config_providers() -> Json<Value> {
    Json(json!({
        "providers": [provider_info()],
        "default": { "pi": "default" },
    }))
}

fn provider_info() -> Value {
    json!({
        "id": "pi",
        "name": "Pi",
        "source": "custom",
        "env": [],
        "options": {},
        "models": {
            "default": provider_model(),
        },
    })
}

fn provider_model() -> Value {
    json!({
        "id": "default",
        "providerID": "pi",
        "api": {
            "id": "pi",
            "url": "http://localhost",
            "npm": "@pi/cli",
        },
        "name": "Default",
        "capabilities": {
            "temperature": true,
            "reasoning": false,
            "attachment": true,
            "toolcall": false,
            "input": {
                "text": true,
                "audio": false,
                "image": true,
                "video": false,
                "pdf": true,
            },
            "output": {
                "text": true,
                "audio": false,
                "image": false,
                "video": false,
                "pdf": false,
            },
            "interleaved": false,
        },
        "cost": {
            "input": 0,
            "output": 0,
            "cache": {
                "read": 0,
                "write": 0,
            },
        },
        "limit": {
            "context": 128000,
            "output": 4096,
        },
        "status": "active",
        "options": {},
        "headers": {},
        "release_date": "2026-01-01",
    })
}

async fn pty_shells() -> Json<Vec<Value>> {
    Json(vec![json!({
        "path": "/bin/zsh",
        "name": "zsh",
        "acceptable": true,
    })])
}

async fn pty_create() -> Json<Value> {
    let now = now_ms();
    Json(json!({
        "id": ids::request_id(),
        "time": { "created": now, "updated": now },
    }))
}

async fn pty_get(AxumPath(pty_id): AxumPath<String>) -> Json<Value> {
    let now = now_ms();
    Json(json!({
        "id": pty_id,
        "time": { "created": now, "updated": now },
    }))
}

async fn pty_token(AxumPath(pty_id): AxumPath<String>) -> Json<Value> {
    Json(json!({
        "ptyID": pty_id,
        "token": ids::request_id(),
        "expires": now_ms() + 60_000,
    }))
}

async fn mcp_auth_start() -> Json<Value> {
    Json(json!({
        "authorizationUrl": "http://localhost",
        "oauthState": ids::request_id(),
    }))
}

async fn mcp_auth_remove() -> Json<Value> {
    Json(json!({ "success": true }))
}

async fn sync_replay() -> Json<Value> {
    Json(json!({ "sessionID": ids::session_id() }))
}

async fn console_state() -> Json<Value> {
    Json(json!({
        "consoleManagedProviders": [],
        "switchableOrgCount": 0,
    }))
}

async fn worktree_create(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "path": state.config.directory.display().to_string(),
    }))
}

async fn workspace_create(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "id": ids::workspace_id(),
        "projectID": ids::project_id(),
        "name": "local",
        "directory": state.config.directory.display().to_string(),
    }))
}

async fn upgrade() -> Json<Value> {
    Json(json!({ "success": true, "version": env!("CARGO_PKG_VERSION") }))
}

async fn empty_object() -> Json<Value> {
    Json(json!({}))
}

async fn empty_array() -> Json<Vec<Value>> {
    Json(Vec::new())
}

async fn ok_true() -> Json<bool> {
    Json(true)
}

async fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn empty_text() -> &'static str {
    ""
}

async fn echo_or_empty_object(body: Bytes) -> Json<Value> {
    Json(
        parse_optional_value(&body)
            .ok()
            .flatten()
            .unwrap_or_else(|| json!({})),
    )
}

async fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "name": "NotFoundError",
            "data": { "message": "Route not found" }
        })),
    )
        .into_response()
}

fn parse_prompt_payload(body: &Bytes) -> Result<PromptPayload> {
    if body.trim_ascii().is_empty() {
        return Err(Error::bad_request("missing prompt payload"));
    }
    let mut value: Value = serde_json::from_slice(body)?;
    if let Some(prompt) = value.get("prompt").cloned()
        && prompt.is_object()
        && value.get("parts").is_none()
        && let Some(parts) = prompt.get("parts")
    {
        value["parts"] = parts.clone();
    }
    serde_json::from_value(value).map_err(Into::into)
}

fn parse_optional_json<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<Option<T>> {
    if body.trim_ascii().is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(body).map(Some).map_err(Into::into)
}

fn parse_optional_value(body: &Bytes) -> Result<Option<Value>> {
    parse_optional_json(body)
}

fn assistant_from_agent_end(
    session: &SessionInfo,
    parent_id: &str,
    cwd: &Path,
    event: &Value,
) -> Option<MessageWithParts> {
    if let Some(error) = event.get("error").and_then(Value::as_str)
        && !error.is_empty()
    {
        return Some(assistant_message(session, parent_id, error, cwd));
    }

    let messages = event.get("messages").and_then(Value::as_array)?;
    let assistant = messages.iter().rev().find(|message| {
        message.get("role").and_then(Value::as_str) == Some("assistant")
            || message.get("type").and_then(Value::as_str) == Some("assistant")
    })?;
    let text = extract_text(assistant);
    Some(assistant_message(session, parent_id, text, cwd))
}

fn extract_text(value: &Value) -> String {
    if let Some(content) = value.get("content") {
        return extract_content_text(content);
    }
    if let Some(content) = value
        .get("message")
        .and_then(|message| message.get("content"))
    {
        return extract_content_text(content);
    }
    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return text.to_string();
    }
    String::new()
}

fn extract_content_text(content: &Value) -> String {
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    if let Some(array) = content.as_array() {
        return array
            .iter()
            .filter_map(|block| {
                block
                    .get("text")
                    .or_else(|| block.get("thinking"))
                    .and_then(Value::as_str)
            })
            .collect::<Vec<_>>()
            .join("");
    }
    String::new()
}

fn resolve_in_root(root: &Path, requested: &str) -> PathBuf {
    let requested = Path::new(requested);
    if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        root.join(requested)
    }
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}
