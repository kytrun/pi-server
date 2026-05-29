use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::Stream;
use ignore::WalkBuilder;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::{Mutex, RwLock, broadcast};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::config::ServerConfig;
use crate::error::{Error, Result};
use crate::ids;
use crate::models::{
    AssistantMessageInfo, CompletedTime, CreateSessionPayload, CreatedTime, FileNode, MessageInfo,
    MessagePath, MessageWithParts, Part, PartTime, PromptPayload, SessionInfo, TextPart, Tokens,
    ToolPart, UserMessageInfo, assistant_message_pending_with_id, assistant_message_with_id,
    now_ms, path_to_string, user_message,
};
use crate::opencode_routes::OPENCODE_ROUTES;
use crate::pi_rpc::PiRpcClient;
use crate::storage::Storage;

#[derive(Debug, Clone)]
pub struct AppState {
    config: ServerConfig,
    project_id: String,
    storage: Storage,
    sessions: Arc<RwLock<HashMap<String, Arc<Mutex<SessionRecord>>>>>,
    statuses: Arc<RwLock<HashMap<String, Value>>>,
    permissions: Arc<RwLock<HashMap<String, PermissionRequest>>>,
    global_events: broadcast::Sender<Value>,
}

#[derive(Debug, Clone)]
struct PermissionRequest {
    session_id: String,
    request: Value,
}

#[derive(Debug)]
struct SessionRecord {
    info: SessionInfo,
    rpc: Option<Arc<PiRpcClient>>,
    messages: Vec<MessageWithParts>,
    live: LiveSessionState,
}

#[derive(Debug, Default)]
struct LiveSessionState {
    assistant: Option<LiveAssistant>,
}

#[derive(Debug)]
struct LiveAssistant {
    message_id: String,
    parent_id: String,
    directory: PathBuf,
    published_message: bool,
    parts: Vec<Part>,
    text_parts: HashMap<String, usize>,
    reasoning_parts: HashMap<String, usize>,
    tool_parts: HashMap<String, usize>,
}

#[derive(Debug, Clone, Copy)]
enum EventStreamShape {
    Instance,
    Global,
}

impl AppState {
    pub fn new(config: ServerConfig) -> Self {
        Self::try_new(config).expect("initialize app state")
    }

    pub fn try_new(config: ServerConfig) -> Result<Self> {
        let (global_events, _) = broadcast::channel(4096);
        let storage = Storage::open(&config.database)?;
        let project = storage.get_or_create_project(&config.directory)?;
        let project_id = project["id"].as_str().unwrap_or_default().to_string();
        let sessions = storage
            .list_sessions()?
            .into_iter()
            .map(|info| {
                Ok((
                    info.id.clone(),
                    Arc::new(Mutex::new(SessionRecord {
                        info,
                        rpc: None,
                        messages: Vec::new(),
                        live: LiveSessionState::default(),
                    })),
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(Self {
            config,
            project_id,
            storage,
            sessions: Arc::new(RwLock::new(sessions)),
            statuses: Arc::new(RwLock::new(HashMap::new())),
            permissions: Arc::new(RwLock::new(HashMap::new())),
            global_events,
        })
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<Value> {
        self.global_events.subscribe()
    }

    pub async fn set_session_todos(&self, session_id: &str, todos: Vec<Value>) -> Result<()> {
        for todo in &todos {
            validate_todo(todo)?;
        }
        let record = self.get_session(session_id).await?;
        let directory = record.lock().await.info.directory.clone();
        self.storage.replace_todos(session_id, &todos)?;
        self.publish_for_directory(
            directory,
            json!({
                "type": "todo.updated",
                "properties": {
                    "sessionID": session_id,
                    "todos": todos,
                },
            }),
        );
        Ok(())
    }

    pub async fn add_permission_request(&self, request: Value) -> Result<()> {
        let id = request
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::bad_request("permission request id missing"))?
            .to_string();
        let session_id = request
            .get("sessionID")
            .or_else(|| request.get("sessionId"))
            .and_then(Value::as_str)
            .ok_or_else(|| Error::bad_request("permission request sessionID missing"))?
            .to_string();
        let record = self.get_session(&session_id).await?;
        let directory = record.lock().await.info.directory.clone();
        let mut request = request;
        if let Some(request) = request.as_object_mut() {
            request.insert("id".to_string(), json!(id));
            request.insert("sessionID".to_string(), json!(session_id));
            request
                .entry("permission")
                .or_insert_with(|| json!("unknown"));
            request.entry("patterns").or_insert_with(|| json!([]));
            request.entry("metadata").or_insert_with(|| json!({}));
            request.entry("always").or_insert_with(|| json!([]));
        }
        self.permissions.write().await.insert(
            id,
            PermissionRequest {
                session_id: session_id.clone(),
                request: request.clone(),
            },
        );
        self.publish_for_directory(
            directory,
            json!({
                "type": "permission.asked",
                "properties": request,
            }),
        );
        Ok(())
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<Mutex<SessionRecord>>> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("session not found: {session_id}")))
    }

    async fn create_session(
        &self,
        payload: Option<CreateSessionPayload>,
        directory: Option<PathBuf>,
    ) -> Result<SessionInfo> {
        let directory = directory.unwrap_or_else(|| self.config.directory.clone());
        let mut info = SessionInfo::new(&directory, payload.as_ref().and_then(|p| p.title.clone()));
        let project = self.storage.get_or_create_project(&directory)?;
        self.publish_project_updated(&project);
        info.project_id = project["id"].as_str().unwrap_or_default().to_string();
        info.path = project["worktree"]
            .as_str()
            .map(|worktree| relative_path(Path::new(worktree), &directory));
        if let Some(payload) = payload {
            info.parent_id = payload.parent_id;
            info.agent = payload.agent.or(info.agent);
            info.model = payload.model.or(info.model);
            info.permission = payload
                .permission
                .as_ref()
                .map(|permission| validate_permission_ruleset(&json!(permission)))
                .transpose()?;
            info.workspace_id = payload.workspace_id;
        }

        let rpc = PiRpcClient::spawn(&self.config.pi_bin, &directory).await?;
        let record = Arc::new(Mutex::new(SessionRecord {
            info: info.clone(),
            rpc: Some(Arc::clone(&rpc)),
            messages: Vec::new(),
            live: LiveSessionState::default(),
        }));
        self.storage.save_session(&info)?;
        self.sessions
            .write()
            .await
            .insert(info.id.clone(), Arc::clone(&record));
        self.forward_session_events(&info, rpc, Arc::clone(&record));
        self.publish_session_created(&info);
        self.publish_session_updated(&info);
        Ok(info)
    }

    fn publish_for_directory(&self, directory: String, payload: Value) {
        let project = self
            .storage
            .project_id_for_directory(&directory)
            .unwrap_or_else(|_| self.project_id.clone());
        let _ = self.global_events.send(json!({
            "directory": directory,
            "project": project,
            "workspace": null,
            "payload": payload,
        }));
    }

    fn publish_session_updated(&self, info: &SessionInfo) {
        self.publish_for_directory(
            info.directory.clone(),
            json!({
                "type": "session.updated",
                "properties": {
                    "sessionID": info.id,
                    "info": info,
                },
            }),
        );
    }

    fn publish_session_created(&self, info: &SessionInfo) {
        self.publish_for_directory(
            info.directory.clone(),
            json!({
                "type": "session.created",
                "properties": {
                    "sessionID": info.id,
                    "info": info,
                },
            }),
        );
    }

    fn publish_session_deleted(&self, info: &SessionInfo) {
        self.publish_for_directory(
            info.directory.clone(),
            json!({
                "type": "session.deleted",
                "properties": {
                    "sessionID": info.id,
                    "info": info,
                },
            }),
        );
    }

    fn publish_project_updated(&self, project: &Value) {
        let Some(project_id) = project.get("id").and_then(Value::as_str) else {
            return;
        };
        let _ = self.global_events.send(json!({
            "directory": "global",
            "project": project_id,
            "workspace": null,
            "payload": {
                "type": "project.updated",
                "properties": project,
            },
        }));
    }

    fn publish_message(&self, directory: &str, message: &MessageWithParts) {
        let session_id = message.info.session_id();
        let assistant = matches!(&message.info, MessageInfo::Assistant(_));
        self.publish_for_directory(
            directory.to_string(),
            json!({
                "type": "message.updated",
                "properties": {
                    "sessionID": session_id,
                    "info": &message.info,
                },
            }),
        );
        for part in &message.parts {
            if assistant && let Some(delta) = text_delta(part) {
                self.publish_part_updated(directory, session_id, started_part(part));
                self.publish_for_directory(
                    directory.to_string(),
                    json!({
                        "type": "message.part.delta",
                        "properties": {
                            "sessionID": session_id,
                            "messageID": delta.message_id,
                            "partID": delta.part_id,
                            "field": "text",
                            "delta": delta.text,
                        },
                    }),
                );
            }
            self.publish_part_updated(directory, session_id, json!(part));
        }
    }

    fn publish_message_snapshot(&self, directory: &str, message: &MessageWithParts) {
        let session_id = message.info.session_id();
        self.publish_for_directory(
            directory.to_string(),
            json!({
                "type": "message.updated",
                "properties": {
                    "sessionID": session_id,
                    "info": &message.info,
                },
            }),
        );
        for part in &message.parts {
            self.publish_part_updated(directory, session_id, json!(part));
        }
    }

    fn publish_part_updated(&self, directory: &str, session_id: &str, part: Value) {
        self.publish_for_directory(
            directory.to_string(),
            json!({
                "type": "message.part.updated",
                "properties": {
                    "sessionID": session_id,
                    "part": part,
                    "time": now_ms(),
                },
            }),
        );
    }

    fn publish_session_status(&self, directory: &str, session_id: &str, status: Value) {
        self.publish_for_directory(
            directory.to_string(),
            json!({
                "type": "session.status",
                "properties": {
                    "sessionID": session_id,
                    "status": status,
                },
            }),
        );
    }

    async fn set_session_status(&self, session_id: &str, status: Value) {
        if status.get("type").and_then(Value::as_str) == Some("idle") {
            self.statuses.write().await.remove(session_id);
        } else {
            self.statuses
                .write()
                .await
                .insert(session_id.to_string(), status.clone());
        }
        let directory = match self.get_session(session_id).await {
            Ok(record) => record.lock().await.info.directory.clone(),
            Err(_) => self.config.directory.display().to_string(),
        };
        self.publish_session_status(&directory, session_id, status);
    }

    async fn ensure_not_busy(&self, session_id: &str) -> Result<()> {
        if self.statuses.read().await.contains_key(session_id) {
            return Err(Error::session_busy(format!(
                "session is busy: {session_id}"
            )));
        }
        Ok(())
    }

    async fn ensure_record_rpc(
        &self,
        record: &Arc<Mutex<SessionRecord>>,
    ) -> Result<Arc<PiRpcClient>> {
        let mut guard = record.lock().await;
        if let Some(rpc) = &guard.rpc {
            return Ok(Arc::clone(rpc));
        }
        let info = guard.info.clone();
        let rpc = PiRpcClient::spawn(&self.config.pi_bin, Path::new(&info.directory)).await?;
        guard.rpc = Some(Arc::clone(&rpc));
        drop(guard);
        self.forward_session_events(&info, Arc::clone(&rpc), Arc::clone(record));
        Ok(rpc)
    }

    async fn ensure_messages_loaded(&self, record: &Arc<Mutex<SessionRecord>>) -> Result<()> {
        if !record.lock().await.messages.is_empty() {
            return Ok(());
        }
        let rpc = self.ensure_record_rpc(record).await?;
        let raw_messages = rpc.get_messages().await?;
        if raw_messages.is_empty() {
            return Ok(());
        }
        let info = record.lock().await.info.clone();
        let messages = pi_messages_to_opencode(&info, raw_messages);
        let mut record = record.lock().await;
        if record.messages.is_empty() {
            record.messages = messages;
        }
        Ok(())
    }

    fn persist_record(&self, record: &SessionRecord) -> Result<()> {
        self.storage.save_session(&record.info)?;
        Ok(())
    }

    fn forward_session_events(
        &self,
        session: &SessionInfo,
        rpc: Arc<PiRpcClient>,
        record: Arc<Mutex<SessionRecord>>,
    ) {
        let mut rx = rpc.subscribe();
        let tx = self.global_events.clone();
        let directory = session.directory.clone();
        let project = self
            .storage
            .project_id_for_directory(&directory)
            .unwrap_or_else(|_| self.project_id.clone());
        let session_id = session.id.clone();
        let state = self.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(payload) => {
                        let _ = tx.send(json!({
                            "directory": directory.clone(),
                            "project": project.clone(),
                            "workspace": null,
                            "payload": {
                                "type": "pi.rpc.event",
                                "properties": {
                                    "sessionID": session_id,
                                    "event": payload,
                                }
                            }
                        }));
                        state
                            .publish_translated_pi_event(&session_id, &record, &payload)
                            .await;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    async fn publish_translated_pi_event(
        &self,
        session_id: &str,
        record: &Arc<Mutex<SessionRecord>>,
        payload: &Value,
    ) {
        let event_type = payload.get("type").and_then(Value::as_str);
        let mut record = record.lock().await;
        if record.live.assistant.is_none() {
            return;
        }

        match event_type {
            Some("message_start") if is_assistant_message_event(payload) => {
                self.ensure_live_assistant_message(&mut record);
            }
            Some("message_update") => {
                self.ensure_live_assistant_message(&mut record);
                if let Some(event) = payload.get("assistantMessageEvent") {
                    self.publish_assistant_message_event(session_id, &mut record, event);
                }
            }
            Some("tool_execution_start") => {
                self.ensure_live_assistant_message(&mut record);
                self.publish_tool_execution_start(session_id, &mut record, payload);
            }
            Some("tool_execution_update") => {
                self.ensure_live_assistant_message(&mut record);
                self.publish_tool_execution_update(session_id, &mut record, payload);
            }
            Some("tool_execution_end") => {
                self.ensure_live_assistant_message(&mut record);
                self.publish_tool_execution_end(session_id, &mut record, payload);
            }
            _ => {}
        }
    }

    fn ensure_live_assistant_message(&self, record: &mut SessionRecord) {
        let Some(live) = record.live.assistant.as_mut() else {
            return;
        };
        if live.published_message {
            return;
        }
        live.published_message = true;
        let info = assistant_message_pending_with_id(
            &record.info,
            &live.parent_id,
            live.message_id.clone(),
            &live.directory,
        );
        self.publish_for_directory(
            record.info.directory.clone(),
            json!({
                "type": "message.updated",
                "properties": {
                    "sessionID": record.info.id.clone(),
                    "info": info,
                },
            }),
        );
    }

    fn publish_assistant_message_event(
        &self,
        session_id: &str,
        record: &mut SessionRecord,
        event: &Value,
    ) {
        let kind = event.get("type").and_then(Value::as_str);
        match kind {
            Some("text_start") => {
                self.publish_live_text_start(session_id, record, LiveTextKind::Text, event);
            }
            Some("text_delta") => {
                self.publish_live_text_delta(session_id, record, LiveTextKind::Text, event);
            }
            Some("text_end") => {
                self.publish_live_text_end(session_id, record, LiveTextKind::Text, event);
            }
            Some("thinking_start") => {
                self.publish_live_text_start(session_id, record, LiveTextKind::Reasoning, event);
            }
            Some("thinking_delta") => {
                self.publish_live_text_delta(session_id, record, LiveTextKind::Reasoning, event);
            }
            Some("thinking_end") => {
                self.publish_live_text_end(session_id, record, LiveTextKind::Reasoning, event);
            }
            Some("toolcall_end") => {
                self.publish_tool_call_pending(session_id, record, event);
            }
            _ => {}
        }
    }

    fn publish_live_text_start(
        &self,
        session_id: &str,
        record: &mut SessionRecord,
        kind: LiveTextKind,
        event: &Value,
    ) {
        let key = content_key(event);
        let Some(part) = ensure_live_text_part(record, kind, &key) else {
            return;
        };
        self.publish_part_updated(&record.info.directory, session_id, part);
    }

    fn publish_live_text_delta(
        &self,
        session_id: &str,
        record: &mut SessionRecord,
        kind: LiveTextKind,
        event: &Value,
    ) {
        let key = content_key(event);
        let delta = event
            .get("delta")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let Some((part_id, message_id, maybe_started)) =
            append_live_text_delta(record, kind, &key, &delta)
        else {
            return;
        };
        if let Some(started) = maybe_started {
            self.publish_part_updated(&record.info.directory, session_id, started);
        }
        self.publish_for_directory(
            record.info.directory.clone(),
            json!({
                "type": "message.part.delta",
                "properties": {
                    "sessionID": session_id,
                    "messageID": message_id,
                    "partID": part_id,
                    "field": "text",
                    "delta": delta,
                },
            }),
        );
    }

    fn publish_live_text_end(
        &self,
        session_id: &str,
        record: &mut SessionRecord,
        kind: LiveTextKind,
        event: &Value,
    ) {
        let key = content_key(event);
        let content = event
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(part) = finish_live_text_part(record, kind, &key, content) else {
            return;
        };
        self.publish_part_updated(&record.info.directory, session_id, part);
    }

    fn publish_tool_call_pending(
        &self,
        session_id: &str,
        record: &mut SessionRecord,
        event: &Value,
    ) {
        let Some(tool_call) = event.get("toolCall") else {
            return;
        };
        let Some(call_id) = tool_call.get("id").and_then(Value::as_str) else {
            return;
        };
        let tool = tool_call
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let input = tool_call
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let state = json!({
            "status": "pending",
            "input": object_or_empty(input.clone()),
            "raw": input.to_string(),
        });
        if let Some(part) = upsert_live_tool_part(record, call_id, tool, state) {
            self.publish_part_updated(&record.info.directory, session_id, part);
        }
    }

    fn publish_tool_execution_start(
        &self,
        session_id: &str,
        record: &mut SessionRecord,
        payload: &Value,
    ) {
        let Some(call_id) = payload.get("toolCallId").and_then(Value::as_str) else {
            return;
        };
        let tool = payload
            .get("toolName")
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let input = object_or_empty(payload.get("args").cloned().unwrap_or_else(|| json!({})));
        let state = json!({
            "status": "running",
            "input": input,
            "title": tool_title(tool, payload.get("args")),
            "time": { "start": now_ms() },
        });
        if let Some(part) = upsert_live_tool_part(record, call_id, tool, state) {
            self.publish_part_updated(&record.info.directory, session_id, part);
        }
    }

    fn publish_tool_execution_update(
        &self,
        session_id: &str,
        record: &mut SessionRecord,
        payload: &Value,
    ) {
        let Some(call_id) = payload.get("toolCallId").and_then(Value::as_str) else {
            return;
        };
        let tool = payload
            .get("toolName")
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let input = object_or_empty(payload.get("args").cloned().unwrap_or_else(|| json!({})));
        let output = payload
            .get("partialResult")
            .map(extract_tool_output)
            .unwrap_or_default();
        let state = json!({
            "status": "running",
            "input": input,
            "title": tool_title(tool, payload.get("args")),
            "metadata": { "partialOutput": output },
            "time": { "start": live_tool_start(record, call_id).unwrap_or_else(now_ms) },
        });
        if let Some(part) = upsert_live_tool_part(record, call_id, tool, state) {
            self.publish_part_updated(&record.info.directory, session_id, part);
        }
    }

    fn publish_tool_execution_end(
        &self,
        session_id: &str,
        record: &mut SessionRecord,
        payload: &Value,
    ) {
        let Some(call_id) = payload.get("toolCallId").and_then(Value::as_str) else {
            return;
        };
        let tool = payload
            .get("toolName")
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let input = object_or_empty(payload.get("args").cloned().unwrap_or_else(|| json!({})));
        let output = payload
            .get("result")
            .map(extract_tool_output)
            .unwrap_or_default();
        let is_error = payload
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || payload
                .get("result")
                .and_then(|result| result.get("isError"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
        let start = live_tool_start(record, call_id).unwrap_or_else(now_ms);
        let state = if is_error {
            json!({
                "status": "error",
                "input": input,
                "error": output,
                "metadata": {},
                "time": { "start": start, "end": now_ms() },
            })
        } else {
            json!({
                "status": "completed",
                "input": input,
                "output": output,
                "title": tool_title(tool, payload.get("args")),
                "metadata": {},
                "time": { "start": start, "end": now_ms() },
            })
        };
        if let Some(part) = upsert_live_tool_part(record, call_id, tool, state) {
            self.publish_part_updated(&record.info.directory, session_id, part);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum LiveTextKind {
    Text,
    Reasoning,
}

fn start_live_assistant(record: &mut SessionRecord, parent_id: &str, directory: PathBuf) -> String {
    let message_id = ids::message_id_after(parent_id);
    record.live.assistant = Some(LiveAssistant {
        message_id: message_id.clone(),
        parent_id: parent_id.to_string(),
        directory,
        published_message: false,
        parts: Vec::new(),
        text_parts: HashMap::new(),
        reasoning_parts: HashMap::new(),
        tool_parts: HashMap::new(),
    });
    message_id
}

fn is_assistant_message_event(payload: &Value) -> bool {
    payload
        .get("message")
        .and_then(|message| message.get("role"))
        .and_then(Value::as_str)
        == Some("assistant")
}

fn content_key(event: &Value) -> String {
    event
        .get("contentIndex")
        .and_then(Value::as_u64)
        .map_or_else(|| "0".to_string(), |index| index.to_string())
}

fn ensure_live_text_part(
    record: &mut SessionRecord,
    kind: LiveTextKind,
    key: &str,
) -> Option<Value> {
    let (index, _) = ensure_live_text_part_index(record, kind, key)?;
    record
        .live
        .assistant
        .as_ref()
        .map(|live| json!(live.parts[index].clone()))
}

fn append_live_text_delta(
    record: &mut SessionRecord,
    kind: LiveTextKind,
    key: &str,
    delta: &str,
) -> Option<(String, String, Option<Value>)> {
    let (index, created) = ensure_live_text_part_index(record, kind, key)?;
    let live = record.live.assistant.as_mut()?;
    let started = created.then(|| json!(live.parts[index].clone()));
    let text = text_part_mut(&mut live.parts[index])?;
    text.text.push_str(delta);
    Some((text.id.clone(), text.message_id.clone(), started))
}

fn finish_live_text_part(
    record: &mut SessionRecord,
    kind: LiveTextKind,
    key: &str,
    content: &str,
) -> Option<Value> {
    let (index, _) = ensure_live_text_part_index(record, kind, key)?;
    let live = record.live.assistant.as_mut()?;
    let text = text_part_mut(&mut live.parts[index])?;
    text.text = content.to_string();
    let now = now_ms();
    match text.time.as_mut() {
        Some(time) => time.end = Some(now),
        None => {
            text.time = Some(PartTime {
                start: now,
                end: Some(now),
            });
        }
    }
    Some(json!(live.parts[index].clone()))
}

fn ensure_live_text_part_index(
    record: &mut SessionRecord,
    kind: LiveTextKind,
    key: &str,
) -> Option<(usize, bool)> {
    let live = record.live.assistant.as_mut()?;
    let existing = match kind {
        LiveTextKind::Text => live.text_parts.get(key).copied(),
        LiveTextKind::Reasoning => live.reasoning_parts.get(key).copied(),
    };
    if let Some(index) = existing {
        return Some((index, false));
    }

    let now = now_ms();
    let part = TextPart {
        id: ids::part_id(),
        session_id: record.info.id.clone(),
        message_id: live.message_id.clone(),
        text: String::new(),
        time: Some(PartTime {
            start: now,
            end: None,
        }),
        metadata: None,
    };
    let part = match kind {
        LiveTextKind::Text => Part::Text(part),
        LiveTextKind::Reasoning => Part::Reasoning(part),
    };
    live.parts.push(part);
    let index = live.parts.len() - 1;
    match kind {
        LiveTextKind::Text => live.text_parts.insert(key.to_string(), index),
        LiveTextKind::Reasoning => live.reasoning_parts.insert(key.to_string(), index),
    };
    Some((index, true))
}

fn text_part_mut(part: &mut Part) -> Option<&mut TextPart> {
    match part {
        Part::Text(text) | Part::Reasoning(text) => Some(text),
        Part::File(_) | Part::Tool(_) => None,
    }
}

fn upsert_live_tool_part(
    record: &mut SessionRecord,
    call_id: &str,
    tool: &str,
    state: Value,
) -> Option<Value> {
    let live = record.live.assistant.as_mut()?;
    if let Some(index) = live.tool_parts.get(call_id).copied() {
        if let Part::Tool(part) = &mut live.parts[index] {
            part.tool = tool.to_string();
            part.state = state;
        }
        return Some(json!(live.parts[index].clone()));
    }

    let part = Part::Tool(ToolPart {
        id: ids::part_id(),
        session_id: record.info.id.clone(),
        message_id: live.message_id.clone(),
        call_id: call_id.to_string(),
        tool: tool.to_string(),
        state,
    });
    live.parts.push(part);
    let index = live.parts.len() - 1;
    live.tool_parts.insert(call_id.to_string(), index);
    Some(json!(live.parts[index].clone()))
}

fn live_tool_start(record: &SessionRecord, call_id: &str) -> Option<i64> {
    let live = record.live.assistant.as_ref()?;
    let index = live.tool_parts.get(call_id)?;
    let Part::Tool(part) = live.parts.get(*index)? else {
        return None;
    };
    part.state
        .get("time")
        .and_then(|time| time.get("start"))
        .and_then(Value::as_i64)
}

fn object_or_empty(value: Value) -> Value {
    if value.is_object() {
        value
    } else if value.is_null() {
        json!({})
    } else {
        json!({ "value": value })
    }
}

fn tool_title(tool: &str, args: Option<&Value>) -> String {
    let Some(args) = args.and_then(Value::as_object) else {
        return tool.to_string();
    };
    let Some((key, value)) = args.iter().next() else {
        return tool.to_string();
    };
    let value = value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string());
    if value.len() > 80 {
        let truncated = value.chars().take(80).collect::<String>();
        format!("{tool} {key}={truncated}")
    } else {
        format!("{tool} {key}={value}")
    }
}

fn extract_tool_output(value: &Value) -> String {
    value
        .get("content")
        .map(extract_content_text)
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| value.to_string())
}

fn assistant_has_text_part(message: &MessageWithParts) -> bool {
    message
        .parts
        .iter()
        .any(|part| matches!(part, Part::Text(text) if !text.text.is_empty()))
}

fn completed_text_part_with_id(session_id: &str, message_id: &str, text: String) -> Part {
    let now = now_ms();
    Part::Text(TextPart {
        id: ids::part_id(),
        session_id: session_id.to_string(),
        message_id: message_id.to_string(),
        text,
        time: Some(PartTime {
            start: now,
            end: Some(now),
        }),
        metadata: None,
    })
}

struct TextDelta {
    part_id: String,
    message_id: String,
    text: String,
}

fn text_delta(part: &Part) -> Option<TextDelta> {
    let text = match part {
        Part::Text(text) | Part::Reasoning(text) => text,
        Part::File(_) | Part::Tool(_) => return None,
    };

    if text.text.is_empty() || text.time.as_ref().and_then(|time| time.end).is_none() {
        return None;
    }

    Some(TextDelta {
        part_id: text.id.clone(),
        message_id: text.message_id.clone(),
        text: text.text.clone(),
    })
}

fn part_id_of(part: &Part) -> &str {
    match part {
        Part::Text(part) | Part::Reasoning(part) => &part.id,
        Part::File(part) => &part.id,
        Part::Tool(part) => &part.id,
    }
}

fn part_message_id(part: &Part) -> &str {
    match part {
        Part::Text(part) | Part::Reasoning(part) => &part.message_id,
        Part::File(part) => &part.message_id,
        Part::Tool(part) => &part.message_id,
    }
}

fn part_session_id(part: &Part) -> &str {
    match part {
        Part::Text(part) | Part::Reasoning(part) => &part.session_id,
        Part::File(part) => &part.session_id,
        Part::Tool(part) => &part.session_id,
    }
}

fn merge_permission_rules(current: Option<&Vec<Value>>, patch: Vec<Value>) -> Vec<Value> {
    let mut merged = current.cloned().unwrap_or_default();
    for rule in patch {
        if !merged.iter().any(|existing| existing == &rule) {
            merged.push(rule);
        }
    }
    merged
}

fn validate_permission_ruleset(value: &Value) -> Result<Vec<Value>> {
    let rules = value
        .as_array()
        .ok_or_else(|| Error::bad_request("session permission must be an array"))?;
    let mut validated = Vec::with_capacity(rules.len());
    for rule in rules {
        let rule = rule
            .as_object()
            .ok_or_else(|| Error::bad_request("permission rule must be an object"))?;
        for field in ["permission", "pattern", "action"] {
            if rule.get(field).and_then(Value::as_str).is_none() {
                return Err(Error::bad_request(format!(
                    "permission rule field must be a string: {field}"
                )));
            }
        }
        let action = rule
            .get("action")
            .and_then(Value::as_str)
            .expect("validated action");
        if !matches!(action, "allow" | "deny" | "ask") {
            return Err(Error::bad_request(format!(
                "invalid permission action: {action}"
            )));
        }
        validated.push(Value::Object(rule.clone()));
    }
    Ok(validated)
}

fn validate_todo(todo: &Value) -> Result<()> {
    for field in ["content", "status", "priority"] {
        if todo.get(field).and_then(Value::as_str).is_none() {
            return Err(Error::bad_request(format!(
                "todo field must be a string: {field}"
            )));
        }
    }
    Ok(())
}

fn pi_messages_to_opencode(
    session: &SessionInfo,
    raw_messages: Vec<Value>,
) -> Vec<MessageWithParts> {
    let base_time = now_ms().saturating_sub(raw_messages.len() as i64);
    let mut messages = Vec::new();
    let mut last_user_id: Option<String> = None;
    let directory = PathBuf::from(&session.directory);
    for (index, raw) in raw_messages.into_iter().enumerate() {
        let role = raw.get("role").and_then(Value::as_str).unwrap_or_default();
        let text = extract_content_text(raw.get("content").unwrap_or(&Value::Null));
        if text.is_empty() {
            continue;
        }
        let created = pi_message_time(&raw).unwrap_or(base_time + index as i64);
        let id = synthetic_message_id(created, index);
        match role {
            "user" => {
                last_user_id = Some(id.clone());
                messages.push(MessageWithParts {
                    info: MessageInfo::User(UserMessageInfo {
                        id: id.clone(),
                        session_id: session.id.clone(),
                        time: CreatedTime { created },
                        agent: session.agent.clone().unwrap_or_else(|| "build".to_string()),
                        model: session.model.clone().unwrap_or_default(),
                        system: None,
                        tools: None,
                    }),
                    parts: vec![synthetic_text_part(&session.id, &id, created, text, false)],
                });
            }
            "assistant" => {
                let parent_id = last_user_id
                    .clone()
                    .or_else(|| messages.last().map(|message| message.info.id().to_string()))
                    .unwrap_or_else(|| synthetic_message_id(created.saturating_sub(1), index));
                let model = session.model.clone().unwrap_or_default();
                messages.push(MessageWithParts {
                    info: MessageInfo::Assistant(AssistantMessageInfo {
                        id: id.clone(),
                        session_id: session.id.clone(),
                        time: CompletedTime {
                            created,
                            completed: Some(created),
                        },
                        parent_id,
                        model_id: model.model_id,
                        provider_id: model.provider_id,
                        mode: session.agent.clone().unwrap_or_else(|| "build".to_string()),
                        agent: session.agent.clone().unwrap_or_else(|| "build".to_string()),
                        path: MessagePath {
                            cwd: directory.display().to_string(),
                            root: directory.display().to_string(),
                        },
                        cost: 0.0,
                        tokens: Tokens::default(),
                        finish: Some(
                            raw.get("stopReason")
                                .and_then(Value::as_str)
                                .unwrap_or("stop")
                                .to_string(),
                        ),
                        error: None,
                    }),
                    parts: vec![synthetic_text_part(&session.id, &id, created, text, true)],
                });
            }
            _ => {}
        }
    }
    messages
}

fn synthetic_message_id(created: i64, index: usize) -> String {
    let sequence = (created.max(0) as u64)
        .saturating_mul(4096)
        .saturating_add(index as u64);
    format!("msg_{:012x}{:014}", sequence & 0x0000_ffff_ffff_ffff, 0)
}

fn synthetic_part_id(created: i64, index: usize) -> String {
    let sequence = (created.max(0) as u64)
        .saturating_mul(4096)
        .saturating_add(index as u64);
    format!("prt_{:012x}{:014}", sequence & 0x0000_ffff_ffff_ffff, 0)
}

fn synthetic_text_part(
    session_id: &str,
    message_id: &str,
    created: i64,
    text: String,
    completed: bool,
) -> Part {
    Part::Text(TextPart {
        id: synthetic_part_id(created, 0),
        session_id: session_id.to_string(),
        message_id: message_id.to_string(),
        text,
        time: Some(PartTime {
            start: created,
            end: completed.then_some(created),
        }),
        metadata: None,
    })
}

fn pi_message_time(raw: &Value) -> Option<i64> {
    let value = raw.get("timestamp").and_then(Value::as_i64)?;
    if value <= 0 {
        return None;
    }
    Some(if value > 1_000_000_000_000 {
        value
    } else {
        value.saturating_mul(1000)
    })
}

fn permission_response(payload: &Value) -> Result<String> {
    let response = payload
        .get("response")
        .or_else(|| payload.get("reply"))
        .and_then(Value::as_str)
        .ok_or_else(|| Error::bad_request("missing permission response"))?;
    match response {
        "once" | "always" | "reject" => Ok(response.to_string()),
        _ => Err(Error::bad_request(format!(
            "invalid permission response: {response}"
        ))),
    }
}

fn always_permission_rules(request: &Value) -> Vec<Value> {
    let permission = request
        .get("permission")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    request
        .get("always")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|pattern| {
            json!({
                "permission": permission,
                "pattern": pattern,
                "action": "allow",
            })
        })
        .collect()
}

fn forked_title(title: &str) -> String {
    let Some(prefix) = title.strip_suffix(')') else {
        return format!("{title} (fork #1)");
    };
    let Some((base, number)) = prefix.rsplit_once(" (fork #") else {
        return format!("{title} (fork #1)");
    };
    number
        .parse::<u64>()
        .map(|number| format!("{base} (fork #{})", number + 1))
        .unwrap_or_else(|_| format!("{title} (fork #1)"))
}

fn fork_message(
    message: &MessageWithParts,
    new_session_id: &str,
    id_map: &HashMap<String, String>,
) -> MessageWithParts {
    let new_message_id = ids::message_id();
    let mut info = message.info.clone();
    match &mut info {
        MessageInfo::User(info) => {
            info.id = new_message_id.clone();
            info.session_id = new_session_id.to_string();
        }
        MessageInfo::Assistant(info) => {
            info.id = new_message_id.clone();
            info.session_id = new_session_id.to_string();
            if let Some(parent_id) = id_map.get(&info.parent_id) {
                info.parent_id = parent_id.clone();
            }
        }
    }
    let parts = message
        .parts
        .iter()
        .cloned()
        .map(|mut part| {
            match &mut part {
                Part::Text(part) | Part::Reasoning(part) => {
                    part.id = ids::part_id();
                    part.session_id = new_session_id.to_string();
                    part.message_id = new_message_id.clone();
                }
                Part::File(part) => {
                    part.id = ids::part_id();
                    part.session_id = new_session_id.to_string();
                    part.message_id = new_message_id.clone();
                }
                Part::Tool(part) => {
                    part.id = ids::part_id();
                    part.session_id = new_session_id.to_string();
                    part.message_id = new_message_id.clone();
                }
            }
            part
        })
        .collect();
    MessageWithParts { info, parts }
}

fn started_part(part: &Part) -> Value {
    let mut value = json!(part);
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    object.insert("text".to_string(), Value::String(String::new()));
    if let Some(time) = object.get_mut("time").and_then(Value::as_object_mut) {
        time.remove("end");
    }
    value
}

pub fn app(config: ServerConfig) -> Router {
    app_with_state(AppState::new(config))
}

pub fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route("/doc", get(doc))
        .route("/global/health", get(global_health))
        .route("/global/event", get(global_event))
        .route("/event", get(instance_event))
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
        .route("/session/:session_id/children", get(session_children))
        .route("/session/:session_id/todo", get(session_todo))
        .route("/session/:session_id/diff", get(session_diff))
        .route(
            "/session/:session_id/message",
            get(session_messages_paged).post(prompt_session),
        )
        .route(
            "/session/:session_id/message/:message_id",
            get(session_message).delete(delete_session_message),
        )
        .route(
            "/session/:session_id/message/:message_id/part/:part_id",
            delete(delete_message_part).patch(update_message_part),
        )
        .route("/session/:session_id/fork", post(fork_session))
        .route("/session/:session_id/abort", post(abort_session))
        .route(
            "/session/:session_id/share",
            post(share_session).delete(unshare_session),
        )
        .route("/session/:session_id/init", post(init_session))
        .route("/session/:session_id/summarize", post(summarize_session))
        .route(
            "/session/:session_id/prompt_async",
            post(prompt_session_async),
        )
        .route("/session/:session_id/command", post(command_session))
        .route("/session/:session_id/shell", post(shell_session))
        .route("/session/:session_id/revert", post(revert_session))
        .route("/session/:session_id/unrevert", post(unrevert_session))
        .route(
            "/session/:session_id/permissions/:permission_id",
            post(session_permission_reply),
        )
        .route("/api/session", get(v2_sessions))
        .route("/api/session/:session_id/message", get(v2_session_messages))
        .route("/api/session/:session_id/context", get(v2_session_context))
        .route("/api/session/:session_id/prompt", post(v2_prompt_session))
        .route("/api/session/:session_id/compact", post(v2_compact_session))
        .route("/api/session/:session_id/wait", post(v2_wait_session))
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
        .route("/permission", get(list_permissions))
        .route("/permission/:request_id/reply", post(permission_reply))
        .route("/question", get(empty_array))
        .route("/question/:request_id/reply", post(ok_true))
        .route("/question/:request_id/reject", post(ok_true))
        .route("/project", get(project_list))
        .route("/project/current", get(project_current))
        .route("/project/git/init", post(project_git_init))
        .route("/project/:project_id", patch(update_project))
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
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    Sse::new(event_stream(state, EventStreamShape::Global)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn instance_event(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    Sse::new(event_stream(state, EventStreamShape::Instance)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

fn event_stream(
    state: AppState,
    shape: EventStreamShape,
) -> impl Stream<Item = std::result::Result<Event, Infallible>> {
    let connected = match shape {
        EventStreamShape::Instance => json!({
            "type": "server.connected",
            "properties": {},
        }),
        EventStreamShape::Global => json!({
            "payload": {
                "type": "server.connected",
                "properties": {},
            },
        }),
    };

    async_stream::stream! {
        yield Ok(Event::default().data(connected.to_string()));

        let mut rx = state.global_events.subscribe();
        loop {
            match rx.recv().await {
                Ok(value) => {
                    let value = match shape {
                        EventStreamShape::Instance => instance_event_payload(value),
                        EventStreamShape::Global => value,
                    };
                    yield Ok(Event::default().data(value.to_string()));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}

fn instance_event_payload(value: Value) -> Value {
    if let Some(payload) = value.get("payload") {
        payload.clone()
    } else {
        value
    }
}

fn request_directory_header(headers: &HeaderMap) -> Option<PathBuf> {
    headers
        .get("x-opencode-directory")
        .and_then(|value| value.to_str().ok())
        .and_then(percent_decode)
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
}

fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = *bytes.get(index + 1)?;
            let low = *bytes.get(index + 2)?;
            decoded.push(hex_value(high)? << 4 | hex_value(low)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

async fn create_session(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<SessionInfo>> {
    let mut payload = parse_optional_json::<CreateSessionPayload>(&body)?;
    if let Some(workspace_id) = query
        .get("workspace")
        .filter(|value| !value.trim().is_empty())
    {
        let payload = payload.get_or_insert(CreateSessionPayload {
            parent_id: None,
            title: None,
            agent: None,
            model: None,
            permission: None,
            workspace_id: None,
        });
        payload
            .workspace_id
            .get_or_insert_with(|| workspace_id.clone());
    }
    let directory = query
        .get("directory")
        .filter(|directory| !directory.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| request_directory_header(&headers));
    state.create_session(payload, directory).await.map(Json)
}

#[derive(Debug, Default, serde::Deserialize)]
struct ListSessionsQuery {
    directory: Option<String>,
    workspace: Option<String>,
    scope: Option<String>,
    path: Option<String>,
    roots: Option<String>,
    start: Option<i64>,
    search: Option<String>,
    limit: Option<usize>,
}

async fn list_sessions(
    State(state): State<AppState>,
    Query(query): Query<ListSessionsQuery>,
) -> Json<Vec<SessionInfo>> {
    let sessions = state.sessions.read().await;
    let mut items = Vec::with_capacity(sessions.len());
    for record in sessions.values() {
        let info = record.lock().await.info.clone();
        if query.scope.as_deref() == Some("project") {
            let project_id = query
                .directory
                .as_deref()
                .and_then(|directory| state.storage.project_id_for_directory(directory).ok())
                .unwrap_or_else(|| state.project_id.clone());
            if info.project_id != project_id {
                continue;
            }
        } else if let Some(directory) = query.directory.as_deref()
            && path_to_string(PathBuf::from(&info.directory))
                != path_to_string(PathBuf::from(directory))
        {
            continue;
        }
        if let Some(workspace) = query.workspace.as_deref()
            && info.workspace_id.as_deref() != Some(workspace)
        {
            continue;
        }
        if let Some(path) = query.path.as_deref()
            && !path.is_empty()
        {
            let matches_path = info.path.as_deref() == Some(path)
                || info
                    .path
                    .as_deref()
                    .is_some_and(|session_path| session_path.starts_with(&format!("{path}/")))
                || (info.path.is_none()
                    && query.directory.as_deref().is_some_and(|directory| {
                        path_to_string(PathBuf::from(&info.directory))
                            == path_to_string(PathBuf::from(directory))
                    }));
            if !matches_path {
                continue;
            }
        }
        if query.roots.as_deref().is_some_and(|roots| roots == "true") && info.parent_id.is_some() {
            continue;
        }
        if let Some(start) = query.start
            && info.time.updated < start
        {
            continue;
        }
        if let Some(search) = query.search.as_deref()
            && !info.title.to_lowercase().contains(&search.to_lowercase())
        {
            continue;
        }
        items.push(info);
    }
    items.sort_by(|a, b| b.time.updated.cmp(&a.time.updated));
    items.truncate(query.limit.unwrap_or(100));
    Json(items)
}

async fn experimental_sessions(State(state): State<AppState>) -> Json<Vec<Value>> {
    let sessions = state.sessions.read().await;
    let mut sessions = futures::future::join_all(
        sessions
            .values()
            .map(|record| async { record.lock().await.info.clone() }),
    )
    .await;
    sessions.sort_by(|a, b| b.time.updated.cmp(&a.time.updated));
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

async fn session_children(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<Vec<SessionInfo>>> {
    state.get_session(&session_id).await?;
    let sessions = state.sessions.read().await;
    let mut children = Vec::new();
    for record in sessions.values() {
        let record = record.lock().await;
        if record.info.parent_id.as_deref() == Some(session_id.as_str()) {
            children.push(record.info.clone());
        }
    }
    children.sort_by(|a, b| b.time.updated.cmp(&a.time.updated));
    Ok(Json(children))
}

#[derive(Debug, Default, serde::Deserialize)]
struct V2SessionsQuery {
    directory: Option<String>,
    workspace: Option<String>,
    limit: Option<usize>,
    order: Option<String>,
    path: Option<String>,
    roots: Option<String>,
    start: Option<i64>,
    search: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct V2Cursor {
    id: String,
    time: i64,
    order: String,
    direction: String,
    #[serde(rename = "workspaceID", skip_serializing_if = "Option::is_none")]
    workspace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    directory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    roots: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    search: Option<String>,
}

async fn v2_sessions(
    State(state): State<AppState>,
    Query(query): Query<V2SessionsQuery>,
) -> Result<Json<Value>> {
    let limit = validate_v2_limit(query.limit, 50)?;
    let decoded = decode_v2_session_cursor(&query)?;
    let order = decoded
        .as_ref()
        .map(|cursor| cursor.order.as_str())
        .or(query.order.as_deref())
        .unwrap_or("desc");
    validate_v2_order(order)?;
    let filters = V2SessionFilters::from_query(&query, decoded.as_ref())?;

    let mut sessions = filtered_sessions(&state, &filters).await;
    sort_sessions_v2(&mut sessions, order);
    if let Some(cursor) = decoded.as_ref() {
        sessions = page_after_session_cursor(sessions, cursor, order, limit);
    } else {
        sessions.truncate(limit);
    }

    let previous = sessions
        .first()
        .map(|session| encode_v2_session_cursor(session, order, "previous", &filters))
        .transpose()?;
    let next = sessions
        .last()
        .map(|session| encode_v2_session_cursor(session, order, "next", &filters))
        .transpose()?;
    Ok(Json(json!({
        "items": sessions,
        "cursor": {
            "previous": previous,
            "next": next,
        },
    })))
}

#[derive(Debug, Default, serde::Deserialize)]
struct V2MessagesQuery {
    limit: Option<usize>,
    order: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct V2MessageCursor {
    id: String,
    time: i64,
    order: String,
    direction: String,
}

#[derive(Debug, Clone)]
struct V2SessionFilters {
    directory: Option<String>,
    path: Option<String>,
    workspace_id: Option<String>,
    roots: Option<bool>,
    start: Option<i64>,
    search: Option<String>,
}

impl V2SessionFilters {
    fn from_query(query: &V2SessionsQuery, cursor: Option<&V2Cursor>) -> Result<Self> {
        if let Some(cursor) = cursor {
            if query.directory.as_deref() != cursor.directory.as_deref()
                && query.directory.is_some()
            {
                return Err(Error::bad_request(
                    "cursor does not match requested directory",
                ));
            }
            if query.workspace.as_deref() != cursor.workspace_id.as_deref()
                && query.workspace.is_some()
            {
                return Err(Error::bad_request(
                    "cursor does not match requested workspace",
                ));
            }
            return Ok(Self {
                directory: cursor.directory.clone(),
                path: cursor.path.clone(),
                workspace_id: cursor.workspace_id.clone(),
                roots: cursor.roots,
                start: cursor.start,
                search: cursor.search.clone(),
            });
        }

        Ok(Self {
            directory: query.directory.clone(),
            path: query.path.clone(),
            workspace_id: query.workspace.clone(),
            roots: query.roots.as_deref().map(parse_query_bool).transpose()?,
            start: query.start,
            search: query.search.clone(),
        })
    }
}

fn decode_v2_session_cursor(query: &V2SessionsQuery) -> Result<Option<V2Cursor>> {
    let Some(cursor) = query.cursor.as_deref() else {
        return Ok(None);
    };
    if query.order.is_some()
        || query.path.is_some()
        || query.roots.is_some()
        || query.start.is_some()
        || query.search.is_some()
    {
        return Err(Error::bad_request(
            "cursor cannot be combined with order or filters",
        ));
    }
    let data = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| Error::bad_request("invalid cursor"))?;
    let cursor: V2Cursor =
        serde_json::from_slice(&data).map_err(|_| Error::bad_request("invalid cursor"))?;
    validate_v2_order(&cursor.order)?;
    validate_v2_direction(&cursor.direction)?;
    Ok(Some(cursor))
}

fn decode_v2_message_cursor(query: &V2MessagesQuery) -> Result<Option<V2MessageCursor>> {
    let Some(cursor) = query.cursor.as_deref() else {
        return Ok(None);
    };
    if query.order.is_some() {
        return Err(Error::bad_request("cursor cannot be combined with order"));
    }
    let data = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| Error::bad_request("invalid cursor"))?;
    let cursor: V2MessageCursor =
        serde_json::from_slice(&data).map_err(|_| Error::bad_request("invalid cursor"))?;
    validate_v2_order(&cursor.order)?;
    validate_v2_direction(&cursor.direction)?;
    Ok(Some(cursor))
}

fn validate_v2_limit(limit: Option<usize>, default: usize) -> Result<usize> {
    let limit = limit.unwrap_or(default);
    if !(1..=200).contains(&limit) {
        return Err(Error::bad_request("limit must be between 1 and 200"));
    }
    Ok(limit)
}

fn validate_v2_order(order: &str) -> Result<()> {
    match order {
        "asc" | "desc" => Ok(()),
        _ => Err(Error::bad_request("order must be asc or desc")),
    }
}

fn validate_v2_direction(direction: &str) -> Result<()> {
    match direction {
        "previous" | "next" => Ok(()),
        _ => Err(Error::bad_request(
            "cursor direction must be previous or next",
        )),
    }
}

fn parse_query_bool(value: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(Error::bad_request("boolean query must be true or false")),
    }
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::bad_request(format!("missing or invalid string field: {field}")))
}

async fn filtered_sessions(state: &AppState, filters: &V2SessionFilters) -> Vec<SessionInfo> {
    let sessions = state.sessions.read().await;
    let mut items = Vec::with_capacity(sessions.len());
    for record in sessions.values() {
        let info = record.lock().await.info.clone();
        if let Some(directory) = filters.directory.as_deref()
            && path_to_string(PathBuf::from(&info.directory))
                != path_to_string(PathBuf::from(directory))
        {
            continue;
        }
        if let Some(path) = filters.path.as_deref()
            && !path.is_empty()
        {
            let matches_path = info.path.as_deref() == Some(path)
                || info
                    .path
                    .as_deref()
                    .is_some_and(|session_path| session_path.starts_with(&format!("{path}/")));
            if !matches_path {
                continue;
            }
        }
        if let Some(workspace_id) = filters.workspace_id.as_deref()
            && info.workspace_id.as_deref() != Some(workspace_id)
        {
            continue;
        }
        if filters.roots == Some(true) && info.parent_id.is_some() {
            continue;
        }
        if let Some(start) = filters.start
            && info.time.updated < start
        {
            continue;
        }
        if let Some(search) = filters.search.as_deref()
            && !info.title.to_lowercase().contains(&search.to_lowercase())
        {
            continue;
        }
        items.push(info);
    }
    items
}

fn sort_sessions_v2(sessions: &mut [SessionInfo], order: &str) {
    sessions.sort_by(|a, b| {
        let ordering = a
            .time
            .updated
            .cmp(&b.time.updated)
            .then_with(|| a.id.cmp(&b.id));
        if order == "asc" {
            ordering
        } else {
            ordering.reverse()
        }
    });
}

fn sort_messages_v2(messages: &mut [MessageWithParts], order: &str) {
    messages.sort_by(|a, b| {
        let ordering = message_created_time(a)
            .cmp(&message_created_time(b))
            .then_with(|| a.info.id().cmp(b.info.id()));
        if order == "asc" {
            ordering
        } else {
            ordering.reverse()
        }
    });
}

fn page_after_session_cursor(
    sessions: Vec<SessionInfo>,
    cursor: &V2Cursor,
    order: &str,
    limit: usize,
) -> Vec<SessionInfo> {
    if cursor.direction == "next" {
        return sessions
            .into_iter()
            .filter(|session| session_after_cursor(session, cursor, order))
            .take(limit)
            .collect();
    }
    let previous = sessions
        .into_iter()
        .filter(|session| session_before_cursor(session, cursor, order))
        .collect::<Vec<_>>();
    let skip = previous.len().saturating_sub(limit);
    previous.into_iter().skip(skip).collect()
}

fn page_after_message_cursor(
    messages: Vec<MessageWithParts>,
    cursor: &V2MessageCursor,
    order: &str,
    limit: usize,
) -> Vec<MessageWithParts> {
    if cursor.direction == "next" {
        return messages
            .into_iter()
            .filter(|message| message_after_cursor(message, cursor, order))
            .take(limit)
            .collect();
    }
    let previous = messages
        .into_iter()
        .filter(|message| message_before_cursor(message, cursor, order))
        .collect::<Vec<_>>();
    let skip = previous.len().saturating_sub(limit);
    previous.into_iter().skip(skip).collect()
}

fn session_after_cursor(session: &SessionInfo, cursor: &V2Cursor, order: &str) -> bool {
    if order == "asc" {
        session.time.updated > cursor.time
            || (session.time.updated == cursor.time && session.id.as_str() > cursor.id.as_str())
    } else {
        session.time.updated < cursor.time
            || (session.time.updated == cursor.time && session.id.as_str() < cursor.id.as_str())
    }
}

fn session_before_cursor(session: &SessionInfo, cursor: &V2Cursor, order: &str) -> bool {
    if order == "asc" {
        session.time.updated < cursor.time
            || (session.time.updated == cursor.time && session.id.as_str() < cursor.id.as_str())
    } else {
        session.time.updated > cursor.time
            || (session.time.updated == cursor.time && session.id.as_str() > cursor.id.as_str())
    }
}

fn message_after_cursor(message: &MessageWithParts, cursor: &V2MessageCursor, order: &str) -> bool {
    let time = message_created_time(message);
    if order == "asc" {
        time > cursor.time || (time == cursor.time && message.info.id() > cursor.id.as_str())
    } else {
        time < cursor.time || (time == cursor.time && message.info.id() < cursor.id.as_str())
    }
}

fn message_before_cursor(
    message: &MessageWithParts,
    cursor: &V2MessageCursor,
    order: &str,
) -> bool {
    let time = message_created_time(message);
    if order == "asc" {
        time < cursor.time || (time == cursor.time && message.info.id() < cursor.id.as_str())
    } else {
        time > cursor.time || (time == cursor.time && message.info.id() > cursor.id.as_str())
    }
}

fn encode_v2_session_cursor(
    session: &SessionInfo,
    order: &str,
    direction: &str,
    filters: &V2SessionFilters,
) -> Result<String> {
    let cursor = V2Cursor {
        id: session.id.clone(),
        time: session.time.updated,
        order: order.to_string(),
        direction: direction.to_string(),
        workspace_id: filters.workspace_id.clone(),
        directory: filters.directory.clone(),
        path: filters.path.clone(),
        roots: filters.roots,
        start: filters.start,
        search: filters.search.clone(),
    };
    encode_base64_json(&cursor)
}

fn encode_v2_message_cursor(
    message: &MessageWithParts,
    order: &str,
    direction: &str,
) -> Result<String> {
    encode_base64_json(&V2MessageCursor {
        id: message.info.id().to_string(),
        time: message_created_time(message),
        order: order.to_string(),
        direction: direction.to_string(),
    })
}

fn encode_base64_json(value: &impl serde::Serialize) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(value)?))
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
    if !patch.is_object() {
        return Err(Error::bad_request(
            "session update payload must be an object",
        ));
    }
    let record = state.get_session(&session_id).await?;
    state.ensure_messages_loaded(&record).await?;
    let mut record = record.lock().await;
    if let Some(title) = patch.get("title") {
        let title = title
            .as_str()
            .ok_or_else(|| Error::bad_request("session title must be a string"))?;
        record.info.title = title.to_string();
        record.info.slug = ids::slug(title);
    }
    if let Some(time) = patch.get("time") {
        let time = time
            .as_object()
            .ok_or_else(|| Error::bad_request("session time must be an object"))?;
        if let Some(archived) = time.get("archived") {
            let archived = archived
                .as_i64()
                .ok_or_else(|| Error::bad_request("session archived time must be a number"))?;
            record.info.time.archived = Some(archived);
        }
    }
    if let Some(permission) = patch.get("permission") {
        let patch = validate_permission_ruleset(permission)?;
        record.info.permission = Some(merge_permission_rules(
            record.info.permission.as_ref(),
            patch,
        ));
    }
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_session_updated(&record.info);
    Ok(Json(record.info.clone()))
}

async fn remove_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<bool>> {
    let parent_by_id = {
        let sessions = state.sessions.read().await;
        let mut parent_by_id = HashMap::with_capacity(sessions.len());
        for (id, record) in sessions.iter() {
            parent_by_id.insert(id.clone(), record.lock().await.info.parent_id.clone());
        }
        parent_by_id
    };
    if !parent_by_id.contains_key(&session_id) {
        return Err(Error::not_found(format!("session not found: {session_id}")));
    }
    let mut stack = vec![session_id.clone()];
    let mut ids = Vec::new();
    while let Some(id) = stack.pop() {
        ids.push(id.clone());
        for (child_id, parent_id) in &parent_by_id {
            if parent_id.as_deref() == Some(id.as_str()) {
                stack.push(child_id.clone());
            }
        }
    }
    let removed = {
        let mut sessions = state.sessions.write().await;
        ids.into_iter()
            .filter_map(|id| sessions.remove(&id).map(|record| (id, record)))
            .collect::<Vec<_>>()
    };
    let mut statuses = state.statuses.write().await;
    for (id, _) in &removed {
        statuses.remove(id);
    }
    drop(statuses);

    for (id, record) in removed.into_iter().rev() {
        let record = record.lock().await;
        let info = record.info.clone();
        if let Some(rpc) = &record.rpc {
            rpc.shutdown().await;
        }
        state.storage.delete_session(&id)?;
        state.publish_session_deleted(&info);
    }
    Ok(Json(true))
}

async fn session_status(State(state): State<AppState>) -> Json<Value> {
    let sessions = state.sessions.read().await;
    let statuses = state.statuses.read().await;
    let mut map = serde_json::Map::new();
    for id in sessions.keys() {
        map.insert(
            id.clone(),
            statuses
                .get(id)
                .cloned()
                .unwrap_or_else(|| json!({ "type": "idle" })),
        );
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
    let payload = parse_optional_value(&body)?
        .ok_or_else(|| Error::bad_request("missing v2 prompt payload"))?;
    if payload.get("prompt").is_none() {
        return Err(Error::bad_request("missing v2 prompt field"));
    }
    state.get_session(&session_id).await?;
    Err(Error::service_unavailable(
        "V2 session prompt is not available yet",
    ))
}

async fn command_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<MessageWithParts>> {
    let value = parse_optional_value(&body)?
        .ok_or_else(|| Error::bad_request("missing command payload"))?;
    let command = required_string(&value, "command")?;
    let arguments = required_string(&value, "arguments")?;
    let text = if arguments.is_empty() {
        format!("/{command}")
    } else {
        format!("/{command} {arguments}")
    };
    let mut payload = value
        .as_object()
        .cloned()
        .ok_or_else(|| Error::bad_request("command payload must be an object"))?;
    payload.insert(
        "parts".to_string(),
        json!([{ "type": "text", "text": text }]),
    );
    if payload.get("model").is_some_and(Value::is_string) {
        payload.remove("model");
    }
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
    let mut value =
        parse_optional_value(&body)?.ok_or_else(|| Error::bad_request("missing shell payload"))?;
    required_string(&value, "agent")?;
    let command = required_string(&value, "command")?.to_string();
    if !value.is_object() {
        return Err(Error::bad_request("shell payload must be an object"));
    }
    state.get_session(&session_id).await?;
    state.ensure_not_busy(&session_id).await?;
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
    if text.trim().is_empty() {
        return Err(Error::bad_request("prompt parts must include text"));
    }
    let record = state.get_session(&session_id).await?;
    let rpc = state.ensure_record_rpc(&record).await?;
    state.ensure_messages_loaded(&record).await?;
    let (rpc, user, directory, assistant_id) = {
        let mut record = record.lock().await;
        let user = user_message(&record.info, &payload, text.clone());
        let directory = PathBuf::from(record.info.directory.clone());
        let assistant_id = start_live_assistant(&mut record, user.info.id(), directory.clone());
        record.messages.push(user.clone());
        record.info.touch();
        state.persist_record(&record)?;
        state.publish_message(&record.info.directory, &user);
        (rpc, user, directory, assistant_id)
    };

    state
        .set_session_status(&session_id, json!({ "type": "busy" }))
        .await;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        match rpc.prompt(&text).await {
            Ok(event) => {
                if let Err(err) = record_assistant_from_event(
                    state.clone(),
                    session_id.clone(),
                    user.info.id().to_string(),
                    assistant_id,
                    directory,
                    event,
                )
                .await
                {
                    tracing::warn!(%err, "failed to record async prompt completion");
                }
            }
            Err(err) => {
                tracing::warn!(%err, "background pi prompt failed");
                if let Ok(record) = state.get_session(&session_id).await {
                    record.lock().await.live.assistant = None;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        state
            .set_session_status(&session_id, json!({ "type": "idle" }))
            .await;
    });
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
    let rpc = state.ensure_record_rpc(&record).await?;
    state.ensure_messages_loaded(&record).await?;
    let (rpc, user, directory, assistant_id) = {
        let mut record = record.lock().await;
        let user = user_message(&record.info, &payload, text.clone());
        let directory = PathBuf::from(record.info.directory.clone());
        let assistant_id = start_live_assistant(&mut record, user.info.id(), directory.clone());
        record.messages.push(user.clone());
        record.info.touch();
        state.persist_record(&record)?;
        state.publish_message(&record.info.directory, &user);
        (rpc, user, directory, assistant_id)
    };

    if payload.no_reply {
        record.lock().await.live.assistant = None;
        return Ok(user);
    }

    state
        .set_session_status(&session_id, json!({ "type": "busy" }))
        .await;
    let event = match rpc.prompt(&text).await {
        Ok(event) => event,
        Err(err) => {
            record.lock().await.live.assistant = None;
            state
                .set_session_status(&session_id, json!({ "type": "idle" }))
                .await;
            return Err(err);
        }
    };
    let assistant = record_assistant_from_event(
        state.clone(),
        session_id.clone(),
        user.info.id().to_string(),
        assistant_id,
        directory,
        event,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    state
        .set_session_status(&session_id, json!({ "type": "idle" }))
        .await;
    Ok(assistant)
}

async fn record_assistant_from_event(
    state: AppState,
    session_id: String,
    parent_id: String,
    assistant_id: String,
    directory: PathBuf,
    event: Value,
) -> Result<MessageWithParts> {
    let record = state.get_session(&session_id).await?;
    let mut record = record.lock().await;
    let mut assistant =
        assistant_from_agent_end(&record.info, &parent_id, &assistant_id, &directory, &event)
            .unwrap_or_else(|| {
                assistant_message_with_id(
                    &record.info,
                    &parent_id,
                    assistant_id.clone(),
                    "",
                    &directory,
                )
            });
    let live = record.live.assistant.take();
    if let Some(live) = live.filter(|live| live.message_id == assistant_id) {
        let final_text = assistant
            .parts
            .iter()
            .find_map(|part| match part {
                Part::Text(text) => Some(text.text.clone()),
                Part::Reasoning(_) | Part::File(_) | Part::Tool(_) => None,
            })
            .unwrap_or_default();
        assistant.parts = live.parts;
        if !final_text.is_empty() && !assistant_has_text_part(&assistant) {
            assistant.parts.push(completed_text_part_with_id(
                &record.info.id,
                &assistant_id,
                final_text,
            ));
        }
        record.messages.push(assistant.clone());
        record.info.touch();
        state.persist_record(&record)?;
        state.publish_message_snapshot(&record.info.directory, &assistant);
        return Ok(assistant);
    }
    record.messages.push(assistant.clone());
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_message(&record.info.directory, &assistant);
    Ok(assistant)
}

#[derive(Debug, Default, serde::Deserialize)]
struct MessagesQuery {
    limit: Option<usize>,
    before: Option<String>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MessageCursor {
    id: String,
    time: i64,
}

fn encode_message_cursor(message: &MessageWithParts) -> Result<String> {
    let cursor = MessageCursor {
        id: message.info.id().to_string(),
        time: message_created_time(message),
    };
    let data = serde_json::to_vec(&cursor)?;
    Ok(URL_SAFE_NO_PAD.encode(data))
}

fn decode_message_cursor(value: &str) -> Result<MessageCursor> {
    let data = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|err| Error::bad_request(err.to_string()))?;
    let cursor: MessageCursor = serde_json::from_slice(&data)?;
    if cursor.time < 0 || cursor.id.is_empty() {
        return Err(Error::bad_request("invalid before cursor"));
    }
    Ok(cursor)
}

fn message_created_time(message: &MessageWithParts) -> i64 {
    match &message.info {
        MessageInfo::User(info) => info.time.created,
        MessageInfo::Assistant(info) => info.time.created,
    }
}

async fn session_messages_paged(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Query(query): Query<MessagesQuery>,
) -> Result<Response> {
    if query.before.is_some() && query.limit.is_none() {
        return Err(Error::bad_request("before requires limit"));
    }
    let mut messages = session_messages_all(&state, &session_id).await?;
    if let Some(before) = query.before.as_deref() {
        let cursor = decode_message_cursor(before)?;
        let Some(index) = messages.iter().position(|message| {
            message.info.id() == cursor.id && message_created_time(message) == cursor.time
        }) else {
            return Err(Error::bad_request("invalid before cursor"));
        };
        messages.truncate(index);
    }

    let mut next_cursor = None;
    if let Some(limit) = query.limit
        && limit > 0
        && messages.len() > limit
    {
        next_cursor = messages
            .get(messages.len() - limit)
            .map(encode_message_cursor)
            .transpose()?;
        messages = messages.split_off(messages.len() - limit);
    }

    let mut response = Json(messages).into_response();
    if let Some(cursor) = next_cursor {
        let headers = response.headers_mut();
        let limit = query.limit.unwrap_or_default();
        let link =
            format!("</session/{session_id}/message?limit={limit}&before={cursor}>; rel=\"next\"");
        headers.insert(
            "x-next-cursor",
            HeaderValue::from_str(&cursor).map_err(|err| Error::bad_request(err.to_string()))?,
        );
        headers.insert(
            header::LINK,
            HeaderValue::from_str(&link).map_err(|err| Error::bad_request(err.to_string()))?,
        );
        headers.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("Link, X-Next-Cursor"),
        );
    }
    Ok(response)
}

async fn session_messages_all(state: &AppState, session_id: &str) -> Result<Vec<MessageWithParts>> {
    let record = state.get_session(session_id).await?;
    state.ensure_messages_loaded(&record).await?;
    Ok(record.lock().await.messages.clone())
}

async fn v2_session_messages(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Query(query): Query<V2MessagesQuery>,
) -> Result<Json<Value>> {
    let limit = validate_v2_limit(query.limit, 50)?;
    let decoded = decode_v2_message_cursor(&query)?;
    let order = decoded
        .as_ref()
        .map(|cursor| cursor.order.as_str())
        .or(query.order.as_deref())
        .unwrap_or("desc");
    validate_v2_order(order)?;

    let mut messages = session_messages_all(&state, &session_id).await?;
    sort_messages_v2(&mut messages, order);
    if let Some(cursor) = decoded.as_ref() {
        messages = page_after_message_cursor(messages, cursor, order, limit);
    } else {
        messages.truncate(limit);
    }

    let previous = messages
        .first()
        .map(|message| encode_v2_message_cursor(message, order, "previous"))
        .transpose()?;
    let next = messages
        .last()
        .map(|message| encode_v2_message_cursor(message, order, "next"))
        .transpose()?;
    Ok(Json(json!({
        "items": messages,
        "cursor": {
            "previous": previous,
            "next": next,
        },
    })))
}

async fn v2_session_context(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<Vec<MessageWithParts>>> {
    Ok(Json(session_messages_all(&state, &session_id).await?))
}

async fn v2_compact_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<StatusCode> {
    state.get_session(&session_id).await?;
    Err(Error::service_unavailable(
        "V2 session compact is not available yet",
    ))
}

async fn v2_wait_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<StatusCode> {
    state.get_session(&session_id).await?;
    Err(Error::service_unavailable(
        "V2 session wait is not available yet",
    ))
}

async fn session_message(
    State(state): State<AppState>,
    AxumPath((session_id, message_id)): AxumPath<(String, String)>,
) -> Result<Json<MessageWithParts>> {
    let record = state.get_session(&session_id).await?;
    state.ensure_messages_loaded(&record).await?;
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

async fn delete_session_message(
    State(state): State<AppState>,
    AxumPath((session_id, message_id)): AxumPath<(String, String)>,
) -> Result<Json<bool>> {
    let record = state.get_session(&session_id).await?;
    state.ensure_not_busy(&session_id).await?;
    state.ensure_messages_loaded(&record).await?;
    let mut record = record.lock().await;
    let Some(index) = record
        .messages
        .iter()
        .position(|message| message.info.id() == message_id)
    else {
        return Err(Error::not_found(format!("message not found: {message_id}")));
    };
    record.messages.remove(index);
    record.info.touch();
    state.persist_record(&record)?;
    Ok(Json(true))
}

async fn update_message_part(
    State(state): State<AppState>,
    AxumPath((session_id, message_id, part_id)): AxumPath<(String, String, String)>,
    body: Bytes,
) -> Result<Json<Value>> {
    let payload = parse_optional_value(&body)?.unwrap_or_else(|| json!({}));
    let replacement: Part = serde_json::from_value(payload.clone())?;
    if part_id_of(&replacement) != part_id
        || part_message_id(&replacement) != message_id
        || part_session_id(&replacement) != session_id
    {
        return Err(Error::bad_request("part identity does not match path"));
    }
    let record = state.get_session(&session_id).await?;
    state.ensure_messages_loaded(&record).await?;
    let mut record = record.lock().await;
    let message = record
        .messages
        .iter_mut()
        .find(|message| message.info.id() == message_id)
        .ok_or_else(|| Error::not_found(format!("message not found: {message_id}")))?;
    let part = message
        .parts
        .iter_mut()
        .find(|part| part_id_of(part) == part_id)
        .ok_or_else(|| Error::not_found(format!("part not found: {part_id}")))?;
    *part = replacement;
    let value = json!(part);
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_part_updated(&record.info.directory, &session_id, value.clone());
    Ok(Json(value))
}

async fn delete_message_part(
    State(state): State<AppState>,
    AxumPath((session_id, message_id, part_id)): AxumPath<(String, String, String)>,
) -> Result<Json<bool>> {
    let record = state.get_session(&session_id).await?;
    state.ensure_messages_loaded(&record).await?;
    let mut record = record.lock().await;
    let message = record
        .messages
        .iter_mut()
        .find(|message| message.info.id() == message_id)
        .ok_or_else(|| Error::not_found(format!("message not found: {message_id}")))?;
    let Some(index) = message
        .parts
        .iter()
        .position(|part| part_id_of(part) == part_id)
    else {
        return Err(Error::not_found(format!("part not found: {part_id}")));
    };
    message.parts.remove(index);
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_for_directory(
        record.info.directory.clone(),
        json!({
            "type": "message.part.removed",
            "properties": {
                "sessionID": session_id,
                "messageID": message_id,
                "partID": part_id,
            },
        }),
    );
    Ok(Json(true))
}

async fn fork_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<SessionInfo>> {
    let payload = parse_optional_value(&body)?.unwrap_or_else(|| json!({}));
    let before_message_id = payload
        .get("messageID")
        .or_else(|| payload.get("messageId"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let parent = state.get_session(&session_id).await?;
    state.ensure_messages_loaded(&parent).await?;
    let (title, directory, workspace_id, messages) = {
        let parent = parent.lock().await;
        (
            Some(forked_title(&parent.info.title)),
            Some(PathBuf::from(&parent.info.directory)),
            parent.info.workspace_id.clone(),
            parent.messages.clone(),
        )
    };
    let child = state
        .create_session(
            Some(CreateSessionPayload {
                parent_id: None,
                title,
                agent: None,
                model: None,
                permission: None,
                workspace_id,
            }),
            directory,
        )
        .await?;

    let child_record = state.get_session(&child.id).await?;
    let child = {
        let mut child_record = child_record.lock().await;
        let mut id_map = HashMap::new();
        for message in &messages {
            if before_message_id
                .as_deref()
                .is_some_and(|boundary| message.info.id() >= boundary)
            {
                break;
            }
            let copied = fork_message(message, &child.id, &id_map);
            id_map.insert(message.info.id().to_string(), copied.info.id().to_string());
            child_record.messages.push(copied);
        }
        child_record.info.touch();
        state.persist_record(&child_record)?;
        child_record.info.clone()
    };
    Ok(Json(child))
}

async fn abort_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<bool>> {
    let record = state.get_session(&session_id).await?;
    if let Some(rpc) = &record.lock().await.rpc {
        rpc.abort().await?;
    }
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
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_session_updated(&record.info);
    Ok(Json(record.info.clone()))
}

async fn unshare_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<SessionInfo>> {
    let record = state.get_session(&session_id).await?;
    let mut record = record.lock().await;
    record.info.share = None;
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_session_updated(&record.info);
    Ok(Json(record.info.clone()))
}

async fn revert_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<SessionInfo>> {
    let payload =
        parse_optional_value(&body)?.ok_or_else(|| Error::bad_request("missing revert payload"))?;
    if !payload.is_object() {
        return Err(Error::bad_request("revert payload must be an object"));
    }
    required_string(&payload, "messageID")?;
    let record = state.get_session(&session_id).await?;
    state.ensure_not_busy(&session_id).await?;
    let mut record = record.lock().await;
    record.info.revert = Some(payload);
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_session_updated(&record.info);
    Ok(Json(record.info.clone()))
}

async fn unrevert_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<SessionInfo>> {
    let record = state.get_session(&session_id).await?;
    state.ensure_not_busy(&session_id).await?;
    let mut record = record.lock().await;
    record.info.revert = None;
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_session_updated(&record.info);
    Ok(Json(record.info.clone()))
}

async fn session_todo(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<Vec<Value>>> {
    state.get_session(&session_id).await?;
    Ok(Json(state.storage.list_todos(&session_id)?))
}

async fn session_diff(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Query(_query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<Value>>> {
    let Ok(record) = state.get_session(&session_id).await else {
        return Ok(Json(Vec::new()));
    };
    let directory = record.lock().await.info.directory.clone();
    git_diff(Path::new(&directory)).await.map(Json)
}

async fn init_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<bool>> {
    let payload =
        parse_optional_value(&body)?.ok_or_else(|| Error::bad_request("missing init payload"))?;
    for field in ["modelID", "providerID", "messageID"] {
        if payload.get(field).and_then(Value::as_str).is_none() {
            return Err(Error::bad_request(format!(
                "missing init payload field: {field}"
            )));
        }
    }
    let message_id = payload["messageID"].as_str().unwrap_or_default();
    let prompt = json!({
        "messageID": message_id,
        "parts": [{ "type": "text", "text": "/init" }],
    });
    prompt_impl(state, session_id, Bytes::from(prompt.to_string()), false).await?;
    Ok(Json(true))
}

async fn summarize_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<bool>> {
    let payload = parse_optional_value(&body)?
        .ok_or_else(|| Error::bad_request("missing summarize payload"))?;
    for field in ["modelID", "providerID"] {
        if payload.get(field).and_then(Value::as_str).is_none() {
            return Err(Error::bad_request(format!(
                "missing summarize payload field: {field}"
            )));
        }
    }
    let record = state.get_session(&session_id).await?;
    let mut record = record.lock().await;
    let messages = record.messages.len();
    record.info.summary = Some(json!({
        "additions": 0,
        "deletions": 0,
        "files": 0,
        "messages": messages,
    }));
    record.info.revert = None;
    record.info.touch();
    state.persist_record(&record)?;
    state.publish_session_updated(&record.info);
    Ok(Json(true))
}

async fn session_permission_reply(
    State(state): State<AppState>,
    AxumPath((session_id, permission_id)): AxumPath<(String, String)>,
    body: Bytes,
) -> Result<Json<bool>> {
    let payload = parse_optional_value(&body)?
        .ok_or_else(|| Error::bad_request("missing permission payload"))?;
    let response = permission_response(&payload)?;
    reply_to_permission(state, Some(session_id), permission_id, response)
        .await
        .map(Json)
}

async fn list_permissions(State(state): State<AppState>) -> Json<Vec<Value>> {
    let permissions = state
        .permissions
        .read()
        .await
        .values()
        .map(|request| request.request.clone())
        .collect();
    Json(permissions)
}

async fn permission_reply(
    State(state): State<AppState>,
    AxumPath(permission_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<bool>> {
    let payload = parse_optional_value(&body)?
        .ok_or_else(|| Error::bad_request("missing permission payload"))?;
    let response = permission_response(&payload)?;
    reply_to_permission(state, None, permission_id, response)
        .await
        .map(Json)
}

async fn reply_to_permission(
    state: AppState,
    session_id: Option<String>,
    permission_id: String,
    response: String,
) -> Result<bool> {
    let request = {
        let mut permissions = state.permissions.write().await;
        let request = permissions.get(&permission_id).cloned().ok_or_else(|| {
            Error::not_found(format!("permission request not found: {permission_id}"))
        })?;
        if let Some(session_id) = &session_id
            && request.session_id != *session_id
        {
            return Err(Error::not_found(format!(
                "permission request not found: {permission_id}"
            )));
        }
        permissions
            .remove(&permission_id)
            .expect("permission exists")
    };

    let record = state.get_session(&request.session_id).await?;
    let mut record = record.lock().await;
    if response == "always" {
        let rules = always_permission_rules(&request.request);
        if !rules.is_empty() {
            record.info.permission = Some(merge_permission_rules(
                record.info.permission.as_ref(),
                rules,
            ));
            record.info.touch();
            state.persist_record(&record)?;
            state.publish_session_updated(&record.info);
        }
    }
    state.publish_for_directory(
        record.info.directory.clone(),
        json!({
            "type": "permission.replied",
            "properties": {
                "sessionID": request.session_id,
                "requestID": permission_id,
                "reply": response,
            },
        }),
    );
    Ok(true)
}

async fn paths(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let directory = query
        .get("directory")
        .cloned()
        .unwrap_or_else(|| state.config.directory.display().to_string());
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

async fn git_diff(directory: &Path) -> Result<Vec<Value>> {
    let output = Command::new("git")
        .args(["diff", "--numstat", "--"])
        .current_dir(directory)
        .output()
        .await?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut diffs = Vec::new();
    for line in stdout.lines() {
        let mut fields = line.splitn(3, '\t');
        let additions = parse_numstat(fields.next());
        let deletions = parse_numstat(fields.next());
        let Some(file) = fields.next().filter(|file| !file.is_empty()) else {
            continue;
        };
        let patch = Command::new("git")
            .args(["diff", "--", file])
            .current_dir(directory)
            .output()
            .await
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).to_string())
            .filter(|patch| !patch.is_empty());
        diffs.push(json!({
            "file": file,
            "patch": patch,
            "additions": additions,
            "deletions": deletions,
            "status": "modified",
        }));
    }
    Ok(diffs)
}

fn parse_numstat(value: Option<&str>) -> i64 {
    value.and_then(|value| value.parse().ok()).unwrap_or(0)
}

async fn project_current(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>> {
    let directory = query
        .get("directory")
        .cloned()
        .unwrap_or_else(|| state.config.directory.display().to_string());
    let project = state.storage.project_for_directory(&directory)?;
    state.publish_project_updated(&project);
    Ok(Json(project))
}

async fn project_list(State(state): State<AppState>) -> Result<Json<Vec<Value>>> {
    Ok(Json(state.storage.list_projects()?))
}

async fn project_git_init(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>> {
    let directory = query
        .get("directory")
        .cloned()
        .unwrap_or_else(|| state.config.directory.display().to_string());
    let project = state.storage.project_for_directory(&directory)?;
    let project_id = project["id"]
        .as_str()
        .ok_or_else(|| Error::bad_request("project id missing"))?;
    let output = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(&directory)
        .output()
        .await
        .map_err(Error::from)?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Err(Error::Process(if stderr.is_empty() {
            stdout
        } else {
            stderr
        }));
    }
    let project = state.storage.update_project(
        project_id,
        json!({
            "vcs": "git",
            "time": {
                "initialized": now_ms(),
            },
        }),
    )?;
    state.publish_project_updated(&project);
    Ok(Json(project))
}

async fn update_project(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<Value>> {
    let patch = parse_optional_value(&body)?.unwrap_or_else(|| json!({}));
    let patch = patch
        .as_object()
        .ok_or_else(|| Error::bad_request("project update payload must be an object"))?;
    let mut sanitized = serde_json::Map::new();
    if let Some(name) = patch.get("name") {
        if !name.is_string() {
            return Err(Error::bad_request("project name must be a string"));
        }
        sanitized.insert("name".to_string(), name.clone());
    }
    if let Some(icon) = patch.get("icon") {
        let icon = icon
            .as_object()
            .ok_or_else(|| Error::bad_request("project icon must be an object"))?;
        let mut sanitized_icon = serde_json::Map::new();
        for field in ["url", "override", "color"] {
            let Some(value) = icon.get(field) else {
                continue;
            };
            if !value.is_string() {
                return Err(Error::bad_request(format!(
                    "project icon field must be a string: {field}"
                )));
            }
            sanitized_icon.insert(field.to_string(), value.clone());
        }
        if !sanitized_icon.is_empty() {
            sanitized.insert("icon".to_string(), Value::Object(sanitized_icon));
        }
    }
    if let Some(commands) = patch.get("commands") {
        let commands = commands
            .as_object()
            .ok_or_else(|| Error::bad_request("project commands must be an object"))?;
        let mut sanitized_commands = serde_json::Map::new();
        if let Some(start) = commands.get("start") {
            if !start.is_string() {
                return Err(Error::bad_request("project command start must be a string"));
            }
            sanitized_commands.insert("start".to_string(), start.clone());
        }
        if !sanitized_commands.is_empty() {
            sanitized.insert("commands".to_string(), Value::Object(sanitized_commands));
        }
    }
    let project = state
        .storage
        .update_project(&project_id, Value::Object(sanitized))?;
    state.publish_project_updated(&project);
    Ok(Json(project))
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
        "type": "local",
        "projectID": state.project_id.clone(),
        "name": "local",
        "directory": state.config.directory.display().to_string(),
        "timeUsed": 0,
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
    assistant_id: &str,
    cwd: &Path,
    event: &Value,
) -> Option<MessageWithParts> {
    if let Some(error) = event.get("error").and_then(Value::as_str)
        && !error.is_empty()
    {
        return Some(assistant_message_with_id(
            session,
            parent_id,
            assistant_id.to_string(),
            error,
            cwd,
        ));
    }

    let messages = event.get("messages").and_then(Value::as_array)?;
    let assistant = messages.iter().rev().find(|message| {
        message.get("role").and_then(Value::as_str) == Some("assistant")
            || message.get("type").and_then(Value::as_str) == Some("assistant")
    })?;
    let text = extract_text(assistant);
    Some(assistant_message_with_id(
        session,
        parent_id,
        assistant_id.to_string(),
        text,
        cwd,
    ))
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
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    path.strip_prefix(&root)
        .unwrap_or(&path)
        .display()
        .to_string()
}
