use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Tokens {
    pub input: f64,
    pub output: f64,
    pub reasoning: f64,
    pub cache: CacheTokens,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CacheTokens {
    pub read: f64,
    pub write: f64,
}

impl Default for Tokens {
    fn default() -> Self {
        Self {
            input: 0.0,
            output: 0.0,
            reasoning: 0.0,
            cache: CacheTokens {
                read: 0.0,
                write: 0.0,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRef {
    pub provider_id: String,
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

impl Default for ModelRef {
    fn default() -> Self {
        Self {
            provider_id: "pi".to_string(),
            model_id: "default".to_string(),
            variant: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTime {
    pub created: i64,
    pub updated: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compacting: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub id: String,
    pub slug: String,
    pub project_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    pub directory: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<Value>,
    pub cost: f64,
    pub tokens: Tokens,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share: Option<Value>,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRef>,
    pub version: String,
    pub time: SessionTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revert: Option<Value>,
}

impl SessionInfo {
    pub fn new(directory: &Path, title: Option<String>) -> Self {
        let now = now_ms();
        let title =
            title.unwrap_or_else(|| format!("New session - {}", chrono::Utc::now().to_rfc3339()));
        Self {
            id: ids::session_id(),
            slug: ids::slug(&title),
            project_id: ids::project_id(),
            workspace_id: None,
            directory: directory.display().to_string(),
            path: None,
            parent_id: None,
            summary: None,
            cost: 0.0,
            tokens: Tokens::default(),
            share: None,
            title,
            agent: Some("build".to_string()),
            model: Some(ModelRef::default()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            time: SessionTime {
                created: now,
                updated: now,
                compacting: None,
                archived: None,
            },
            permission: None,
            revert: None,
        }
    }

    pub fn touch(&mut self) {
        self.time.updated = now_ms();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageWithParts {
    pub info: MessageInfo,
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "role")]
pub enum MessageInfo {
    #[serde(rename = "user")]
    User(UserMessageInfo),
    #[serde(rename = "assistant")]
    Assistant(AssistantMessageInfo),
}

impl MessageInfo {
    pub fn id(&self) -> &str {
        match self {
            Self::User(info) => &info.id,
            Self::Assistant(info) => &info.id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserMessageInfo {
    pub id: String,
    pub session_id: String,
    pub time: CreatedTime,
    pub agent: String,
    pub model: ModelRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssistantMessageInfo {
    pub id: String,
    pub session_id: String,
    pub time: CompletedTime,
    pub parent_id: String,
    pub model_id: String,
    pub provider_id: String,
    pub mode: String,
    pub agent: String,
    pub path: MessagePath,
    pub cost: f64,
    pub tokens: Tokens,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatedTime {
    pub created: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedTime {
    pub created: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePath {
    pub cwd: String,
    pub root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Part {
    #[serde(rename = "text")]
    Text(TextPart),
    #[serde(rename = "reasoning")]
    Reasoning(TextPart),
    #[serde(rename = "file")]
    File(FilePart),
    #[serde(rename = "tool")]
    Tool(ToolPart),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TextPart {
    pub id: String,
    pub session_id: String,
    pub message_id: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<CompletedTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilePart {
    pub id: String,
    pub session_id: String,
    pub message_id: String,
    pub mime: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolPart {
    pub id: String,
    pub session_id: String,
    pub message_id: String,
    pub call_id: String,
    pub tool: String,
    pub state: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptPayload {
    #[serde(default)]
    pub parts: Vec<Value>,
    #[serde(default)]
    pub no_reply: bool,
    pub model: Option<ModelRef>,
    pub agent: Option<String>,
    pub message_id: Option<String>,
    #[serde(default)]
    pub tools: Option<Value>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

impl PromptPayload {
    pub fn text(&self) -> String {
        if let Some(text) = self.extra.get("message").and_then(Value::as_str) {
            return text.to_string();
        }
        if let Some(text) = self.extra.get("prompt").and_then(Value::as_str) {
            return text.to_string();
        }
        self.parts
            .iter()
            .filter_map(|part| {
                (part.get("type").and_then(Value::as_str) == Some("text"))
                    .then(|| part.get("text").and_then(Value::as_str))
                    .flatten()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSessionPayload {
    pub parent_id: Option<String>,
    pub title: Option<String>,
    pub agent: Option<String>,
    pub model: Option<ModelRef>,
    pub permission: Option<Vec<Value>>,
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileNode {
    pub name: String,
    pub path: String,
    pub kind: String,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn text_part(session_id: &str, message_id: &str, text: impl Into<String>) -> Part {
    Part::Text(TextPart {
        id: ids::part_id(),
        session_id: session_id.to_string(),
        message_id: message_id.to_string(),
        text: text.into(),
        time: None,
        metadata: None,
    })
}

pub fn user_message(
    session: &SessionInfo,
    payload: &PromptPayload,
    default_text: impl Into<String>,
) -> MessageWithParts {
    let id = payload.message_id.clone().unwrap_or_else(ids::message_id);
    let text = default_text.into();
    let agent = payload
        .agent
        .clone()
        .or_else(|| session.agent.clone())
        .unwrap_or_else(|| "build".to_string());
    let model = payload
        .model
        .clone()
        .or_else(|| session.model.clone())
        .unwrap_or_default();
    MessageWithParts {
        info: MessageInfo::User(UserMessageInfo {
            id: id.clone(),
            session_id: session.id.clone(),
            time: CreatedTime { created: now_ms() },
            agent,
            model,
            system: None,
            tools: payload.tools.clone(),
        }),
        parts: vec![text_part(&session.id, &id, text)],
    }
}

pub fn assistant_message(
    session: &SessionInfo,
    parent_id: &str,
    text: impl Into<String>,
    cwd: &Path,
) -> MessageWithParts {
    let id = ids::message_id();
    let now = now_ms();
    let model = session.model.clone().unwrap_or_default();
    MessageWithParts {
        info: MessageInfo::Assistant(AssistantMessageInfo {
            id: id.clone(),
            session_id: session.id.clone(),
            time: CompletedTime {
                created: now,
                completed: Some(now),
            },
            parent_id: parent_id.to_string(),
            model_id: model.model_id,
            provider_id: model.provider_id,
            mode: session.agent.clone().unwrap_or_else(|| "build".to_string()),
            agent: session.agent.clone().unwrap_or_else(|| "build".to_string()),
            path: MessagePath {
                cwd: cwd.display().to_string(),
                root: cwd.display().to_string(),
            },
            cost: 0.0,
            tokens: Tokens::default(),
            finish: Some("stop".to_string()),
            error: None,
        }),
        parts: vec![text_part(&session.id, &id, text)],
    }
}

pub fn path_to_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}
