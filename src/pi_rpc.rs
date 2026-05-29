use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::timeout;
use tracing::{debug, error, warn};

use crate::error::{Error, Result};
use crate::ids;

const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const RPC_AGENT_END_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug)]
pub struct PiRpcClient {
    stdin: Arc<Mutex<ChildStdin>>,
    child: Arc<Mutex<Child>>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
    events: broadcast::Sender<Value>,
    prompt_lock: Mutex<()>,
}

impl PiRpcClient {
    pub async fn spawn(pi_bin: &Path, directory: &Path) -> Result<Arc<Self>> {
        let mut command = Command::new(pi_bin);
        command
            .args(["--mode", "rpc"])
            .current_dir(directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|err| {
            Error::Process(format!(
                "failed to spawn {} --mode rpc: {err}",
                pi_bin.display()
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Process("pi child stdin was not piped".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Process("pi child stdout was not piped".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::Process("pi child stderr was not piped".to_string()))?;

        let (events, _) = broadcast::channel(1024);
        let client = Arc::new(Self {
            stdin: Arc::new(Mutex::new(stdin)),
            child: Arc::new(Mutex::new(child)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            events,
            prompt_lock: Mutex::new(()),
        });

        Self::spawn_stdout_reader(Arc::clone(&client), stdout);
        Self::spawn_stderr_reader(stderr);

        Ok(client)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    pub async fn request(&self, command: &str, payload: Value) -> Result<Value> {
        let id = ids::request_id();
        let mut request = match payload {
            Value::Object(map) => Value::Object(map),
            Value::Null => json!({}),
            other => json!({ "data": other }),
        };
        let object = request
            .as_object_mut()
            .ok_or_else(|| Error::bad_request("RPC payload must be an object"))?;
        object.insert("id".to_string(), Value::String(id.clone()));
        object.insert("type".to_string(), Value::String(command.to_string()));

        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);

        let line = format!("{request}\n");
        let write_result = {
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(line.as_bytes()).await
        };
        if let Err(err) = write_result {
            self.pending.lock().await.remove(&id);
            return Err(Error::Process(format!(
                "failed to write pi rpc request: {err}"
            )));
        }

        let response = timeout(RPC_REQUEST_TIMEOUT, rx)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Process("pi rpc response channel closed".to_string()))?;

        if response.get("success").and_then(Value::as_bool) == Some(false) {
            let error = response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("pi rpc command failed");
            return Err(Error::pi_rpc(error));
        }

        Ok(response)
    }

    pub async fn prompt(&self, text: &str) -> Result<Value> {
        let _guard = self.prompt_lock.lock().await;
        let mut events = self.subscribe();
        let ack = self.request("prompt", json!({ "message": text })).await?;
        debug!(?ack, "pi prompt accepted");
        wait_for_agent_end(&mut events).await
    }

    pub async fn prompt_async(self: &Arc<Self>, text: String) -> Result<()> {
        let client = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = client.prompt(&text).await {
                warn!(%err, "background pi prompt failed");
            }
        });
        Ok(())
    }

    pub async fn get_messages(&self) -> Result<Vec<Value>> {
        let response = self.request("get_messages", Value::Null).await?;
        Ok(response
            .get("data")
            .and_then(|data| data.get("messages"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    pub async fn abort(&self) -> Result<()> {
        let _ = self.request("abort", Value::Null).await?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        let mut child = self.child.lock().await;
        if let Err(err) = child.kill().await {
            debug!(%err, "failed to kill pi child");
        }
    }

    fn spawn_stdout_reader(client: Arc<Self>, stdout: tokio::process::ChildStdout) {
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                let line = match lines.next_line().await {
                    Ok(Some(line)) => line,
                    Ok(None) => break,
                    Err(err) => {
                        error!(%err, "failed to read pi rpc stdout");
                        break;
                    }
                };

                if line.trim().is_empty() {
                    continue;
                }

                let value: Value = match serde_json::from_str(&line) {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(%err, line, "pi rpc emitted non-json stdout line");
                        continue;
                    }
                };

                if value.get("type").and_then(Value::as_str) == Some("response")
                    && let Some(id) = value.get("id").and_then(Value::as_str)
                    && let Some(tx) = client.pending.lock().await.remove(id)
                {
                    let _ = tx.send(value.clone());
                }

                let _ = client.events.send(value);
            }

            let mut pending = client.pending.lock().await;
            for (_, tx) in pending.drain() {
                let _ = tx.send(json!({
                    "type": "response",
                    "success": false,
                    "error": "pi rpc process exited",
                }));
            }
        });
    }

    fn spawn_stderr_reader(stderr: tokio::process::ChildStderr) {
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if !line.trim().is_empty() {
                    debug!(line, "pi rpc stderr");
                }
            }
        });
    }
}

async fn wait_for_agent_end(events: &mut broadcast::Receiver<Value>) -> Result<Value> {
    timeout(RPC_AGENT_END_TIMEOUT, async move {
        loop {
            match events.recv().await {
                Ok(value) if value.get("type").and_then(Value::as_str) == Some("agent_end") => {
                    return Ok(value);
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "lagged while waiting for pi agent_end");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(Error::Process("pi rpc event stream closed".to_string()));
                }
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)?
}
