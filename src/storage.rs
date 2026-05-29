use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

use crate::error::{Error, Result};
use crate::ids;
use crate::models::{SessionInfo, now_ms};

#[derive(Debug, Clone)]
pub struct Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                worktree TEXT NOT NULL UNIQUE,
                data TEXT NOT NULL,
                updated INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                directory TEXT NOT NULL,
                parent_id TEXT,
                project_id TEXT NOT NULL,
                updated INTEGER NOT NULL,
                data TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS sessions_directory_updated_idx
                ON sessions(directory, updated DESC);
            CREATE INDEX IF NOT EXISTS sessions_parent_idx
                ON sessions(parent_id);

            CREATE TABLE IF NOT EXISTS todos (
                session_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (session_id, position),
                FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS todos_session_position_idx
                ON todos(session_id, position);
            "#,
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn get_or_create_project(&self, worktree: &Path) -> Result<Value> {
        let worktree = path_to_string(&project_worktree(worktree));
        if let Some(project) = self.project_by_worktree(&worktree)? {
            let project = self.refresh_project_vcs(project)?;
            return Ok(project);
        }

        let now = now_ms();
        let mut project = json!({
            "id": ids::project_id(),
            "name": PathBuf::from(&worktree).file_name().and_then(|name| name.to_str()),
            "worktree": worktree,
            "time": { "created": now, "updated": now },
            "sandboxes": [],
        });
        if git_root(Path::new(project["worktree"].as_str().unwrap_or_default())).is_some()
            && let Some(project) = project.as_object_mut()
        {
            project.insert("vcs".to_string(), json!("git"));
        }
        self.save_project(&project)?;
        Ok(project)
    }

    pub fn project_for_directory(&self, directory: &str) -> Result<Value> {
        self.get_or_create_project(Path::new(directory))
    }

    pub fn project_id_for_directory(&self, directory: &str) -> Result<String> {
        let project = self.project_for_directory(directory)?;
        Ok(project["id"].as_str().unwrap_or_default().to_string())
    }

    pub fn list_projects(&self) -> Result<Vec<Value>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare("SELECT data FROM projects ORDER BY updated DESC, id DESC")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|data| serde_json::from_str(&data).map_err(Error::from))
            .collect()
    }

    pub fn project_by_id(&self, project_id: &str) -> Result<Option<Value>> {
        let data = self
            .conn()?
            .query_row(
                "SELECT data FROM projects WHERE id = ?1",
                params![project_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        data.map(|data| serde_json::from_str(&data).map_err(Error::from))
            .transpose()
    }

    pub fn update_project(&self, project_id: &str, patch: Value) -> Result<Value> {
        let Some(mut project) = self.project_by_id(project_id)? else {
            return Err(Error::not_found(format!("project not found: {project_id}")));
        };

        if let (Some(project), Some(patch)) = (project.as_object_mut(), patch.as_object()) {
            for (key, value) in patch {
                if key == "id" || key == "worktree" {
                    continue;
                }
                if key == "time" {
                    if let (Some(current), Some(incoming)) = (
                        project.get_mut("time").and_then(Value::as_object_mut),
                        value.as_object(),
                    ) {
                        for (time_key, time_value) in incoming {
                            current.insert(time_key.clone(), time_value.clone());
                        }
                    }
                    continue;
                }
                project.insert(key.clone(), value.clone());
            }
            let now = now_ms();
            let time = project.entry("time").or_insert_with(|| json!({}));
            if let Some(time) = time.as_object_mut() {
                time.entry("created").or_insert(json!(now));
                time.insert("updated".to_string(), json!(now));
            }
        }

        self.save_project(&project)?;
        Ok(project)
    }

    pub fn save_session(&self, session: &SessionInfo) -> Result<()> {
        let data = serde_json::to_string(session)?;
        self.conn()?.execute(
            r#"
            INSERT INTO sessions (id, directory, parent_id, project_id, updated, data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(id) DO UPDATE SET
                directory = excluded.directory,
                parent_id = excluded.parent_id,
                project_id = excluded.project_id,
                updated = excluded.updated,
                data = excluded.data
            "#,
            params![
                session.id,
                session.directory,
                session.parent_id,
                session.project_id,
                session.time.updated,
                data
            ],
        )?;
        Ok(())
    }

    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        self.conn()?
            .execute("DELETE FROM sessions WHERE id = ?1", params![session_id])?;
        Ok(())
    }

    pub fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare("SELECT data FROM sessions ORDER BY updated DESC, id DESC")?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|data| serde_json::from_str(&data).map_err(Error::from))
            .collect()
    }

    pub fn replace_todos(&self, session_id: &str, todos: &[Value]) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM todos WHERE session_id = ?1",
            params![session_id],
        )?;
        {
            let mut stmt = tx.prepare(
                r#"
                INSERT INTO todos (session_id, position, data)
                VALUES (?1, ?2, ?3)
                "#,
            )?;
            for (position, todo) in todos.iter().enumerate() {
                stmt.execute(params![
                    session_id,
                    position as i64,
                    serde_json::to_string(todo)?
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_todos(&self, session_id: &str) -> Result<Vec<Value>> {
        let conn = self.conn()?;
        let mut stmt =
            conn.prepare("SELECT data FROM todos WHERE session_id = ?1 ORDER BY position ASC")?;
        let rows = stmt
            .query_map(params![session_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|data| serde_json::from_str(&data).map_err(Error::from))
            .collect()
    }

    fn save_project(&self, project: &Value) -> Result<()> {
        let id = project["id"]
            .as_str()
            .ok_or_else(|| Error::bad_request("project id missing"))?;
        let worktree = project["worktree"]
            .as_str()
            .ok_or_else(|| Error::bad_request("project worktree missing"))?;
        let updated = project["time"]["updated"].as_i64().unwrap_or_else(now_ms);
        let data = serde_json::to_string(project)?;
        self.conn()?.execute(
            r#"
            INSERT INTO projects (id, worktree, data, updated)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(id) DO UPDATE SET
                worktree = excluded.worktree,
                data = excluded.data,
                updated = excluded.updated
            "#,
            params![id, worktree, data, updated],
        )?;
        Ok(())
    }

    fn project_by_worktree(&self, worktree: &str) -> Result<Option<Value>> {
        let data = self
            .conn()?
            .query_row(
                "SELECT data FROM projects WHERE worktree = ?1",
                params![worktree],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        data.map(|data| serde_json::from_str(&data).map_err(Error::from))
            .transpose()
    }

    fn refresh_project_vcs(&self, mut project: Value) -> Result<Value> {
        let Some(worktree) = project["worktree"].as_str() else {
            return Ok(project);
        };
        if project.get("vcs").is_none() && git_root(Path::new(worktree)).is_some() {
            if let Some(project) = project.as_object_mut() {
                project.insert("vcs".to_string(), json!("git"));
                let now = now_ms();
                let time = project.entry("time").or_insert_with(|| json!({}));
                if let Some(time) = time.as_object_mut() {
                    time.entry("created").or_insert(json!(now));
                    time.insert("updated".to_string(), json!(now));
                }
            }
            self.save_project(&project)?;
        }
        Ok(project)
    }

    fn conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| Error::Process("sqlite connection lock poisoned".to_string()))
    }
}

fn project_worktree(path: &Path) -> PathBuf {
    git_root(path).unwrap_or_else(|| path.to_path_buf())
}

fn git_root(path: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!root.is_empty()).then(|| PathBuf::from(root))
}

fn path_to_string(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}
