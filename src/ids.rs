use uuid::Uuid;

pub fn session_id() -> String {
    prefixed("ses")
}

pub fn message_id() -> String {
    prefixed("msg")
}

pub fn part_id() -> String {
    prefixed("prt")
}

pub fn project_id() -> String {
    prefixed("prj")
}

pub fn workspace_id() -> String {
    prefixed("wrk")
}

pub fn request_id() -> String {
    prefixed("req")
}

pub fn slug(input: &str) -> String {
    let mut result = String::new();
    let mut previous_dash = false;
    for ch in input.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            result.push(ch);
            previous_dash = false;
        } else if !previous_dash {
            result.push('-');
            previous_dash = true;
        }
    }
    let trimmed = result.trim_matches('-');
    if trimmed.is_empty() {
        "session".to_string()
    } else {
        trimmed.to_string()
    }
}

fn prefixed(prefix: &str) -> String {
    let raw = Uuid::new_v4().simple().to_string();
    format!("{prefix}_{raw}")
}
