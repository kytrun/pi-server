use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use pi_server::config::ServerConfig;
use pi_server::ids;
use pi_server::opencode_routes::OPENCODE_ROUTES;
use pi_server::server::{AppState, app, app_with_state};
use regex::Regex;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
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

    let projects = get_json(app.clone(), "/project").await;
    let project = &projects.as_array().expect("projects array")[0];
    assert!(project["id"].as_str().is_some());
    assert_eq!(project["worktree"], harness.workdir.display().to_string());
    assert!(project["time"]["created"].is_number());
    assert!(project["time"]["updated"].is_number());
    assert!(project["sandboxes"].is_array());

    let current_project = get_json(app.clone(), "/project/current").await;
    assert_eq!(current_project["id"], project["id"]);
    assert!(current_project["time"]["updated"].is_number());

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
async fn tui_can_create_a_new_session_with_selected_model_shape() {
    let harness = Harness::new();
    let app = app(harness.config());

    let session = post_json(
        app,
        "/session",
        json!({
            "agent": "build",
            "model": {
                "providerID": "pi",
                "id": "default"
            }
        }),
        StatusCode::OK,
    )
    .await;

    assert_eq!(session["agent"], "build");
    assert_eq!(session["model"]["providerID"], "pi");
    assert_eq!(session["model"]["modelID"], "default");
}

#[tokio::test]
async fn session_create_emits_desktop_sidebar_created_before_updated() {
    let harness = Harness::new();
    let state = AppState::new(harness.config());
    let mut events = state.subscribe_events();
    let app = app_with_state(state);

    let session = create_session(app.clone()).await;
    let session_id = session["id"].as_str().expect("session id");

    let created = next_event(&mut events, "session.created").await;
    assert_eq!(created["properties"]["sessionID"], session_id);
    assert_eq!(created["properties"]["info"]["id"], session_id);

    let updated = next_event(&mut events, "session.updated").await;
    assert_eq!(updated["properties"]["sessionID"], session_id);

    let directory = harness.workdir.to_str().expect("workdir");
    let sessions = get_json(
        app.clone(),
        &format!("/session?directory={directory}&roots=true&limit=10"),
    )
    .await;
    assert_eq!(sessions.as_array().expect("sessions").len(), 1);
    assert_eq!(sessions[0]["id"], session_id);

    let forked = post_json(
        app.clone(),
        &format!("/session/{session_id}/fork"),
        json!({}),
        StatusCode::OK,
    )
    .await;
    let forked_id = forked["id"].as_str().expect("fork id");
    let _fork_created = next_event(&mut events, "session.created").await;
    let _fork_updated = next_event(&mut events, "session.updated").await;

    let roots = get_json(
        app.clone(),
        &format!("/session?directory={directory}&roots=true"),
    )
    .await;
    assert_eq!(roots.as_array().expect("root sessions").len(), 1);
    assert_eq!(roots[0]["id"], session_id);

    let all = get_json(app, &format!("/session?directory={directory}")).await;
    assert_eq!(
        all.as_array()
            .expect("all sessions")
            .iter()
            .map(|session| session["id"].as_str().expect("id"))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([session_id, forked_id])
    );
}

#[tokio::test]
async fn desktop_session_create_uses_directory_header_for_sidebar_lists() {
    let harness = Harness::new();
    let state = AppState::new(harness.config());
    let mut events = state.subscribe_events();
    let app = app_with_state(state);
    let desktop_directory = harness._tmp.path().join("desktop-project");
    fs::create_dir(&desktop_directory).expect("desktop project dir");
    let encoded_directory = encode_component(&desktop_directory.display().to_string());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/session")
                .header("content-type", "application/json")
                .header("x-opencode-directory", encoded_directory.clone())
                .body(Body::from(
                    json!({ "title": "Desktop session" }).to_string(),
                ))
                .expect("desktop create session request"),
        )
        .await
        .expect("POST /session");
    assert_eq!(response.status(), StatusCode::OK);

    let session = response_json(response).await;
    let session_id = session["id"].as_str().expect("session id");
    let directory = desktop_directory.display().to_string();
    assert_eq!(session["directory"], directory);

    let created = next_wrapped_event(&mut events, "session.created").await;
    assert_eq!(created["directory"], directory);
    assert_eq!(created["payload"]["properties"]["sessionID"], session_id);

    let updated = next_wrapped_event(&mut events, "session.updated").await;
    assert_eq!(updated["directory"], directory);
    assert_eq!(updated["payload"]["properties"]["sessionID"], session_id);

    let desktop_sessions = get_json(
        app.clone(),
        &format!("/session?directory={encoded_directory}&roots=true&limit=10"),
    )
    .await;
    assert_eq!(
        desktop_sessions
            .as_array()
            .expect("desktop sessions")
            .iter()
            .map(|session| session["id"].as_str().expect("id"))
            .collect::<Vec<_>>(),
        vec![session_id]
    );

    let default_sessions = get_json(
        app.clone(),
        &format!(
            "/session?directory={}",
            encode_component(&harness.workdir.display().to_string())
        ),
    )
    .await;
    assert!(
        default_sessions
            .as_array()
            .expect("default sessions")
            .is_empty(),
        "desktop-created session should not be hidden under the server cwd"
    );

    let paths = get_json(app, &format!("/path?directory={encoded_directory}")).await;
    assert_eq!(paths["directory"], directory);
    assert_eq!(paths["worktree"], directory);
}

#[tokio::test]
async fn desktop_prompt_events_are_published_to_the_session_directory() {
    let harness = Harness::new();
    let state = AppState::new(harness.config());
    let mut events = state.subscribe_events();
    let app = app_with_state(state);
    let desktop_directory = harness._tmp.path().join("desktop-project");
    fs::create_dir(&desktop_directory).expect("desktop project dir");
    let encoded_directory = encode_component(&desktop_directory.display().to_string());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/session")
                .header("content-type", "application/json")
                .header("x-opencode-directory", encoded_directory)
                .body(Body::empty())
                .expect("desktop create session request"),
        )
        .await
        .expect("POST /session");
    assert_eq!(response.status(), StatusCode::OK);
    let session = response_json(response).await;
    let session_id = session["id"].as_str().expect("session id");
    let directory = desktop_directory.display().to_string();
    let _created = next_wrapped_event(&mut events, "session.created").await;
    let _updated = next_wrapped_event(&mut events, "session.updated").await;

    let assistant = prompt(app, session_id, "desktop live response").await;
    assert_eq!(assistant["info"]["role"], "assistant");

    let mut saw_assistant_message = false;
    let mut saw_assistant_part = false;
    let mut saw_idle_status = false;
    timeout(Duration::from_secs(2), async {
        while !(saw_assistant_message && saw_assistant_part && saw_idle_status) {
            let event = events.recv().await.expect("event");
            if event["payload"]["properties"]["sessionID"] != session_id {
                continue;
            }
            let event_type = event["payload"]["type"].as_str().expect("event type");
            match event_type {
                "message.updated"
                | "message.part.updated"
                | "message.part.delta"
                | "session.status" => {
                    assert_eq!(
                        event["directory"], directory,
                        "{event_type} should be routed to the session directory"
                    );
                }
                _ => continue,
            }
            if event_type == "message.updated"
                && event["payload"]["properties"]["info"]["role"] == "assistant"
            {
                saw_assistant_message = true;
            }
            if event_type == "message.part.updated"
                && event["payload"]["properties"]["part"]["messageID"] == assistant["info"]["id"]
            {
                saw_assistant_part = true;
            }
            if event_type == "session.status"
                && event["payload"]["properties"]["status"]["type"] == "idle"
            {
                saw_idle_status = true;
            }
        }
    })
    .await
    .expect("prompt events for desktop session");
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
    assert!(
        body["parts"][0]["time"]["end"].is_number(),
        "assistant text parts need time.end for opencode run output"
    );

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

#[tokio::test]
async fn smoke_session_lifecycle_followups_and_events_are_opencode_shaped() {
    let harness = Harness::new();
    let state = AppState::new(harness.config());
    let mut events = state.subscribe_events();
    let app = app_with_state(state);

    let project = get_json(app.clone(), "/project/current").await;
    let session = post_json(
        app.clone(),
        "/session",
        json!({ "title": "Smoke root" }),
        StatusCode::OK,
    )
    .await;
    let session_id = session["id"].as_str().expect("session id");
    assert_eq!(session["projectID"], project["id"]);

    let created_event = next_event(&mut events, "session.updated").await;
    assert_eq!(created_event["properties"]["sessionID"], session_id);
    assert_eq!(created_event["properties"]["info"]["id"], session_id);

    let first = prompt(app.clone(), session_id, "first turn").await;
    assert_eq!(first["info"]["role"], "assistant");
    assert_eq!(first["parts"][0]["text"], "echo: first turn");
    let first_part_event = next_event(&mut events, "message.part.updated").await;
    assert_eq!(
        first_part_event["properties"]["part"]["sessionID"],
        session_id
    );

    let second = prompt(app.clone(), session_id, "follow up").await;
    assert_eq!(second["parts"][0]["text"], "echo: follow up");

    let messages = get_json(app.clone(), &format!("/session/{session_id}/message")).await;
    let messages = messages.as_array().expect("messages array");
    assert_eq!(messages.len(), 4);
    assert_eq!(messages[0]["info"]["role"], "user");
    assert_eq!(messages[1]["info"]["role"], "assistant");
    assert!(messages[1]["parts"][0]["time"]["end"].is_number());
    assert_eq!(messages[2]["parts"][0]["text"], "follow up");
    assert_eq!(messages[3]["info"]["parentID"], messages[2]["info"]["id"]);
    assert!(messages[3]["parts"][0]["time"]["end"].is_number());

    let updated = patch_json(
        app.clone(),
        &format!("/session/{session_id}"),
        json!({
            "title": "Renamed smoke",
            "time": { "archived": 1234 }
        }),
    )
    .await;
    assert_eq!(updated["title"], "Renamed smoke");
    assert_eq!(updated["time"]["archived"], 1234);
    let updated_event = next_event(&mut events, "session.updated").await;
    assert_eq!(
        updated_event["properties"]["info"]["title"],
        "Renamed smoke"
    );

    let shared = post_json(
        app.clone(),
        &format!("/session/{session_id}/share"),
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert!(
        shared["share"]["url"]
            .as_str()
            .expect("share url")
            .contains(session_id)
    );
    let unshared = delete_json(
        app.clone(),
        &format!("/session/{session_id}/share"),
        StatusCode::OK,
    )
    .await;
    assert!(unshared.get("share").is_none() || unshared["share"].is_null());

    let forked = post_json(
        app.clone(),
        &format!("/session/{session_id}/fork"),
        json!({}),
        StatusCode::OK,
    )
    .await;
    let forked_id = forked["id"].as_str().expect("forked id");
    assert_eq!(forked["parentID"], session_id);
    let children = get_json(app.clone(), &format!("/session/{session_id}/children")).await;
    assert_eq!(children.as_array().expect("children")[0]["id"], forked_id);

    let deleted_child = delete_json(
        app.clone(),
        &format!("/session/{forked_id}"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(deleted_child, true);
    let deleted_event = next_event(&mut events, "session.deleted").await;
    assert_eq!(deleted_event["properties"]["sessionID"], forked_id);
    assert_eq!(deleted_event["properties"]["info"]["id"], forked_id);

    let deleted_parent = delete_json(
        app.clone(),
        &format!("/session/{session_id}"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(deleted_parent, true);
    let list = get_json(app, "/session").await;
    assert!(list.as_array().expect("sessions").is_empty());
}

#[tokio::test]
async fn async_prompt_records_the_assistant_followup() {
    let harness = Harness::new();
    let app = app(harness.config());
    let session = create_session(app.clone()).await;
    let session_id = session["id"].as_str().expect("session id");

    let response = app
        .clone()
        .oneshot(request(
            Method::POST,
            &format!("/session/{session_id}/prompt_async"),
            Body::from(json!({ "parts": [{ "type": "text", "text": "background" }] }).to_string()),
        ))
        .await
        .expect("POST prompt_async");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let messages = timeout(Duration::from_secs(2), async {
        loop {
            let messages = get_json(app.clone(), &format!("/session/{session_id}/message")).await;
            if messages.as_array().expect("messages").len() == 2 {
                return messages;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("async prompt completion");

    assert_eq!(messages[0]["info"]["role"], "user");
    assert_eq!(messages[1]["info"]["role"], "assistant");
    assert_eq!(messages[1]["parts"][0]["text"], "echo: background");
    assert!(messages[1]["parts"][0]["time"]["end"].is_number());
}

#[tokio::test]
async fn live_message_events_keep_opencode_binary_search_order() {
    let harness = Harness::new();
    let state = AppState::new(harness.config());
    let mut events = state.subscribe_events();
    let app = app_with_state(state);

    let session = create_session(app.clone()).await;
    let session_id = session["id"].as_str().expect("session id");
    let _created_event = next_event(&mut events, "session.updated").await;

    prompt(app.clone(), session_id, "first live turn").await;
    prompt(app, session_id, "second live turn").await;

    let mut message_infos = Vec::new();
    while message_infos.len() < 4 {
        let event = next_event(&mut events, "message.updated").await;
        if event["properties"]["sessionID"] == session_id {
            message_infos.push(event["properties"]["info"].clone());
        }
    }

    assert_eq!(
        message_infos
            .iter()
            .map(|info| info["role"].as_str().expect("role"))
            .collect::<Vec<_>>(),
        ["user", "assistant", "user", "assistant"]
    );

    let mut live_inserted = Vec::<Value>::new();
    for info in message_infos {
        let id = info["id"].as_str().expect("message id");
        let index = live_inserted
            .binary_search_by(|message| message["id"].as_str().expect("message id").cmp(id))
            .unwrap_or_else(|index| index);
        live_inserted.insert(index, info);
    }

    assert_eq!(
        live_inserted
            .iter()
            .map(|info| info["role"].as_str().expect("role"))
            .collect::<Vec<_>>(),
        ["user", "assistant", "user", "assistant"],
        "opencode inserts live message.updated events by id, so ids must sort chronologically"
    );
}

#[tokio::test]
async fn assistant_events_sort_after_tui_supplied_user_message_ids() {
    let harness = Harness::new();
    let state = AppState::new(harness.config());
    let mut events = state.subscribe_events();
    let app = app_with_state(state);

    let session = create_session(app.clone()).await;
    let session_id = session["id"].as_str().expect("session id");
    let _created_event = next_event(&mut events, "session.updated").await;

    let user_message_id = high_suffix_message_id();
    prompt_with_message_id(app, session_id, "client supplied id", &user_message_id).await;

    let mut message_infos = Vec::new();
    while message_infos.len() < 2 {
        let event = next_event(&mut events, "message.updated").await;
        if event["properties"]["sessionID"] == session_id {
            message_infos.push(event["properties"]["info"].clone());
        }
    }

    let mut live_inserted = Vec::<Value>::new();
    for info in message_infos {
        let id = info["id"].as_str().expect("message id");
        let index = live_inserted
            .binary_search_by(|message| message["id"].as_str().expect("message id").cmp(id))
            .unwrap_or_else(|index| index);
        live_inserted.insert(index, info);
    }

    assert_eq!(
        live_inserted
            .iter()
            .map(|info| info["role"].as_str().expect("role"))
            .collect::<Vec<_>>(),
        ["user", "assistant"],
        "assistant message ids must sort after TUI supplied parent ids"
    );
}

#[tokio::test]
async fn pi_rpc_semantic_stream_is_translated_to_native_opencode_parts() {
    let harness = Harness::new();
    let state = AppState::new(harness.config());
    let mut events = state.subscribe_events();
    let app = app_with_state(state);

    let session = create_session(app.clone()).await;
    let session_id = session["id"].as_str().expect("session id");
    let _created_event = next_event(&mut events, "session.updated").await;

    let assistant = prompt(app.clone(), session_id, "stream events").await;
    assert_eq!(assistant["info"]["role"], "assistant");

    let mut saw_reasoning_update = false;
    let mut saw_reasoning_delta = false;
    let mut saw_tool_running = false;
    let mut saw_tool_completed = false;
    let mut saw_text_delta = false;

    timeout(Duration::from_secs(2), async {
        while !(saw_reasoning_update
            && saw_reasoning_delta
            && saw_tool_running
            && saw_tool_completed
            && saw_text_delta)
        {
            let event = events.recv().await.expect("event");
            let payload = &event["payload"];
            if payload["properties"]["sessionID"] != session_id {
                continue;
            }
            match payload["type"].as_str() {
                Some("message.part.updated") => {
                    let part = &payload["properties"]["part"];
                    match part["type"].as_str() {
                        Some("reasoning") => {
                            saw_reasoning_update = true;
                        }
                        Some("tool") => match part["state"]["status"].as_str() {
                            Some("running") => {
                                assert_eq!(part["tool"], "bash");
                                assert_eq!(part["state"]["input"]["cmd"], "printf hi");
                                saw_tool_running = true;
                            }
                            Some("completed") => {
                                assert_eq!(part["state"]["output"], "hi");
                                saw_tool_completed = true;
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
                Some("message.part.delta") => {
                    if payload["properties"]["field"] == "text" {
                        match payload["properties"]["delta"].as_str() {
                            Some("thinking") => saw_reasoning_delta = true,
                            Some("streamed answer") => saw_text_delta = true,
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .expect("translated stream events");

    let messages = get_json(app, &format!("/session/{session_id}/message")).await;
    let assistant = &messages.as_array().expect("messages")[1];
    assert!(
        assistant["parts"]
            .as_array()
            .expect("assistant parts")
            .iter()
            .any(|part| part["type"] == "reasoning" && part["text"] == "thinking")
    );
    assert!(
        assistant["parts"]
            .as_array()
            .expect("assistant parts")
            .iter()
            .any(|part| part["type"] == "tool"
                && part["tool"] == "bash"
                && part["state"]["status"] == "completed")
    );
}

#[tokio::test]
async fn event_routes_match_opencode_instance_and_global_shapes() {
    let harness = Harness::new();
    let (base_url, server) = spawn_test_server(app(harness.config())).await;
    let client = reqwest::Client::new();

    let mut instance_events = client
        .get(format!("{base_url}/event"))
        .send()
        .await
        .expect("GET /event");
    assert_eq!(instance_events.status(), reqwest::StatusCode::OK);

    let mut global_events = client
        .get(format!("{base_url}/global/event"))
        .send()
        .await
        .expect("GET /global/event");
    assert_eq!(global_events.status(), reqwest::StatusCode::OK);

    let global_connected = next_sse_json(&mut global_events, |value| {
        value["payload"]["type"] == "server.connected"
    })
    .await;
    assert_eq!(global_connected["payload"]["type"], "server.connected");
    assert!(
        global_connected.get("directory").is_none(),
        "global server.connected must stay global so desktop refreshes global project state"
    );

    let session = client
        .post(format!("{base_url}/session"))
        .send()
        .await
        .expect("POST /session");
    assert_eq!(session.status(), reqwest::StatusCode::OK);

    let instance_event = next_sse_json(&mut instance_events, |value| {
        value["type"] == "session.updated"
    })
    .await;
    assert_eq!(instance_event["type"], "session.updated");
    assert!(
        instance_event.get("payload").is_none(),
        "/event must stream raw instance events for opencode run"
    );

    let global_event = next_sse_json(&mut global_events, |value| {
        value["payload"]["type"] == "session.updated"
    })
    .await;
    assert_eq!(global_event["payload"]["type"], "session.updated");
    assert!(global_event["directory"].is_string());
    assert!(global_event["project"].is_string());

    server.abort();
}

#[tokio::test]
async fn opencode_run_attach_streams_assistant_text() {
    if Command::new("opencode")
        .arg("--version")
        .output()
        .await
        .is_err()
    {
        eprintln!("Skipping opencode CLI smoke test: opencode not found in PATH");
        return;
    }

    let harness = Harness::new();
    let (base_url, mut server) = spawn_binary_server(&harness).await;
    let output = timeout(
        Duration::from_secs(15),
        Command::new("opencode")
            .args([
                "run",
                "--attach",
                &base_url,
                "--format",
                "json",
                "attach smoke",
            ])
            .output(),
    )
    .await
    .expect("opencode run should finish")
    .expect("opencode run should execute");

    let _ = server.kill().await;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "opencode run failed\nstdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("attach smoke"),
        "opencode run should stream assistant text\nstdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
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

async fn prompt(app: axum::Router, session_id: &str, text: &str) -> Value {
    post_json(
        app,
        &format!("/session/{session_id}/message"),
        json!({ "parts": [{ "type": "text", "text": text }] }),
        StatusCode::OK,
    )
    .await
}

async fn prompt_with_message_id(
    app: axum::Router,
    session_id: &str,
    text: &str,
    message_id: &str,
) -> Value {
    post_json(
        app,
        &format!("/session/{session_id}/message"),
        json!({
            "messageID": message_id,
            "parts": [{ "type": "text", "text": text }],
        }),
        StatusCode::OK,
    )
    .await
}

fn high_suffix_message_id() -> String {
    let id = ids::message_id();
    let body = id.strip_prefix("msg_").expect("message id prefix");
    let time = body.get(..12).expect("message id time prefix");
    format!("msg_{time}zzzzzzzzzzzzzz")
}

async fn get_json(app: axum::Router, uri: &str) -> Value {
    let response = app
        .oneshot(request(Method::GET, uri, Body::empty()))
        .await
        .expect("GET route");
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

async fn post_json(app: axum::Router, uri: &str, body: Value, status: StatusCode) -> Value {
    json_request(app, Method::POST, uri, body, status).await
}

async fn patch_json(app: axum::Router, uri: &str, body: Value) -> Value {
    json_request(app, Method::PATCH, uri, body, StatusCode::OK).await
}

async fn delete_json(app: axum::Router, uri: &str, status: StatusCode) -> Value {
    json_request(app, Method::DELETE, uri, json!({}), status).await
}

async fn json_request(
    app: axum::Router,
    method: Method,
    uri: &str,
    body: Value,
    status: StatusCode,
) -> Value {
    let response = app
        .oneshot(request(method, uri, Body::from(body.to_string())))
        .await
        .expect("JSON request");
    assert_eq!(response.status(), status);
    response_json(response).await
}

async fn next_event(rx: &mut broadcast::Receiver<Value>, event_type: &str) -> Value {
    timeout(Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.expect("event");
            if value["payload"]["type"] == event_type {
                return value["payload"].clone();
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for event {event_type}"))
}

async fn next_wrapped_event(rx: &mut broadcast::Receiver<Value>, event_type: &str) -> Value {
    timeout(Duration::from_secs(2), async {
        loop {
            let value = rx.recv().await.expect("event");
            if value["payload"]["type"] == event_type {
                return value;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for wrapped event {event_type}"))
}

fn encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
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

async fn spawn_test_server(app: axum::Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("test server addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("test server");
    });
    (format!("http://{addr}"), server)
}

async fn spawn_binary_server(harness: &Harness) -> (String, Child) {
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let binary = std::env::var("CARGO_BIN_EXE_pi-server")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/pi-server")
        });
    let mut child = Command::new(binary)
        .args([
            "--hostname",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--pi-bin",
            harness.fake_pi.to_str().expect("fake pi path"),
            "--directory",
            harness.workdir.to_str().expect("workdir path"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn pi-server binary");

    let client = reqwest::Client::new();
    let ready = timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("{base_url}/global/health"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            assert!(
                child.try_wait().expect("poll pi-server").is_none(),
                "pi-server exited before becoming ready"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if ready.is_err() {
        let _ = child.kill().await;
        panic!("timed out waiting for pi-server binary");
    }

    (base_url, child)
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free port addr").port()
}

async fn next_sse_json(
    response: &mut reqwest::Response,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    timeout(Duration::from_secs(2), async {
        let mut buffer = String::new();
        loop {
            let chunk = response
                .chunk()
                .await
                .expect("SSE chunk")
                .expect("SSE stream ended");
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(frame_end) = buffer.find("\n\n") {
                let frame = buffer.drain(..frame_end + 2).collect::<String>();
                for line in frame.lines() {
                    let Some(data) = line.strip_prefix("data:") else {
                        continue;
                    };
                    let value: Value = serde_json::from_str(data.trim()).expect("SSE JSON event");
                    if predicate(&value) {
                        return value;
                    }
                }
            }
        }
    })
    .await
    .expect("timed out waiting for matching SSE event")
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

if sys.argv[1:] != ["--mode", "rpc"]:
    print("expected --mode rpc, got " + repr(sys.argv[1:]), file=sys.stderr, flush=True)
    sys.exit(2)

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
        if message == "stream events":
            assistant = {
                "role": "assistant",
                "content": [],
                "api": "test",
                "provider": "test",
                "model": "test",
                "usage": {},
                "stopReason": "stop",
                "timestamp": 0
            }
            print(json.dumps({"type": "message_start", "message": assistant}), flush=True)
            print(json.dumps({
                "type": "message_update",
                "message": assistant,
                "assistantMessageEvent": {
                    "type": "thinking_start",
                    "contentIndex": 0,
                    "partial": {**assistant, "content": [{"type": "thinking", "thinking": ""}]}
                }
            }), flush=True)
            print(json.dumps({
                "type": "message_update",
                "message": assistant,
                "assistantMessageEvent": {
                    "type": "thinking_delta",
                    "contentIndex": 0,
                    "delta": "thinking",
                    "partial": {**assistant, "content": [{"type": "thinking", "thinking": "thinking"}]}
                }
            }), flush=True)
            print(json.dumps({
                "type": "message_update",
                "message": assistant,
                "assistantMessageEvent": {
                    "type": "thinking_end",
                    "contentIndex": 0,
                    "content": "thinking",
                    "partial": {**assistant, "content": [{"type": "thinking", "thinking": "thinking"}]}
                }
            }), flush=True)
            print(json.dumps({
                "type": "message_update",
                "message": assistant,
                "assistantMessageEvent": {
                    "type": "toolcall_end",
                    "contentIndex": 1,
                    "toolCall": {"id": "tool-1", "name": "bash", "arguments": {"cmd": "printf hi"}},
                    "partial": assistant
                }
            }), flush=True)
            print(json.dumps({"type": "tool_execution_start", "toolCallId": "tool-1", "toolName": "bash", "args": {"cmd": "printf hi"}}), flush=True)
            print(json.dumps({
                "type": "tool_execution_end",
                "toolCallId": "tool-1",
                "toolName": "bash",
                "isError": False,
                "result": {"content": [{"type": "text", "text": "hi"}], "details": None, "isError": False}
            }), flush=True)
            print(json.dumps({
                "type": "message_update",
                "message": assistant,
                "assistantMessageEvent": {
                    "type": "text_start",
                    "contentIndex": 2,
                    "partial": assistant
                }
            }), flush=True)
            print(json.dumps({
                "type": "message_update",
                "message": assistant,
                "assistantMessageEvent": {
                    "type": "text_delta",
                    "contentIndex": 2,
                    "delta": "streamed answer",
                    "partial": {**assistant, "content": [{"type": "text", "text": "streamed answer"}]}
                }
            }), flush=True)
            print(json.dumps({
                "type": "message_update",
                "message": assistant,
                "assistantMessageEvent": {
                    "type": "text_end",
                    "contentIndex": 2,
                    "content": "streamed answer",
                    "partial": {**assistant, "content": [{"type": "text", "text": "streamed answer"}]}
                }
            }), flush=True)
            print(json.dumps({
                "type": "agent_end",
                "messages": [{**assistant, "content": [{"type": "text", "text": "streamed answer"}]}],
                "error": None
            }), flush=True)
            continue
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
