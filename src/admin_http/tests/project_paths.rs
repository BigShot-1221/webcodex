//! Admin register/create path ingress: cross-platform absolute shapes only.
//! Passing cases stop at Runner lookup (`agent_unavailable`); they do not assert
//! filesystem acceptance or Windows end-to-end registration success.

use super::service;
use crate::auth::scopes::SCOPE_ADMIN;
use crate::auth::{AuthContext, AuthKind};
use salvo::http::StatusCode;
use salvo::test::{ResponseExt, TestClient};
use serde_json::{json, Value};

fn admin_auth() -> AuthContext {
    let mut auth = AuthContext::new(AuthKind::ApiToken);
    auth.scopes.push(SCOPE_ADMIN.to_string());
    auth
}

fn base_body(path: &str, idempotency_key: &str) -> Value {
    json!({
        "client_id": "path-client",
        "project_id": "demo",
        "name": "Demo",
        "path": path,
        "allow_patch": true,
        "idempotency_key": idempotency_key,
    })
}

async fn post_admin_project(route: &str, body: Value) -> (StatusCode, Value) {
    let service = service(Some(admin_auth()));
    let mut response = TestClient::post(format!("http://127.0.0.1/admin/projects/{route}"))
        .add_header("host", "127.0.0.1", true)
        .add_header("origin", "http://127.0.0.1", true)
        .add_header("content-type", "application/json", true)
        .body(body.to_string())
        .send(&service)
        .await;
    let status = response.status_code.unwrap();
    let body = response.take_json::<Value>().await.unwrap();
    (status, body)
}

async fn assert_path_rejected(route: &str, path: &str, label: &str) {
    let body = base_body(path, label);
    let (status, json) = post_admin_project(route, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{label} on {route}");
    assert_eq!(json["error"]["code"], "invalid_request", "{label} on {route}");
}

async fn assert_path_reaches_runner_lookup(route: &str, path: &str, label: &str) {
    let body = base_body(path, label);
    let (status, json) = post_admin_project(route, body).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{label} on {route}: expected Runner lookup after path validation"
    );
    assert_eq!(
        json["error"]["code"], "agent_unavailable",
        "{label} on {route}"
    );
}

#[tokio::test]
async fn admin_register_rejects_invalid_project_paths() {
    let overlong = format!("/{}", "x".repeat(4096));
    for (path, label) in [
        ("", "empty"),
        ("relative/path", "relative-posix"),
        (r"C:repo", "drive-relative"),
        ("D:", "bare-drive"),
        (r"relative\path", "relative-windows"),
        (r"\repo", "root-relative"),
        ("/root/git/\0", "nul"),
        (overlong.as_str(), "overlong"),
    ] {
        assert_path_rejected("register", path, label).await;
    }
}

#[tokio::test]
async fn admin_create_rejects_invalid_project_paths() {
    let overlong = format!("/{}", "x".repeat(4096));
    for (path, label) in [
        ("", "empty"),
        ("relative/path", "relative-posix"),
        (r"C:repo", "drive-relative"),
        ("D:", "bare-drive"),
        (r"relative\path", "relative-windows"),
        (r"\repo", "root-relative"),
        ("/root/git/\0", "nul"),
        (overlong.as_str(), "overlong"),
    ] {
        assert_path_rejected("create", path, label).await;
    }
}

#[tokio::test]
async fn admin_register_accepts_cross_platform_absolute_paths() {
    let max_length = format!("/{}", "a".repeat(4095));
    for (path, label) in [
        ("/root/git/my-project", "posix"),
        (r"C:\repo", "windows-drive"),
        ("c:/repo", "windows-drive-forward-slash"),
        (r"\\?\C:\repo", "verbatim"),
        (r"\\server\share\repo", "unc"),
        (r"\\?\UNC\server\share\repo", "verbatim-unc"),
        ("//?/D:/projects/demo", "verbatim-drive-forward-slash"),
        (max_length.as_str(), "max-length"),
    ] {
        assert_path_reaches_runner_lookup("register", path, label).await;
    }
}

#[tokio::test]
async fn admin_create_accepts_cross_platform_absolute_paths() {
    let max_length = format!("/{}", "a".repeat(4095));
    for (path, label) in [
        ("/root/git/my-project", "posix"),
        (r"C:\repo", "windows-drive"),
        ("c:/repo", "windows-drive-forward-slash"),
        (r"\\?\C:\repo", "verbatim"),
        (r"\\server\share\repo", "unc"),
        (r"\\?\UNC\server\share\repo", "verbatim-unc"),
        ("//?/D:/projects/demo", "verbatim-drive-forward-slash"),
        (max_length.as_str(), "max-length"),
    ] {
        assert_path_reaches_runner_lookup("create", path, label).await;
    }
}
