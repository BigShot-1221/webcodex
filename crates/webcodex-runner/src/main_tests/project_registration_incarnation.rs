use super::*;

#[test]
fn explicit_register_mints_stable_registration_incarnation() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("repo");
    let project_registry_dir = tmp.path().join("project-registry");
    std::fs::create_dir(&project_dir).unwrap();
    let policy = project_policy(tmp.path());
    let req = project_request(
        "register_project",
        serde_json::json!({
            "id": "demo",
            "name": "Demo",
            "path": project_dir.to_string_lossy(),
            "allow_patch": true
        }),
    );

    let first = project_ok(handle_project_op(&policy, &project_registry_dir, &req));
    let incarnation = first["registration_incarnation"]
        .as_str()
        .expect("registration response must include registration_incarnation");
    assert!(valid_registration_incarnation(incarnation));

    let retry = project_ok(handle_project_op(&policy, &project_registry_dir, &req));
    assert_eq!(retry["recovered"], true);
    assert_eq!(retry["registration_incarnation"], incarnation);

    let summaries = load_runner_project_summaries_from_dir(&project_registry_dir);
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries[0].registration_incarnation.as_deref(),
        Some(incarnation)
    );
    let persisted =
        parse_runner_project_toml(&std::fs::read_to_string(project_registry_dir.join("demo.toml")).unwrap())
            .unwrap();
    assert_eq!(persisted.registration_incarnation.as_deref(), Some(incarnation));
}

#[test]
fn unregister_then_register_mints_fresh_registration_incarnation() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("repo");
    let project_registry_dir = tmp.path().join("project-registry");
    std::fs::create_dir(&project_dir).unwrap();
    let policy = project_policy(tmp.path());
    let register = project_request(
        "register_project",
        serde_json::json!({
            "id": "demo",
            "name": "Demo",
            "path": project_dir.to_string_lossy(),
            "allow_patch": true
        }),
    );
    let first = project_ok(handle_project_op(&policy, &project_registry_dir, &register));
    let first_incarnation = first["registration_incarnation"]
        .as_str()
        .expect("registration_incarnation");
    let revision = first["revision"].as_str().expect("revision");

    let disabled = project_ok(handle_project_lifecycle_op(
        &policy,
        &project_registry_dir,
        &project_request(
            "project_lifecycle_disable",
            serde_json::json!({"project_id": "demo", "expected_revision": revision}),
        ),
    ));
    assert_eq!(disabled["outcome"], "disabled");
    assert_eq!(disabled["registration_incarnation"], first_incarnation);

    let disabled_revision = disabled["revision"].as_str().expect("revision");
    let unregistered = project_ok(handle_project_lifecycle_op(
        &policy,
        &project_registry_dir,
        &project_request(
            "project_lifecycle_unregister",
            serde_json::json!({
                "project_id": "demo",
                "expected_revision": disabled_revision
            }),
        ),
    ));
    assert_eq!(unregistered["outcome"], "unregistered");
    assert!(!project_registry_dir.join("demo.toml").exists());

    let second = project_ok(handle_project_op(&policy, &project_registry_dir, &register));
    let second_incarnation = second["registration_incarnation"]
        .as_str()
        .expect("registration_incarnation");
    assert_ne!(first_incarnation, second_incarnation);
}

#[test]
fn auto_path_register_mints_and_preserves_registration_incarnation() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("repo");
    let project_registry_dir = tmp.path().join("project-registry");
    std::fs::create_dir(&project_dir).unwrap();
    let policy = project_policy(tmp.path());

    let first = project_ok(handle_resolve_or_register_project(
        &policy,
        &project_registry_dir,
        &project_request(
            "resolve_or_register_project",
            serde_json::json!({"path": project_dir.to_string_lossy()}),
        ),
    ));
    let incarnation = first["registration_incarnation"]
        .as_str()
        .expect("registration_incarnation");
    assert!(valid_registration_incarnation(incarnation));

    let second = project_ok(handle_resolve_or_register_project(
        &policy,
        &project_registry_dir,
        &project_request(
            "resolve_or_register_project",
            serde_json::json!({"path": project_dir.to_string_lossy()}),
        ),
    ));
    assert_eq!(second["outcome"], "reused_existing_registration");
    assert_eq!(second["registration_incarnation"], incarnation);
}

#[test]
fn legacy_registry_record_migrates_incarnation_once_before_inventory_advertises_it() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("repo");
    let project_registry_dir = tmp.path().join("project-registry");
    std::fs::create_dir(&project_dir).unwrap();
    std::fs::create_dir(&project_registry_dir).unwrap();
    let legacy = format!(
        "id = \"legacy\"\nname = \"Legacy\"\npath = {:?}\nallow_patch = true\n\n[hooks]\nprecommit = [\"cargo test\"]\n",
        project_dir.to_string_lossy()
    );
    std::fs::write(project_registry_dir.join("legacy.toml"), &legacy).unwrap();

    let summaries = load_runner_project_summaries_from_dir(&project_registry_dir);
    assert_eq!(summaries.len(), 1);
    let incarnation = summaries[0]
        .registration_incarnation
        .as_deref()
        .expect("legacy record must be upgraded before inventory");
    assert!(valid_registration_incarnation(incarnation));

    let migrated_text = std::fs::read_to_string(project_registry_dir.join("legacy.toml")).unwrap();
    assert!(migrated_text.contains("registration_incarnation"));
    assert!(migrated_text.contains("[hooks]"));

    let summaries_retry = load_runner_project_summaries_from_dir(&project_registry_dir);
    assert_eq!(
        summaries_retry[0].registration_incarnation.as_deref(),
        Some(incarnation)
    );
}

#[cfg(unix)]
#[test]
fn symlinked_project_registry_config_is_not_upgraded_or_advertised() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("repo");
    let project_registry_dir = tmp.path().join("project-registry");
    let real_config = tmp.path().join("real-config");
    std::fs::create_dir_all(&project_dir).unwrap();
    std::fs::create_dir_all(&project_registry_dir).unwrap();
    std::fs::create_dir(&real_config).unwrap();
    let legacy = format!(
        "id = \"legacy\"\npath = {:?}\n",
        project_dir.to_string_lossy()
    );
    std::fs::write(real_config.join("legacy.toml"), &legacy).unwrap();
    symlink(real_config.join("legacy.toml"), project_registry_dir.join("legacy.toml")).unwrap();

    let summaries = load_runner_project_summaries_from_dir(&project_registry_dir);
    assert!(summaries.is_empty());
    let target = std::fs::read_to_string(real_config.join("legacy.toml")).unwrap();
    assert!(!target.contains("registration_incarnation"));
}
