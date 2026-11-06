use super::*;
use crate::mcp_providers::McpProviderRequest;

#[test]
fn acp_unavailable_provider_reports_identity_without_mutating_desired_config() {
    let dir = crate::coding_agents::tests::Scratch::new();
    let program = dir.0.join("optional-agent.exe");
    std::fs::write(&program, b"fixture").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut acp = CodingAgentStore::load(&dir.0);
    let mut request = crate::coding_agents::tests::request("missing-agent", 0);
    request.profile.executable = program.to_string_lossy().into_owned();
    acp.stage_update(request).unwrap();
    acp.commit().unwrap();
    let original = std::fs::read(dir.0.join("coding-agents.json")).unwrap();
    std::fs::remove_file(program).unwrap();
    let error = preflight_providers(
        None,
        &McpProviderStore::load(&dir.0),
        &CodingAgentStore::load(&dir.0),
    )
    .unwrap_err();
    assert_eq!(error.code, "coding_agent_executable_unavailable");
    assert_eq!(
        error.details.as_ref().unwrap()["provider_id"],
        "missing-agent"
    );
    assert_eq!(
        std::fs::read(dir.0.join("coding-agents.json")).unwrap(),
        original
    );
}

#[test]
fn enabled_streamable_http_mcp_does_not_require_a_local_executable() {
    let dir = crate::coding_agents::tests::Scratch::new();
    let mut mcp = McpProviderStore::load(&dir.0);
    mcp.update(McpProviderRequest {
        id: None,
        expected_revision: 0,
        name: "Penpot".into(),
        transport: ProviderTransport::StreamableHttp,
        command: String::new(),
        args: Vec::new(),
        cwd: None,
        url: Some("https://design.example/mcp/stream".into()),
        http_headers: std::collections::BTreeMap::new(),
        http_query_params: std::collections::BTreeMap::new(),
        enabled: true,
        env: std::collections::BTreeMap::new(),
    })
    .unwrap();

    preflight_providers(None, &mcp, &CodingAgentStore::load(&dir.0)).unwrap();
}

#[test]
fn operator_owned_streamable_http_mcp_does_not_require_an_executable() {
    let dir = crate::coding_agents::tests::Scratch::new();
    let path = dir.0.join("runner.toml");
    let runtime = StoredRuntime {
        server_url: "http://127.0.0.1:1".into(),
        runner_client_id: Some("fixture".into()),
        runner_config: Some(path.clone()),
        server_env_file: None,
        user_token_file: None,
        project_id: None,
        runtime_project_id: None,
    };
    let original = "client_id = 'fixture'\nserver_url = 'http://127.0.0.1:1'\nmcp = { providers = [{ id = 'operator-http', name = 'Remote MCP', transport = 'streamable_http', url = 'https://example.com/mcp', http_headers_from_env = {}, http_query_from_env = {}, env_from_env = {} }] }\n";
    std::fs::write(&path, original).unwrap();

    preflight_providers(
        Some(&runtime),
        &McpProviderStore::load(&dir.0),
        &CodingAgentStore::load(&dir.0),
    )
    .unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
}

#[test]
fn operator_owned_mcp_and_inline_acp_are_checked_without_rewriting_toml() {
    let dir = crate::coding_agents::tests::Scratch::new();
    let path = dir.0.join("runner.toml");
    let runtime = StoredRuntime {
        server_url: "http://127.0.0.1:1".into(),
        runner_client_id: Some("fixture".into()),
        runner_config: Some(path.clone()),
        server_env_file: None,
        user_token_file: None,
        project_id: None,
        runtime_project_id: None,
    };
    for (section, entries, kind, code) in [
        (
            "mcp",
            "providers",
            "mcp",
            "mcp_provider_executable_unavailable",
        ),
        (
            "acp",
            "agents",
            "acp",
            "coding_agent_executable_unavailable",
        ),
    ] {
        let original=format!("client_id = 'fixture'\nserver_url = 'http://127.0.0.1:1'\n{section} = {{ {entries} = [{{ id = 'operator', name = 'Operator tool', executable = 'webcodex-fixture-program-does-not-exist' }}] }}\n");
        std::fs::write(&path, &original).unwrap();
        let failure = preflight_providers(
            Some(&runtime),
            &McpProviderStore::load(&dir.0),
            &CodingAgentStore::load(&dir.0),
        )
        .unwrap_err();
        assert_eq!(failure.code, code);
        let details = failure.details.unwrap();
        assert_eq!(details["provider_kind"], kind);
        assert_eq!(
            details["provider_config_path"],
            path.to_string_lossy().as_ref()
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
    }
}
