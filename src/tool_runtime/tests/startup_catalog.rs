//! Characterization of the startup-only catalog projection, not resource authority.

use crate::tool_runtime::startup_catalog::{
    bounded_extension_description, StartupExtensions, StartupPluginEntry, StartupPluginsCatalog,
    StartupSkillEntry, StartupSkillsCatalog, STARTUP_EXTENSION_CATALOG_HARD_MAX_BYTES,
    STARTUP_PLUGIN_CATALOG_MAX_BYTES, STARTUP_SKILL_CATALOG_MAX_BYTES,
};
use crate::tool_runtime::surface::discovery::TaskRelevance;
use serde_json::{json, Value};

const SKILL_HINT: &str = "Use skills.catalog or list_skills for broader or refreshed discovery.";
const PLUGIN_HINT: &str = "Use plugins.catalog or explicit plugin_tool list and describe for broader or current schema discovery.";

// Independent JSON oracle for the original greedy-prefix wire contract. Keep
// the omitted hint and upstream-incomplete cases explicit: they affect bytes.
fn reference_catalog(
    revision: &str,
    entries: &[Value],
    total_count: usize,
    upstream_truncated: bool,
    max_bytes: usize,
    hint: &str,
) -> Value {
    let project = |returned: &[Value]| {
        let truncated = upstream_truncated || returned.len() < total_count;
        let mut value = json!({
            "status": "available",
            "catalog_revision": revision,
            "total_count": total_count,
            "returned_count": returned.len(),
            "truncated": truncated,
            "entries": returned,
        });
        if truncated {
            value["discovery_hint"] = json!(hint);
        }
        value
    };
    let mut returned = Vec::new();
    for entry in entries {
        let mut candidate = returned.clone();
        candidate.push(entry.clone());
        if serde_json::to_vec(&project(&candidate)).unwrap().len() > max_bytes {
            break;
        }
        returned = candidate;
    }
    project(&returned)
}

fn skill(index: usize, description: &str) -> StartupSkillEntry {
    StartupSkillEntry {
        skill_id: format!(
            "wc_skill_{}",
            webcodex_core::compact::encode(&(index as u128).to_be_bytes()[0..])
        ),
        name: format!("skill-{index}"),
        description: description.to_string(),
        source_scope: if index % 2 == 0 { "project" } else { "runner" }.to_string(),
        trust: if index % 2 == 0 {
            "project_content"
        } else {
            "operator_installed_guidance"
        }
        .to_string(),
        name_conflict: index % 3 == 0,
    }
}

fn plugin(index: usize, description: &str) -> StartupPluginEntry {
    StartupPluginEntry {
        plugin: format!("provider-{index}"),
        name: format!("Provider {index}"),
        tool: format!("tool-{index}"),
        title: (index % 2 == 0).then(|| format!("Tool {index}")),
        description: (index % 3 != 0).then(|| description.to_string()),
        annotations: Default::default(),
    }
}

#[test]
fn startup_catalog_skill_projection_matches_original_prefix_contract() {
    let revision = "wc_skillcat_qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo".to_string();
    for count in [0, 1, 2, 8, 64] {
        for width in [0, 32, 180, 512, 3000] {
            let description = "\u{8d44}\u{6e90}\"\\\n".repeat(width);
            let entries: Vec<_> = (0..count).map(|i| skill(i, &description)).collect();
            let values: Vec<_> = entries.iter().map(|entry| json!(entry)).collect();
            for upstream_truncated in [false, true] {
                let actual = StartupSkillsCatalog::available(
                    revision.clone(),
                    upstream_truncated,
                    entries.clone(),
                );
                assert_eq!(
                    json!(actual),
                    reference_catalog(
                        &revision,
                        &values,
                        count,
                        upstream_truncated,
                        STARTUP_SKILL_CATALOG_MAX_BYTES,
                        SKILL_HINT,
                    ),
                    "count={count}, width={width}, upstream={upstream_truncated}",
                );
                assert!(
                    serde_json::to_vec(&actual).unwrap().len() <= STARTUP_SKILL_CATALOG_MAX_BYTES
                );
            }
        }
    }
}

#[test]
fn startup_catalog_plugin_projection_preserves_provider_total_and_optional_fields() {
    let revision = format!("wc_plugcat_{}", webcodex_core::compact::encode([0xbb; 32]));
    for count in [0, 1, 2, 8, 64] {
        for width in [0, 32, 180, 512, 3000] {
            let description = "\u{63d2}\u{4ef6}\"\\\n".repeat(width);
            let entries: Vec<_> = (0..count).map(|i| plugin(i, &description)).collect();
            let values: Vec<_> = entries.iter().map(|entry| json!(entry)).collect();
            for total_count in [count, count + 7] {
                let actual = StartupPluginsCatalog::available(
                    revision.clone(),
                    total_count,
                    entries.clone(),
                );
                assert_eq!(
                    json!(actual),
                    reference_catalog(
                        &revision,
                        &values,
                        total_count,
                        false,
                        STARTUP_PLUGIN_CATALOG_MAX_BYTES,
                        PLUGIN_HINT,
                    ),
                    "count={count}, width={width}, total={total_count}",
                );
                assert!(
                    serde_json::to_vec(&actual).unwrap().len() <= STARTUP_PLUGIN_CATALOG_MAX_BYTES
                );
            }
        }
    }
}

#[test]
fn startup_catalog_unavailable_is_not_a_successfully_empty_catalog() {
    for (value, reason, hint) in [
        (
            json!(StartupSkillsCatalog::unavailable(
                "skills_catalog_unavailable"
            )),
            "skills_catalog_unavailable",
            "Use skills.catalog or list_skills for explicit discovery when available.",
        ),
        (
            json!(StartupPluginsCatalog::unavailable(
                "plugin_runtime_unavailable"
            )),
            "plugin_runtime_unavailable",
            "Use explicit plugin_tool list and describe when Plugin discovery is available.",
        ),
    ] {
        assert_eq!(
            value,
            json!({
                "status": "unavailable",
                "reason_code": reason,
                "total_count": 0,
                "returned_count": 0,
                "truncated": false,
                "entries": [],
                "discovery_hint": hint,
            })
        );
    }
}

#[test]
fn startup_catalog_combined_budget_and_utf8_description_remain_bounded() {
    let description = bounded_extension_description(&"\u{8d44}\u{6e90}\"\\\n".repeat(200));
    assert!(description.len() <= 512);
    let extensions = StartupExtensions {
        skills: StartupSkillsCatalog::available(
            "wc_skillcat_qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo".to_string(),
            false,
            (0..64).map(|i| skill(i, &description)).collect(),
        ),
        plugins: StartupPluginsCatalog::available(
            format!("wc_plugcat_{}", webcodex_core::compact::encode([0xbb; 32])),
            64,
            (0..64).map(|i| plugin(i, &description)).collect(),
        ),
    };
    assert!(extensions.serialized_len() <= STARTUP_EXTENSION_CATALOG_HARD_MAX_BYTES);
    assert!(extensions.skills.truncated);
    assert!(extensions.plugins.truncated);
}

#[test]
fn startup_catalog_task_relevance_preserves_late_skills_and_plugins_before_budgeting() {
    let description = "Unrelated capability metadata. ".repeat(15);
    let mut skills: Vec<_> = (0..20).map(|i| skill(i, &description)).collect();
    skills[19].name = "quartz-audit".to_string();
    let mut plugins: Vec<_> = (0..20).map(|i| plugin(i, &description)).collect();
    plugins[19].title = Some("Quartz audit".to_string());
    let query = TaskRelevance::new("Perform the quartz audit");
    let original_skills =
        StartupSkillsCatalog::available("revision".to_string(), true, skills.clone());
    let original_plugins =
        StartupPluginsCatalog::available("revision".to_string(), 27, plugins.clone());
    assert!(!original_skills
        .entries
        .iter()
        .any(|entry| entry.name == "quartz-audit"));
    assert!(!original_plugins
        .entries
        .iter()
        .any(|entry| entry.title.as_deref() == Some("Quartz audit")));
    let ranked = StartupExtensions {
        skills: StartupSkillsCatalog::available_for_task(
            "revision".to_string(),
            true,
            skills,
            &query,
        ),
        plugins: StartupPluginsCatalog::available_for_task(
            "revision".to_string(),
            27,
            plugins,
            &query,
        ),
    };
    assert_eq!(ranked.skills.entries[0].name, "quartz-audit");
    assert_eq!(ranked.plugins.entries[0].tool, "tool-19");
    // A relevant row is promoted, not the only row retained. Unmatched rows
    // keep discovery order; provider totals and source revisions stay canonical.
    assert_eq!(ranked.skills.entries[1].name, "skill-0");
    assert_eq!(ranked.plugins.entries[1].tool, "tool-0");
    assert_eq!(ranked.skills.total_count, 20);
    assert_eq!(ranked.plugins.total_count, 27);
    assert_eq!(ranked.skills.catalog_revision.as_deref(), Some("revision"));
    assert_eq!(ranked.plugins.catalog_revision.as_deref(), Some("revision"));
    assert!(ranked.skills.truncated && ranked.plugins.truncated);
    assert_eq!(ranked.skills.discovery_hint, Some(SKILL_HINT));
    assert_eq!(ranked.plugins.discovery_hint, Some(PLUGIN_HINT));
    assert!(serde_json::to_vec(&ranked.skills).unwrap().len() <= STARTUP_SKILL_CATALOG_MAX_BYTES);
    assert!(serde_json::to_vec(&ranked.plugins).unwrap().len() <= STARTUP_PLUGIN_CATALOG_MAX_BYTES);
    assert!(ranked.serialized_len() <= STARTUP_EXTENSION_CATALOG_HARD_MAX_BYTES);
}

#[test]
fn startup_catalog_task_relevance_prefers_names_and_keeps_ties_and_unmatched_rows() {
    let mut entries: Vec<_> = (0..4).map(|i| skill(i, "unrelated")).collect();
    entries[0].description = "quartz audit invoices receipts".to_string();
    entries[1].name = "quartz".to_string();
    entries[2].name = "quartz".to_string();
    let original = StartupSkillsCatalog::available("revision".to_string(), false, entries.clone());
    for query in ["", "the and please", "nonexistent"] {
        let actual = StartupSkillsCatalog::available_for_task(
            "revision".to_string(),
            false,
            entries.clone(),
            &TaskRelevance::new(query),
        );
        assert_eq!(actual, original);
    }
    let actual = StartupSkillsCatalog::available_for_task(
        "revision".to_string(),
        false,
        entries.clone(),
        &TaskRelevance::new("QUARTZ audit invoices receipts receipts"),
    );
    assert_eq!(
        actual
            .entries
            .iter()
            .map(|e| e.skill_id.as_str())
            .collect::<Vec<_>>(),
        [
            entries[1].skill_id.as_str(),
            entries[2].skill_id.as_str(),
            entries[0].skill_id.as_str(),
            entries[3].skill_id.as_str()
        ]
    );
    assert!(!actual.truncated);
    assert_eq!(actual.returned_count, 4);
    assert_eq!(actual.discovery_hint, None);
}

#[test]
fn startup_catalog_task_relevance_matches_complete_chinese_metadata_phrases() {
    let mut entries: Vec<_> = (0..3).map(|i| plugin(i, "unrelated")).collect();
    entries[0].name = "审".to_string();
    entries[1].description = Some("发票审计".to_string());
    entries[2].name = "发票审计".to_string();
    let actual = StartupPluginsCatalog::available_for_task(
        "revision".to_string(),
        3,
        entries,
        &TaskRelevance::new("请完成发票审计，并给出当前流程"),
    );
    assert_eq!(
        actual
            .entries
            .iter()
            .map(|e| e.tool.as_str())
            .collect::<Vec<_>>(),
        ["tool-2", "tool-1", "tool-0"]
    );
}

#[test]
fn startup_catalog_long_skill_metadata_ranks_before_display_clipping() {
    for (query, keyword) in [
        ("QUARTZ audits", "Quartz audit"),
        ("请完成发票审计", "发票审计"),
    ] {
        let description = format!(
            "{} {keyword}. {}",
            "Neutral metadata. ".repeat(35),
            "后续说明。".repeat(15)
        );
        assert!(description.chars().count() <= 1024);
        assert!(!bounded_extension_description(&description).contains(keyword));
        let mut entries: Vec<_> = (0..20)
            .map(|i| skill(i, &"Neutral metadata. ".repeat(30)))
            .collect();
        entries[19].description = description.clone();
        let actual = StartupSkillsCatalog::available_for_task(
            "revision".to_string(),
            true,
            entries,
            &TaskRelevance::new(query),
        );
        assert_eq!(actual.entries[0].name, "skill-19");
        let excerpt = &actual.entries[0].description;
        assert!(excerpt.contains(keyword), "{excerpt}");
        assert!(excerpt.starts_with('…'));
        assert!(excerpt.len() <= 512);
        assert!(description.contains(excerpt.trim_matches('…')));
        assert_eq!(actual.entries[1].name, "skill-0");
        assert_eq!(actual.total_count, 20);
        assert_eq!(actual.catalog_revision.as_deref(), Some("revision"));
        assert!(actual.truncated);
        assert!(serde_json::to_vec(&actual).unwrap().len() <= STARTUP_SKILL_CATALOG_MAX_BYTES);
    }
}

#[test]
fn startup_catalog_skill_excerpts_preserve_prefixes_and_original_utf8_text() {
    let cases = [
        ("unmatched".to_string(), "quartz"),
        (format!("{} quartz", "x".repeat(512)), "missing"),
        (format!("quartz {} quartz", "中".repeat(190)), "quartz"),
        (format!("{} quartz", "中".repeat(171)), "quartz"),
        (
            format!("{} 发票审计、收据", "😀".repeat(128)),
            "请完成发票审计",
        ),
        (
            format!("{} quartz {} audit", "x".repeat(510), "中".repeat(10)),
            "quartz audit",
        ),
        (format!("{} KUARTZ", "中".repeat(180)), "quartz"),
        // Unicode lowercase expansion before a match must not shift source offsets.
        (
            format!("{} QUARTZ {}", "İ".repeat(260), "后".repeat(180)),
            "quartz",
        ),
    ];
    for (description, query) in cases {
        let relevance = TaskRelevance::new(query);
        let prefix = bounded_extension_description(&description);
        let actual = StartupSkillsCatalog::available_for_task(
            "revision".to_string(),
            false,
            vec![skill(0, &description)],
            &relevance,
        );
        let excerpt = &actual.entries[0].description;
        assert!(excerpt.len() <= 512);
        if relevance.score(&[], &prefix).1 > 0 || relevance.score(&[], &description).1 == 0 {
            assert_eq!(excerpt, &prefix);
        } else {
            assert!(excerpt.starts_with('…'));
            assert!(description.contains(excerpt.trim_matches('…')));
            if query == "quartz audit" {
                assert!(excerpt.starts_with("…quartz"));
            }
            assert!(relevance.score(&[], excerpt).1 > 0, "{excerpt}");
        }
    }
}
