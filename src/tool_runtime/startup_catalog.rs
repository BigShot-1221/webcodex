//! Bounded selection metadata for already-observed Skills and Plugins.
//! Discovery and authorization stay in their domain owners; startup composition
//! consumes these values without owning those services or reloading catalogs.
use super::surface::discovery::TaskRelevance;
use crate::json_measurement::serialized_json_len;
use serde::Serialize;
use std::cmp::Reverse;

pub(crate) const STARTUP_EXTENSION_CATALOG_HARD_MAX_BYTES: usize = 6 * 1024;
pub(crate) const STARTUP_SKILL_CATALOG_MAX_BYTES: usize = 2_900;
pub(crate) const STARTUP_PLUGIN_CATALOG_MAX_BYTES: usize = 2_900;
pub(crate) const STARTUP_EXTENSION_DESCRIPTION_MAX_BYTES: usize = 512;
pub(super) const SKILL_DISCOVERY_HINT: &str =
    "Use skills.catalog or list_skills for broader or refreshed discovery.";
pub(super) const PLUGIN_DISCOVERY_HINT: &str = "Use plugins.catalog or explicit plugin_tool list and describe for broader or current schema discovery.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StartupSkillEntry {
    pub(crate) skill_id: String,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) source_scope: String,
    pub(crate) trust: String,
    pub(crate) name_conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StartupPluginEntry {
    pub(crate) plugin: String,
    pub(crate) name: String,
    pub(crate) tool: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "webcodex_core::plugin::PluginSelectionAnnotations::is_empty")]
    pub(crate) annotations: webcodex_core::plugin::PluginSelectionAnnotations,
}

/// Shared startup metadata projection, not a resource store or authority.
/// Entry types, discovery, and execution remain owned by their domains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StartupCatalog<Entry> {
    pub(crate) status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason_code: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) catalog_revision: Option<String>,
    pub(crate) total_count: usize,
    pub(crate) returned_count: usize,
    pub(crate) truncated: bool,
    pub(crate) entries: Vec<Entry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) discovery_hint: Option<&'static str>,
}

pub(crate) type StartupSkillsCatalog = StartupCatalog<StartupSkillEntry>;
pub(crate) type StartupPluginsCatalog = StartupCatalog<StartupPluginEntry>;

impl<Entry: Serialize> StartupCatalog<Entry> {
    fn unavailable_with_hint(reason_code: &'static str, discovery_hint: &'static str) -> Self {
        Self {
            status: "unavailable",
            reason_code: Some(reason_code),
            catalog_revision: None,
            total_count: 0,
            returned_count: 0,
            truncated: false,
            entries: Vec::new(),
            discovery_hint: Some(discovery_hint),
        }
    }

    fn update_completeness(&mut self, upstream_truncated: bool, discovery_hint: &'static str) {
        self.returned_count = self.entries.len();
        self.truncated = upstream_truncated || self.returned_count < self.total_count;
        self.discovery_hint = self.truncated.then_some(discovery_hint);
    }

    fn available_bounded(
        catalog_revision: String,
        total_count: usize,
        upstream_truncated: bool,
        entries: Vec<Entry>,
        max_bytes: usize,
        discovery_hint: &'static str,
    ) -> Self {
        let mut projection = Self {
            status: "available",
            reason_code: None,
            catalog_revision: Some(catalog_revision),
            total_count,
            returned_count: 0,
            truncated: false,
            entries: Vec::new(),
            discovery_hint: None,
        };
        for entry in entries {
            projection.entries.push(entry);
            projection.update_completeness(upstream_truncated, discovery_hint);
            // Measure the full wire envelope: optional hints and JSON escaping
            // participate in the budget. Preserve the original greedy prefix.
            if !serialized_json_len(&projection)
                .map(|bytes| bytes <= max_bytes)
                .unwrap_or(false)
            {
                projection.entries.pop();
                break;
            }
        }
        projection.update_completeness(upstream_truncated, discovery_hint);
        projection
    }
}

impl StartupSkillsCatalog {
    pub(crate) fn available_for_task(
        catalog_revision: String,
        discovery_truncated: bool,
        mut entries: Vec<StartupSkillEntry>,
        relevance: &TaskRelevance,
    ) -> Self {
        entries.sort_by_cached_key(|entry| {
            Reverse(relevance.score(&[&entry.name], &entry.description))
        });
        // Ranking consumes the complete, already-bounded Skill metadata. Only
        // the startup display projection is clipped; explicit catalogs stay intact.
        for entry in &mut entries {
            entry.description = task_extension_description(&entry.description, relevance);
        }
        Self::available(catalog_revision, discovery_truncated, entries)
    }

    pub(crate) fn unavailable(reason_code: &'static str) -> Self {
        Self::unavailable_with_hint(
            reason_code,
            "Use skills.catalog or list_skills for explicit discovery when available.",
        )
    }

    pub(crate) fn available(
        catalog_revision: String,
        discovery_truncated: bool,
        entries: Vec<StartupSkillEntry>,
    ) -> Self {
        Self::available_bounded(
            catalog_revision,
            entries.len(),
            discovery_truncated,
            entries,
            STARTUP_SKILL_CATALOG_MAX_BYTES,
            SKILL_DISCOVERY_HINT,
        )
    }
}

impl StartupPluginsCatalog {
    pub(crate) fn available_for_task(
        catalog_revision: String,
        total_count: usize,
        mut entries: Vec<StartupPluginEntry>,
        relevance: &TaskRelevance,
    ) -> Self {
        entries.sort_by_cached_key(|entry| {
            Reverse(relevance.score(
                &[
                    &entry.plugin,
                    &entry.name,
                    &entry.tool,
                    entry.title.as_deref().unwrap_or_default(),
                ],
                entry.description.as_deref().unwrap_or_default(),
            ))
        });
        Self::available(catalog_revision, total_count, entries)
    }

    pub(crate) fn unavailable(reason_code: &'static str) -> Self {
        Self::unavailable_with_hint(
            reason_code,
            "Use explicit plugin_tool list and describe when Plugin discovery is available.",
        )
    }

    pub(crate) fn available(
        catalog_revision: String,
        total_count: usize,
        entries: Vec<StartupPluginEntry>,
    ) -> Self {
        Self::available_bounded(
            catalog_revision,
            total_count,
            false,
            entries,
            STARTUP_PLUGIN_CATALOG_MAX_BYTES,
            PLUGIN_DISCOVERY_HINT,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StartupExtensions {
    pub(crate) skills: StartupSkillsCatalog,
    pub(crate) plugins: StartupPluginsCatalog,
}

impl StartupExtensions {
    pub(crate) fn serialized_len(&self) -> usize {
        serialized_json_len(self).unwrap_or(usize::MAX)
    }
}

pub(crate) fn bounded_extension_description(value: &str) -> String {
    if value.len() <= STARTUP_EXTENSION_DESCRIPTION_MAX_BYTES {
        return value.to_string();
    }
    let mut end = STARTUP_EXTENSION_DESCRIPTION_MAX_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

/// Retain the old display prefix unless it hides all description evidence.
/// Excerpts contain only original text; omission marks count toward the budget.
fn task_extension_description(value: &str, relevance: &TaskRelevance) -> String {
    let prefix = bounded_extension_description(value);
    if value.len() <= STARTUP_EXTENSION_DESCRIPTION_MAX_BYTES || relevance.score(&[], &prefix).1 > 0
    {
        return prefix;
    }
    let Some(start) = relevance.first_description_hit(value) else {
        return prefix;
    };
    const OMISSION: &str = "…";
    let leading = if start > 0 { OMISSION } else { "" };
    let mut end = value
        .len()
        .min(start + STARTUP_EXTENSION_DESCRIPTION_MAX_BYTES - leading.len());
    let trailing = if end < value.len() {
        end -= OMISSION.len();
        OMISSION
    } else {
        ""
    };
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{leading}{}{trailing}", &value[start..end])
}
