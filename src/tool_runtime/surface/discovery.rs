//! Deterministic lexical retrieval over already-admitted canonical descriptors.
//! This is selection metadata only: no aliases, schema expansion, or authority.
use webcodex_tool_contracts::registry::ToolDescriptor;

const TASK_QUERY_MAX_CHARS: usize = 2048;
const TASK_QUERY_MAX_ASCII_TERMS: usize = 64;
const TASK_MAX_CHINESE_HITS: usize = 64;

/// Bounded lexical evidence for ordering already-admitted startup catalog rows.
/// Names take precedence over descriptions at the caller; neither grants authority.
pub(crate) struct TaskRelevance {
    query: String,
    ascii_terms: Vec<String>,
}

impl TaskRelevance {
    pub(crate) fn new(instruction: &str) -> Self {
        let query = instruction
            .chars()
            .take(TASK_QUERY_MAX_CHARS)
            .collect::<String>()
            .to_lowercase();
        let ascii_terms = terms(&query)
            .into_iter()
            .take(TASK_QUERY_MAX_ASCII_TERMS)
            .map(str::to_owned)
            .collect();
        Self { query, ascii_terms }
    }

    /// Count unique matched terms in each field, independent of repetitions.
    /// The tuple's ordinary ordering makes a name hit dominate description hits.
    pub(crate) fn score(&self, names: &[&str], description: &str) -> (usize, usize) {
        (self.hits(names), self.hits(&[description]))
    }

    /// Locate lexical evidence in the original UTF-8 text, not its lowercase
    /// copy: Unicode case conversion can change byte offsets. Skill metadata
    /// remains bounded by its discovery contract, and query terms stay capped.
    pub(crate) fn first_description_hit(&self, text: &str) -> Option<usize> {
        let lowercase = text.to_lowercase();
        let mut offset = 0;
        let mut first = None;
        for chunk in lowercase.split_inclusive(|ch: char| !ch.is_ascii_alphanumeric()) {
            let word = chunk.trim_end_matches(|ch: char| !ch.is_ascii_alphanumeric());
            if !word.is_empty()
                && self
                    .ascii_terms
                    .binary_search_by(|term| term.as_str().cmp(singular(word)))
                    .is_ok()
            {
                first = Some(offset);
                break;
            }
            offset += chunk.len();
        }
        offset = 0;
        for chunk in lowercase.split_inclusive(|ch| !is_han(ch)) {
            if first.is_some_and(|start| offset >= start) {
                break;
            }
            let phrase = chunk.trim_end_matches(|ch| !is_han(ch));
            if phrase.chars().count() >= 2 && self.query.contains(phrase) {
                first = Some(offset);
                break;
            }
            offset += chunk.len();
        }
        let first = first?;
        let mut normalized_offset = 0;
        for (source_offset, ch) in text.char_indices() {
            normalized_offset += ch.to_lowercase().map(char::len_utf8).sum::<usize>();
            if first < normalized_offset {
                return Some(source_offset);
            }
        }
        None
    }

    fn hits(&self, texts: &[&str]) -> usize {
        let lowercase = texts
            .iter()
            .map(|text| text.to_lowercase())
            .collect::<Vec<_>>();
        let metadata_terms = lowercase
            .iter()
            .flat_map(|text| terms_for_name(text))
            .collect::<std::collections::BTreeSet<_>>();
        let ascii_hits = self
            .ascii_terms
            .iter()
            .filter(|term| metadata_terms.contains(term.as_str()))
            .count();
        // Metadata supplies lexical phrase boundaries. Do not score arbitrary
        // single Han characters or synthesize every substring of a sentence.
        let chinese_hits = lowercase
            .iter()
            .flat_map(|text| text.split(|ch| !is_han(ch)))
            .filter(|phrase| phrase.chars().count() >= 2 && self.query.contains(*phrase))
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            .min(TASK_MAX_CHINESE_HITS);
        ascii_hits + chinese_hits
    }
}

fn is_han(ch: char) -> bool {
    matches!(ch, '\u{3400}'..='\u{4dbf}' | '\u{4e00}'..='\u{9fff}' | '\u{f900}'..='\u{faff}' | '\u{20000}'..='\u{323af}')
}

/// Query work is bounded by the manifest's 200-character input limit and the
/// static catalog. Score once per descriptor, then break ties by canonical name.
pub(crate) fn rank<'a>(specs: &mut Vec<&'a ToolDescriptor>, query: &str) {
    // Underscore-separated keywords support partial names, but each supplied
    // term must have evidence. Unknown task words cannot be discarded merely
    // because a generic word such as "repository" occurs in safety prose.
    let token_query = query.contains('_')
        && query
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    let terms = terms(query);
    let mut ranked = specs
        .iter()
        .filter_map(|spec| {
            let exact = spec.name == query;
            let name = terms_for_name(&spec.name);
            let name_hits = terms.iter().filter(|term| name.contains(term)).count();
            let keyword_terms = spec
                .discovery_keywords
                .iter()
                .flat_map(|keyword| terms_for_name(keyword))
                .collect::<std::collections::BTreeSet<_>>();
            let english_hits = terms
                .iter()
                .filter(|term| keyword_terms.contains(*term))
                .count();
            // Match declared multi-character Chinese vocabulary, never arbitrary
            // individual CJK characters or corpus-specific sentence aliases.
            let chinese_hits = spec
                .discovery_keywords
                .iter()
                .filter(|keyword| !keyword.is_ascii() && query.contains(**keyword))
                .count();
            let keyword_hits = english_hits + chinese_hits;
            let description_text = spec.description.to_lowercase();
            let description = terms_for_name(&description_text);
            let description_hits = terms
                .iter()
                .filter(|term| description.contains(term))
                .count();
            if token_query
                && !terms.iter().all(|term| {
                    name.contains(term)
                        || keyword_terms.contains(term)
                        || description.contains(term)
                })
            {
                return None;
            }
            let score = name_hits * 12 + keyword_hits * 6 + description_hits;
            (exact || score > 0).then_some((*spec, (exact, score, name_hits, keyword_hits)))
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|(left, left_score), (right, right_score)| {
        right_score
            .cmp(left_score)
            .then_with(|| left.name.cmp(&right.name))
    });
    *specs = ranked.into_iter().map(|(spec, _)| spec).collect();
}

fn terms_for_name(text: &str) -> Vec<&str> {
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(singular)
        .collect()
}

fn singular(word: &str) -> &str {
    if word.len() > 3 && word.ends_with('s') && !word.ends_with("ss") && !word.ends_with("us") {
        &word[..word.len() - 1]
    } else {
        word
    }
}

fn terms(text: &str) -> Vec<&str> {
    let mut terms = terms_for_name(text);
    terms.retain(|word| {
        word.len() > 1
            && !matches!(
                *word,
                "the"
                    | "and"
                    | "or"
                    | "of"
                    | "to"
                    | "in"
                    | "on"
                    | "at"
                    | "for"
                    | "from"
                    | "with"
                    | "without"
                    | "by"
                    | "as"
                    | "an"
                    | "is"
                    | "are"
                    | "be"
                    | "was"
                    | "were"
                    | "this"
                    | "that"
                    | "these"
                    | "those"
                    | "it"
                    | "my"
                    | "me"
                    | "we"
                    | "please"
                    | "can"
                    | "could"
                    | "would"
                    | "want"
                    | "need"
                    | "whether"
                    | "how"
                    | "what"
                    | "which"
                    | "some"
                    | "several"
                    | "few"
                    | "one"
                    | "all"
                    | "do"
                    | "doe"
            )
    });
    terms.sort_unstable();
    terms.dedup();
    terms
}

#[cfg(test)]
mod task_relevance_tests {
    use super::{TaskRelevance, TASK_QUERY_MAX_CHARS};

    #[test]
    fn matches_unique_normalized_ascii_terms_in_each_field() {
        let relevance = TaskRelevance::new("Please FIND project_files files");
        assert_eq!(
            relevance.score(&["PROJECT_FILES", "files"], "Find files; find files."),
            (2, 2)
        );
        assert_eq!(relevance.score(&["please"], "unrelated"), (0, 0));
    }

    #[test]
    fn matches_complete_chinese_metadata_phrases_without_single_characters() {
        let relevance = TaskRelevance::new("请搜索知识库中的文档");
        assert_eq!(
            relevance.score(&["知识库", "知识库"], "搜索、文档、搜索"),
            (1, 2)
        );
        assert_eq!(relevance.score(&["库"], "知识管理"), (0, 0));
    }

    #[test]
    fn bounds_instruction_characters_and_unique_ascii_terms() {
        let beyond_limit = format!("{} files", "中".repeat(TASK_QUERY_MAX_CHARS));
        assert_eq!(
            TaskRelevance::new(&beyond_limit).score(&["files"], ""),
            (0, 0)
        );

        let query = (0..100)
            .map(|index| format!("term{index:03}"))
            .collect::<Vec<_>>()
            .join(" ");
        let relevance = TaskRelevance::new(&query);
        assert_eq!(relevance.score(&[&query], ""), (64, 0));
        assert_eq!(
            TaskRelevance::new("").score(&["files", "知识库"], ""),
            (0, 0)
        );
    }
}
