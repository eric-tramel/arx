//! BM25 ranking over small in-memory corpora (one paper's cached material).
//!
//! Follows the BM25S design (Lù, 2024): impact scores are computed eagerly
//! when the index is built, so a query is just sparse postings lookups and
//! additions. IDF uses the Lucene formulation `ln(1 + (n - df + 0.5) / (df + 0.5))`,
//! which stays non-negative for terms present in every document.

use std::collections::HashMap;

pub const BM25_K1: f64 = 1.2;
pub const BM25_B: f64 = 0.75;

/// Tokenize text for indexing and querying.
///
/// Splits on non-alphanumeric characters, which also handles TeX markup:
/// `\section{Deep Learning}` yields `section`, `deep`, `learning`, and
/// `arXiv:2101.00001` yields `arxiv`, `2101`, `00001`. Single-character
/// tokens are dropped because inline math makes them ubiquitous noise.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|token| token.chars().count() >= 2)
        .map(|token| token.to_lowercase())
        .collect()
}

#[derive(Debug, Default)]
pub struct Bm25Index {
    postings: HashMap<String, Vec<(usize, f64)>>,
    document_count: usize,
}

impl Bm25Index {
    /// Build an index from pre-tokenized documents, eagerly computing the
    /// BM25 impact of every (term, document) pair.
    pub fn from_documents(documents: &[Vec<String>]) -> Self {
        let document_count = documents.len();
        if document_count == 0 {
            return Self::default();
        }

        let mut term_frequencies: Vec<HashMap<&str, u32>> = Vec::with_capacity(document_count);
        let mut total_length = 0usize;
        for tokens in documents {
            total_length += tokens.len();
            let mut frequencies = HashMap::new();
            for token in tokens {
                *frequencies.entry(token.as_str()).or_insert(0) += 1;
            }
            term_frequencies.push(frequencies);
        }
        let average_length = (total_length as f64 / document_count as f64).max(1.0);

        let mut document_frequencies: HashMap<&str, u64> = HashMap::new();
        for frequencies in &term_frequencies {
            for term in frequencies.keys() {
                *document_frequencies.entry(term).or_insert(0) += 1;
            }
        }

        let mut postings: HashMap<String, Vec<(usize, f64)>> = HashMap::new();
        for (doc_id, frequencies) in term_frequencies.iter().enumerate() {
            let doc_length = documents[doc_id].len() as f64;
            for (term, tf) in frequencies {
                let idf = idf(document_count as u64, document_frequencies[term]);
                let impact = idf * term_score(*tf as f64, doc_length, average_length);
                postings
                    .entry((*term).to_string())
                    .or_default()
                    .push((doc_id, impact));
            }
        }

        Self {
            postings,
            document_count,
        }
    }

    /// Score every document against the query and return `(doc_id, score)`
    /// pairs with positive scores, best first. Ties break on doc_id so
    /// earlier documents (metadata before source) win.
    pub fn rank(&self, query_tokens: &[String]) -> Vec<(usize, f64)> {
        let mut query_frequencies: HashMap<&str, f64> = HashMap::new();
        for token in query_tokens {
            *query_frequencies.entry(token.as_str()).or_insert(0.0) += 1.0;
        }

        let mut scores: HashMap<usize, f64> = HashMap::new();
        for (term, weight) in query_frequencies {
            if let Some(entries) = self.postings.get(term) {
                for (doc_id, impact) in entries {
                    *scores.entry(*doc_id).or_insert(0.0) += weight * impact;
                }
            }
        }

        let mut ranked: Vec<(usize, f64)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked
    }

    pub fn is_empty(&self) -> bool {
        self.document_count == 0
    }
}

fn term_score(tf: f64, doc_length: f64, average_length: f64) -> f64 {
    let norm = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * (doc_length / average_length));
    if norm <= 0.0 {
        0.0
    } else {
        tf * (BM25_K1 + 1.0) / norm
    }
}

fn idf(document_count: u64, document_frequency: u64) -> f64 {
    let n = document_count.max(document_frequency) as f64;
    let df = document_frequency as f64;
    (1.0 + ((n - df + 0.5) / (df + 0.5))).ln().max(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_of(texts: &[&str]) -> (Bm25Index, Vec<Vec<String>>) {
        let documents: Vec<Vec<String>> = texts.iter().map(|text| tokenize(text)).collect();
        (Bm25Index::from_documents(&documents), documents)
    }

    #[test]
    fn tokenize_handles_tex_markup() {
        assert_eq!(
            tokenize(r"\section{Deep Learning} $x_i$ arXiv:2101.00001"),
            vec!["section", "deep", "learning", "arxiv", "2101", "00001"]
        );
    }

    #[test]
    fn tokenize_lowercases_and_drops_single_characters() {
        assert_eq!(
            tokenize("A Calibration THEOREM"),
            vec!["calibration", "theorem"]
        );
    }

    #[test]
    fn rank_prefers_documents_matching_more_query_terms() {
        let (index, _) = index_of(&[
            "gradient descent converges slowly",
            "stochastic gradient descent with momentum",
            "closed form solution",
        ]);
        let ranked = index.rank(&tokenize("stochastic gradient descent"));
        assert_eq!(ranked[0].0, 1);
        assert_eq!(ranked.len(), 2);
        assert!(ranked[0].1 > ranked[1].1);
    }

    #[test]
    fn rank_weighs_rare_terms_higher_than_common_ones() {
        let (index, _) = index_of(&[
            "model training model evaluation",
            "model calibration analysis",
            "model deployment guide",
        ]);
        let ranked = index.rank(&tokenize("model calibration"));
        // "calibration" is rare (df=1) so its document must outrank
        // documents matching only the ubiquitous "model".
        assert_eq!(ranked[0].0, 1);
        assert_eq!(ranked.len(), 3);
    }

    #[test]
    fn rank_returns_empty_for_unmatched_query() {
        let (index, _) = index_of(&["alpha beta", "gamma delta"]);
        assert!(index.rank(&tokenize("omega")).is_empty());
    }

    #[test]
    fn scores_match_reference_formula() {
        // Cross-checked against moraine's bm25_term_score/bm25_idf
        // (crates/moraine-conversations/src/clickhouse_repo/search.rs):
        // idf = ln(1 + (n - df + 0.5)/(df + 0.5)), tf-norm with k1=1.2, b=0.75.
        let (index, documents) = index_of(&["calibration theorem", "local search theorem"]);
        let ranked = index.rank(&tokenize("calibration"));
        assert_eq!(ranked.len(), 1);
        let doc_length = documents[0].len() as f64; // 2
        let average_length = 2.5;
        let expected_idf = (1.0f64 + (2.0 - 1.0 + 0.5) / 1.5).ln();
        let expected_tf = (BM25_K1 + 1.0)
            / (1.0 + BM25_K1 * (1.0 - BM25_B + BM25_B * doc_length / average_length));
        assert!((ranked[0].1 - expected_idf * expected_tf).abs() < 1e-12);
    }

    #[test]
    fn empty_corpus_yields_empty_index() {
        let index = Bm25Index::from_documents(&[]);
        assert!(index.is_empty());
        assert!(index.rank(&tokenize("anything")).is_empty());
    }
}
