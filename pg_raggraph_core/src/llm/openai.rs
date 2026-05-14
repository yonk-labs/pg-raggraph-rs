//! `OpenAiProvider` — Chat Completions, JSON-mode for structured extraction.
//!
//! SC-001. Uses the shared `HttpClient` (sync) via `reqwest::blocking`.
//! `base_url` defaults to `https://api.openai.com`; per-provider override
//! flows through `pgrg.providers.base_url` (handled by the pgrx-side
//! provider factory in a later task).
//!
//! Status mapping (matches T9 contract):
//!   200 -> parse and return `Extraction`
//!   429, 5xx -> `CoreError::Http` (retryable by `RetryingProvider`)
//!   other 4xx -> `CoreError::Llm` (permanent)

use crate::error::{CoreError, CoreResult};
use crate::llm::http::{HttpClassification, HttpClient};
use crate::llm::{ExtractedEntity, ExtractedRelationship, Extraction, LlmProvider};

const EXTRACTION_SYSTEM_PROMPT: &str = include_str!("prompts/extraction_system.txt");

pub struct OpenAiProvider {
    api_key: String,
    model: String,
    base_url: String,
    http: HttpClient,
}

impl OpenAiProvider {
    pub fn new(
        api_key: impl Into<String>,
        model: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            model: model.into(),
            base_url: base_url.into(),
            http: HttpClient::new(),
        }
    }
}

impl LlmProvider for OpenAiProvider {
    fn extract(&self, chunk_text: &str, _namespace: &str) -> CoreResult<Extraction> {
        let body = serde_json::json!({
            "model": self.model,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": format!("Extract from:\n\n{chunk_text}")}
            ]
        });
        let url = format!("{}/v1/chat/completions", self.base_url);
        let (status, body) = self.http.post_json(&url, Some(&self.api_key), &body)?;
        match HttpClassification::from_status(status) {
            HttpClassification::Retryable => {
                return Err(CoreError::Http(format!("status {status}: {body}")));
            }
            HttpClassification::Permanent => {
                return Err(CoreError::Llm(format!("status {status}: {body}")));
            }
            HttpClassification::Ok => {}
        }
        parse_response(&body)
    }
}

fn parse_response(body: &str) -> CoreResult<Extraction> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| CoreError::Llm(format!("parse: {e}")))?;
    let content = v["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| CoreError::Llm("no choices[0].message.content".into()))?;
    let parsed: serde_json::Value =
        serde_json::from_str(content).map_err(|e| CoreError::Llm(format!("content parse: {e}")))?;

    let entities = parsed["entities"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|e| {
                    Some(ExtractedEntity {
                        name: e["name"].as_str()?.to_string(),
                        kind: e["kind"].as_str().map(str::to_string),
                        description: e["description"].as_str().map(str::to_string),
                        // Confidence is documented as a value in [0, 1]; f64 -> f32
                        // truncation is acceptable precision loss for a probability.
                        #[allow(clippy::cast_possible_truncation)]
                        confidence: e["confidence"].as_f64().unwrap_or(1.0) as f32,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let relationships = parsed["relationships"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|r| {
                    Some(ExtractedRelationship {
                        src_name: r["src_name"].as_str()?.to_string(),
                        dst_name: r["dst_name"].as_str()?.to_string(),
                        kind: r["kind"].as_str()?.to_string(),
                        // Weight and confidence are bounded small floats; f64 -> f32
                        // truncation is acceptable precision loss.
                        #[allow(clippy::cast_possible_truncation)]
                        weight: r["weight"].as_f64().unwrap_or(1.0) as f32,
                        #[allow(clippy::cast_possible_truncation)]
                        confidence: r["confidence"].as_f64().unwrap_or(1.0) as f32,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(Extraction {
        entities,
        relationships,
    })
}
