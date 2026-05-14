//! `OllamaProvider` — local Ollama HTTP API (`/api/generate`) with
//! `format: "json"` for structured extraction.
//!
//! SC-001. No auth header — Ollama is typically a local-network API. Uses
//! the shared `HttpClient` for timeouts + `pg-raggraph/0.1` User-Agent.
//!
//! Status mapping (matches T9 contract):
//!   200 -> parse the `.response` JSON string into Extraction
//!   429, 5xx -> `CoreError::Http` (retryable)
//!   other 4xx -> `CoreError::Llm` (permanent)

use crate::error::{CoreError, CoreResult};
use crate::llm::http::{HttpClassification, HttpClient};
use crate::llm::{Completion, ExtractedEntity, ExtractedRelationship, Extraction, LlmProvider};

const EXTRACTION_SYSTEM_PROMPT: &str = include_str!("prompts/extraction_system.txt");

pub struct OllamaProvider {
    model: String,
    base_url: String,
    http: HttpClient,
}

impl OllamaProvider {
    pub fn new(model: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            base_url: base_url.into(),
            http: HttpClient::new(),
        }
    }
}

impl LlmProvider for OllamaProvider {
    fn extract(&self, chunk_text: &str, _namespace: &str) -> CoreResult<Extraction> {
        // Ollama's /api/generate accepts a single flattened prompt (no
        // chat-roles structure). Combine system + user into one prompt.
        let prompt = format!("{EXTRACTION_SYSTEM_PROMPT}\n\nExtract from:\n\n{chunk_text}");
        let body = serde_json::json!({
            "model":  self.model,
            "prompt": prompt,
            "format": "json",
            "stream": false
        });
        let url = format!("{}/api/generate", self.base_url);
        let (status, resp_body) = self.http.post_json(&url, None, &body)?;
        match HttpClassification::from_status(status) {
            HttpClassification::Retryable => {
                return Err(CoreError::Http(format!("status {status}: {resp_body}")));
            }
            HttpClassification::Permanent => {
                return Err(CoreError::Llm(format!("status {status}: {resp_body}")));
            }
            HttpClassification::Ok => {}
        }
        parse_response(&resp_body)
    }

    fn complete(&self, prompt: &str) -> CoreResult<Completion> {
        let body = serde_json::json!({
            "model":  self.model,
            "prompt": prompt,
            "stream": false
        });
        let url = format!("{}/api/generate", self.base_url);
        let (status, resp_body) = self.http.post_json(&url, None, &body)?;
        match HttpClassification::from_status(status) {
            HttpClassification::Retryable => {
                return Err(CoreError::Http(format!("status {status}: {resp_body}")));
            }
            HttpClassification::Permanent => {
                return Err(CoreError::Llm(format!("status {status}: {resp_body}")));
            }
            HttpClassification::Ok => {}
        }
        let v: serde_json::Value =
            serde_json::from_str(&resp_body).map_err(|e| CoreError::Llm(format!("parse: {e}")))?;
        let text = v["response"]
            .as_str()
            .ok_or_else(|| CoreError::Llm("response missing .response".into()))?
            .to_string();
        // Token counts are bounded by what the provider reports; u64 -> u32
        // truncation is acceptable (no real model will report > 2^32 tokens).
        #[allow(clippy::cast_possible_truncation)]
        let prompt_tokens = v["prompt_eval_count"].as_u64().unwrap_or(0) as u32;
        #[allow(clippy::cast_possible_truncation)]
        let completion_tokens = v["eval_count"].as_u64().unwrap_or(0) as u32;
        Ok(Completion {
            text,
            prompt_tokens,
            completion_tokens,
        })
    }
}

fn parse_response(body: &str) -> CoreResult<Extraction> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| CoreError::Llm(format!("parse: {e}")))?;
    // Ollama returns: { "response": "<JSON string>", "done": true, ... }
    // Note: when stream=false, the entire output is a single JSON string in `.response`.
    let inner = v["response"]
        .as_str()
        .ok_or_else(|| CoreError::Llm("response missing .response string".into()))?;
    let parsed: serde_json::Value = serde_json::from_str(inner)
        .map_err(|e| CoreError::Llm(format!("response inner parse: {e}")))?;

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
