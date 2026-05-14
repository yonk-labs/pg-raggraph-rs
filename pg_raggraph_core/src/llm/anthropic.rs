//! `AnthropicProvider` — Messages API with forced tool use for structured extraction.
//!
//! SC-001. Same status-mapping contract as `OpenAiProvider`:
//!   200            -> parse and return `Extraction`
//!   429, 5xx (incl. 529 "overloaded") -> `CoreError::Http` (retryable)
//!   other 4xx      -> `CoreError::Llm` (permanent)
//!
//! Auth note: Anthropic uses the `x-api-key` header (NOT bearer auth), plus the
//! required `anthropic-version: 2023-06-01` header. All requests go through
//! `HttpClient::post_json_with_headers` so the configured timeout (30 s) and
//! User-Agent (`pg-raggraph/0.1`) apply uniformly. Ollama (T13) can use the
//! same surface for its auth-free calls.

use crate::error::{CoreError, CoreResult};
use crate::llm::http::{HttpClassification, HttpClient};
use crate::llm::{ExtractedEntity, ExtractedRelationship, Extraction, LlmProvider};

const EXTRACTION_TOOL_NAME: &str = "extract_graph";

pub struct AnthropicProvider {
    api_key: String,
    model: String,
    base_url: String,
    http: HttpClient,
}

impl AnthropicProvider {
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

impl LlmProvider for AnthropicProvider {
    fn extract(&self, chunk_text: &str, _namespace: &str) -> CoreResult<Extraction> {
        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": 4096,
            "tools": [{
                "name": EXTRACTION_TOOL_NAME,
                "description": "Extract entities and relationships from text.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "entities": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name":       {"type": "string"},
                                    "kind":       {"type": "string"},
                                    "confidence": {"type": "number"}
                                },
                                "required": ["name"]
                            }
                        },
                        "relationships": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "src_name":   {"type": "string"},
                                    "dst_name":   {"type": "string"},
                                    "kind":       {"type": "string"},
                                    "weight":     {"type": "number"},
                                    "confidence": {"type": "number"}
                                },
                                "required": ["src_name", "dst_name", "kind"]
                            }
                        }
                    },
                    "required": ["entities", "relationships"]
                }
            }],
            "tool_choice": {"type": "tool", "name": EXTRACTION_TOOL_NAME},
            "messages": [{
                "role": "user",
                "content": format!("Extract entities and relationships from:\n\n{chunk_text}")
            }]
        });

        let url = format!("{}/v1/messages", self.base_url);
        let headers: &[(&str, &str)] = &[
            ("x-api-key", self.api_key.as_str()),
            ("anthropic-version", "2023-06-01"),
            ("content-type", "application/json"),
        ];
        let (status, resp_body) = self.http.post_json_with_headers(&url, headers, &body)?;

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
}

fn parse_response(body: &str) -> CoreResult<Extraction> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| CoreError::Llm(format!("parse: {e}")))?;

    // Find the first content[] element with "type": "tool_use".
    let content = v["content"]
        .as_array()
        .ok_or_else(|| CoreError::Llm("response missing content[]".into()))?;
    let tool_block = content
        .iter()
        .find(|b| b["type"].as_str() == Some("tool_use"))
        .ok_or_else(|| CoreError::Llm("no tool_use block in content[]".into()))?;
    let input = &tool_block["input"];

    let entities = input["entities"]
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

    let relationships = input["relationships"]
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
