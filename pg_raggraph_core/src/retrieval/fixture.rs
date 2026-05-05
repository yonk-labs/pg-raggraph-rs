//! JSONL fixture parser for `pgrg.ingest_extracted`.
//!
//! Mission brief SC-003: a fixture file with `chunks + entities +
//! relationships + chunk_entities + pre-computed embeddings` is loaded
//! directly into the schema, bypassing chunk/embed/extract.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{CoreError, CoreResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureDocument {
    pub id: Uuid,
    pub namespace: String,
    pub source: String,
    pub content_hash: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default = "default_obj")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureChunk {
    pub id: Uuid,
    pub namespace: String,
    pub document_id: Uuid,
    pub ord: i32,
    pub text: String,
    pub token_count: i32,
    pub embedding: Vec<f32>,
    #[serde(default = "default_obj")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureEntity {
    pub id: Uuid,
    pub namespace: String,
    pub name: String,
    /// Renamed from `kind` to avoid collision with the JSONL discriminator field.
    #[serde(rename = "kind_label", default)]
    pub kind_label: Option<String>,
    pub name_emb: Vec<f32>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureRelationship {
    pub id: Uuid,
    pub namespace: String,
    pub src_id: Uuid,
    pub dst_id: Uuid,
    /// Renamed from `kind` to avoid collision with the JSONL discriminator
    /// field (mirrors `FixtureEntity::kind_label`).
    #[serde(rename = "kind_label")]
    pub kind: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureChunkEntity {
    pub chunk_id: Uuid,
    pub entity_id: Uuid,
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    #[serde(default = "default_classification")]
    pub classification: String,
}

fn default_obj() -> Value {
    serde_json::json!({})
}
fn default_weight() -> f64 {
    1.0
}
fn default_confidence() -> f64 {
    1.0
}
fn default_classification() -> String {
    "extracted".to_string()
}

#[derive(Debug, Clone)]
pub enum FixtureRecord {
    Document(FixtureDocument),
    Chunk(FixtureChunk),
    Entity(FixtureEntity),
    Relationship(FixtureRelationship),
    ChunkEntity(FixtureChunkEntity),
}

/// Parse one JSONL line. Empty/whitespace-only lines yield
/// `Err(CoreError::InvalidConfig("empty line"))` — caller should skip.
pub fn parse_jsonl_line(line: &str) -> CoreResult<FixtureRecord> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(CoreError::InvalidConfig("empty line".into()));
    }
    let v: Value = serde_json::from_str(trimmed)?;
    let kind = v
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| CoreError::InvalidConfig("missing `kind` field".into()))?;
    match kind {
        "document" => Ok(FixtureRecord::Document(serde_json::from_value(v)?)),
        "chunk" => Ok(FixtureRecord::Chunk(serde_json::from_value(v)?)),
        "entity" => Ok(FixtureRecord::Entity(serde_json::from_value(v)?)),
        "relationship" => Ok(FixtureRecord::Relationship(serde_json::from_value(v)?)),
        "chunk_entity" => Ok(FixtureRecord::ChunkEntity(serde_json::from_value(v)?)),
        other => Err(CoreError::InvalidConfig(format!(
            "unknown record kind: {other}"
        ))),
    }
}
