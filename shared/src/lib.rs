use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptionTask {
    pub task_id: String,
    pub file_path: String,
    pub chat_id: String,
    pub message_id: Option<String>,
    pub filename: String,
    pub created_at: i64,
    pub file_hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TranscriptionResponse {
    pub task_id: String,
    pub status: String,
    pub file_path: String,
}
