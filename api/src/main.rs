use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use shared::{TranscriptionResponse, TranscriptionTask};
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use uuid::Uuid;
#[derive(Debug)]
enum TranscribeError {
    FileOperation(String),
    RabbitMQ(String),
    Validation(String),
}

impl IntoResponse for TranscribeError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            TranscribeError::Validation(msg) => (StatusCode::BAD_REQUEST, msg),
            TranscribeError::FileOperation(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            TranscribeError::RabbitMQ(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg),
        };

        let body = Json(serde_json::json!({
            "error": error_message
        }));

        (status, body).into_response()
    }
}

#[derive(Clone)]
struct RabbitMQPublisher {
    channel: amqprs::channel::Channel,
}

impl RabbitMQPublisher {
    fn new(channel: amqprs::channel::Channel) -> Self {
        Self { channel }
    }

    /// Отправляет задачу в RabbitMQ очередь
    async fn publish_task(&self, task: &TranscriptionTask) -> Result<(), TranscribeError> {
        info!(
            task_id = %task.task_id,
            file_path = %task.file_path,
            "Publishing task to RabbitMQ"
        );

        let json_payload = serde_json::to_vec(task)
            .map_err(|e| TranscribeError::RabbitMQ(format!("JSON serialization failed: {}", e)))?;

        let mut props = amqprs::BasicProperties::default();
        props.with_delivery_mode(2);

        self.channel
            .exchange_declare(amqprs::channel::ExchangeDeclareArguments::new(
                "transcribe",
                "direct",
            ))
            .await
            .map_err(|e| TranscribeError::RabbitMQ(format!("Failed to declare exchange: {}", e)))?;

        let payload_size = json_payload.len();
        self.channel
            .basic_publish(
                props,
                json_payload,
                amqprs::channel::BasicPublishArguments::new("transcribe", "transcribe.task"),
            )
            .await
            .map_err(|e| TranscribeError::RabbitMQ(format!("Publish failed: {}", e)))?;

        info!(
            task_id = %task.task_id,
            payload_size = payload_size,
            "Task published successfully"
        );

        Ok(())
    }
}

#[derive(Clone)]
struct FileStorage {
    tmp_dir: String,
    max_file_size: u64, // в байтах
}

impl FileStorage {
    fn new(tmp_dir: String, max_file_size_mb: u64) -> Self {
        Self {
            tmp_dir,
            max_file_size: max_file_size_mb * 1024 * 1024,
        }
    }

    async fn ensure_tmp_dir(&self) -> Result<(), TranscribeError> {
        tokio::fs::create_dir_all(&self.tmp_dir)
            .await
            .map_err(|e| TranscribeError::FileOperation(format!("Cannot create tmp dir: {}", e)))?;
        Ok(())
    }

    fn generate_file_path(&self, original_filename: &str) -> Result<String, TranscribeError> {
        // Валидация имени файла
        if original_filename.is_empty() || original_filename.len() > 255 {
            return Err(TranscribeError::Validation(
                "Invalid filename length".to_string(),
            ));
        }

        let safe_filename = original_filename
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '.' || *c == '-' || *c == '_')
            .collect::<String>();

        if safe_filename.is_empty() {
            return Err(TranscribeError::Validation(
                "Filename contains no valid characters".to_string(),
            ));
        }

        let ext = Path::new(&safe_filename)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("ogg");
        let unique_filename = format!("{}_{}.{}", Uuid::new_v4(), safe_filename, ext);

        Ok(format!("{}/{}", self.tmp_dir, unique_filename))
    }

    async fn save_file_streaming_with_hash(
        &self,
        file_path: &str,
        mut multipart_field: axum::extract::multipart::Field<'_>,
    ) -> Result<(u64, String), TranscribeError> {
        let file = tokio::fs::File::create(file_path)
            .await
            .map_err(|e| TranscribeError::FileOperation(format!("Cannot create file: {}", e)))?;
        let mut file = tokio::io::BufWriter::new(file);

        let mut hasher = Sha256::new();
        let mut total_bytes = 0u64;

        while let Some(chunk) = multipart_field
            .chunk()
            .await
            .map_err(|e| TranscribeError::FileOperation(format!("Failed to read chunk: {}", e)))?
        {
            let chunk_size = chunk.len() as u64;

            if total_bytes + chunk_size > self.max_file_size {
                let _ = tokio::fs::remove_file(file_path).await;
                return Err(TranscribeError::Validation(format!(
                    "File size exceeds limit of {} MB",
                    self.max_file_size / (1024 * 1024)
                )));
            }

            hasher.update(&chunk);

            file.write_all(&chunk).await.map_err(|e| {
                TranscribeError::FileOperation(format!("Failed to write chunk: {}", e))
            })?;

            total_bytes += chunk_size;
        }

        file.flush().await.map_err(|e| {
            TranscribeError::FileOperation(format!("Failed to flush buffer: {}", e))
        })?;

        let file = file.into_inner();
        file.sync_all()
            .await
            .map_err(|e| TranscribeError::FileOperation(format!("Failed to sync file: {}", e)))?;

        let hash_bytes = hasher.finalize();
        let file_hash = hex::encode(hash_bytes);

        info!(
            file_path = %file_path,
            total_bytes = total_bytes,
            file_hash = %file_hash,
            "File saved successfully with hash"
        );

        Ok((total_bytes, file_hash))
    }

    async fn delete_file(&self, file_path: &str) -> Result<(), TranscribeError> {
        tokio::fs::remove_file(file_path).await.map_err(|e| {
            warn!(file_path = %file_path, error = %e, "Failed to delete file");
            TranscribeError::FileOperation(format!("Cannot delete file: {}", e))
        })
    }
}

#[derive(Clone)]
struct AppState {
    storage: FileStorage,
    rabbitmq: RabbitMQPublisher,
}

#[derive(Debug, Deserialize)]
struct TranscribeQuery {
    chat_id: String,
    message_id: Option<String>,
}

async fn handle_transcribe(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Query(query_params): axum::extract::Query<TranscribeQuery>,
    mut multipart: Multipart,
) -> Result<Json<TranscriptionResponse>, TranscribeError> {
    let storage = &state.storage;
    let rabbitmq = &state.rabbitmq;

    storage.ensure_tmp_dir().await?;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| TranscribeError::FileOperation(format!("Multipart parsing failed: {}", e)))?
    {
        let field_name = field.name().unwrap_or("file").to_string();

        if field_name != "file" {
            warn!(field_name = %field_name, "Skipping non-file field");
            continue;
        }

        let original_filename = field
            .file_name()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "audio.ogg".to_string());

        let ext = Path::new(&original_filename)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        if !matches!(ext, "ogg" | "mp3" | "wav" | "webm" | "m4a") {
            return Err(TranscribeError::Validation(
                "Unsupported audio format. Allowed: ogg, mp3, wav, webm, m4a".to_string(),
            ));
        }

        let file_path = storage.generate_file_path(&original_filename)?;

        return match storage
            .save_file_streaming_with_hash(&file_path, field)
            .await
        {
            Ok((file_size, file_hash)) => {
                info!(
                    file_path = %file_path,
                    file_size = file_size,
                    file_hash = %file_hash,
                    "File saved and hashed"
                );

                let cached_result_path = format!("{}/cache_{}.json", storage.tmp_dir, file_hash);
                if tokio::fs::metadata(&cached_result_path).await.is_ok() {
                    match tokio::fs::read_to_string(&cached_result_path).await {
                        Ok(content)
                            if serde_json::from_str::<serde_json::Value>(&content).is_ok() =>
                        {
                            info!(file_hash = %file_hash, "Cache hit! Returning cached result");

                            let _ = storage.delete_file(&file_path).await;

                            let task_id = Uuid::new_v4().to_string();
                            let result_path = format!("{}/{}.json", storage.tmp_dir, task_id);

                            match tokio::fs::copy(&cached_result_path, &result_path).await {
                                Ok(_) => {
                                    return Ok(Json(TranscriptionResponse {
                                        task_id,
                                        status: "completed".to_string(),
                                        file_path: result_path,
                                    }));
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to copy cached result, will process normally");
                                }
                            }
                        }
                        _ => {
                            warn!(file_hash = %file_hash, "Corrupted cache file detected, removing and processing normally");
                            let _ = tokio::fs::remove_file(&cached_result_path).await;
                        }
                    }
                }

                let task = TranscriptionTask {
                    task_id: Uuid::new_v4().to_string(),
                    file_path: file_path.clone(),
                    chat_id: query_params.chat_id.clone(),
                    message_id: query_params.message_id.clone(),
                    filename: original_filename.clone(),
                    created_at: chrono::Utc::now().timestamp(),
                    file_hash,
                };

                match rabbitmq.publish_task(&task).await {
                    Ok(_) => Ok(Json(TranscriptionResponse {
                        task_id: task.task_id,
                        status: "queued".to_string(),
                        file_path,
                    })),
                    Err(e) => {
                        let _ = storage.delete_file(&file_path).await;
                        Err(e)
                    }
                }
            }
            Err(e) => {
                let _ = storage.delete_file(&file_path).await;
                Err(e)
            }
        };
    }

    Err(TranscribeError::Validation("No file uploaded".to_string()))
}

/// Healthcheck
async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "transcribe-api"
    }))
}

async fn get_result(
    axum::extract::Path(task_id): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::response::Response {
    if !task_id.chars().all(|c| c.is_alphanumeric() || c == '-') {
        return (StatusCode::BAD_REQUEST, "Invalid task ID").into_response();
    }

    let result_path = format!("{}/{}.json", state.storage.tmp_dir, task_id);

    match tokio::fs::read_to_string(&result_path).await {
        Ok(json_content) => match serde_json::from_str::<serde_json::Value>(&json_content) {
            Ok(json) => Json(json).into_response(),
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Invalid JSON result").into_response(),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (
            StatusCode::OK,
            Json(serde_json::json!({
                "task_id": task_id,
                "status": "processing"
            })),
        )
            .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Error reading result").into_response(),
    }
}

async fn cleanup_task(dir: String, result_max_age_secs: u64, cache_max_age_secs: u64) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60 * 10)); // Каждые 10 минут
    loop {
        interval.tick().await;

        let mut deleted_results = 0;
        let mut deleted_cache = 0;

        if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(metadata) = entry.metadata().await {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(elapsed) = modified.elapsed() {
                            let path = entry.path();
                            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                            if filename.starts_with("cache_") && filename.ends_with(".json") {
                                if elapsed.as_secs() > cache_max_age_secs {
                                    if tokio::fs::remove_file(&path).await.is_ok() {
                                        deleted_cache += 1;
                                    }
                                }
                            } else if filename.ends_with(".json") {
                                if elapsed.as_secs() > result_max_age_secs {
                                    if tokio::fs::remove_file(&path).await.is_ok() {
                                        deleted_results += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if deleted_results > 0 || deleted_cache > 0 {
            info!(
                deleted_results = deleted_results,
                deleted_cache = deleted_cache,
                "Cleanup completed"
            );
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .compact()
        .init();

    info!("Starting transcribe service...");

    // Инициализация RabbitMQ
    let rabbitmq_host = std::env::var("RABBITMQ_HOST").unwrap_or_else(|_| "localhost".to_string());
    let rabbitmq_user = std::env::var("RABBITMQ_USER").unwrap_or_else(|_| "guest".to_string());
    let rabbitmq_pass = std::env::var("RABBITMQ_PASS").unwrap_or_else(|_| "guest".to_string());
    let connection =
        amqprs::connection::Connection::open(&amqprs::connection::OpenConnectionArguments::new(
            &rabbitmq_host,
            5672,
            &rabbitmq_user,
            &rabbitmq_pass,
        ))
        .await
        .expect("Failed to connect to RabbitMQ");

    let channel = connection
        .open_channel(None)
        .await
        .expect("Failed to open RabbitMQ channel");

    let state = AppState {
        storage: FileStorage::new("/tmp/transcribe".to_string(), 500),
        rabbitmq: RabbitMQPublisher::new(channel),
    };

    tokio::spawn(cleanup_task("/tmp/transcribe".to_string(), 3600, 604800));

    let app = Router::new()
        .route("/health", get(health))
        .route("/result/{task_id}", get(get_result))
        .route(
            "/transcribe",
            post(handle_transcribe).layer(DefaultBodyLimit::max(500 * 1024 * 1024)),
        )
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let listen_host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let bind_addr = format!("{}:3000", listen_host);
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|_| panic!("Failed to bind to {}", bind_addr));

    info!("Server running on http://{}:3000", listen_host);
    info!(
        "Transcribe endpoint: POST http://{}:3000/transcribe",
        listen_host
    );

    axum::serve(listener, app).await.expect("Server error");
}
