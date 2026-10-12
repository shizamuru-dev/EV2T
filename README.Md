# EV2T 

A full-featured microservice for audio-to-text transcription based on `whisper.cpp`. The architecture consists of a REST API for receiving files and a worker for asynchronous task queue processing through RabbitMQ.

## Features

- 🎙️ **Multi-format support**: OGG, MP3, WAV, WebM, M4A
- 📦 **Asynchronous processing**: RabbitMQ integration for scalability
- 🚀 **REST API**: Simple-to-use interface for uploading and retrieving results
- 🐳 **Docker & Kubernetes**: Ready-to-use configs for containerization and orchestration
- 📊 **Scalability**: Modular architecture — multiple workers can process tasks in parallel

## Architecture

The project is organized as a Rust monorepo:

```
├── api/          # REST API service (Actix-web)
├── worker/       # Worker for RabbitMQ queue processing
├── shared/       # Common types and utilities
└── k8s/          # Kubernetes manifests
```

### Data Flow

1. **API** receives an audio file and query parameters (`chat_id`, optionally `message_id`)
2. File is saved to disk, a message is created in the RabbitMQ queue
3. **Worker** receives the message, runs transcription via `whisper-cli`
4. Result is saved as a JSON file
5. Client retrieves the result via `/result/{task_id}` endpoint

## Requirements

- **Rust 1.70+** (or use Docker)
- **RabbitMQ 3.8+** (for queue processing)
- **whisper.cpp** (must be available in PATH as `whisper-cli`)
- Optional: Docker & Docker Compose for containerization

## Installation and Running

### Option 1: Local (without Docker)

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/Extera-V2T-Server.git
   cd Extera-V2T-Server
   ```

2. **Prepare environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env if necessary
   ```

3. **Ensure RabbitMQ is running:**
   ```bash
   # Locally on localhost:5672 or set RABBITMQ_HOST in .env
   ```

4. **Run the API:**
   ```bash
   cargo run -p api
   ```
   API will be available at `http://localhost:3000`

5. **In a separate terminal, run the worker:**
   ```bash
   cargo run -p worker
   ```

### Option 2: Docker Compose

1. **Start everything with one command:**
   ```bash
   docker-compose up
   ```

   This will start:
    - RabbitMQ (localhost:5672)
    - API service (localhost:3000)
    - Worker (background process)

2. **Stop:**
   ```bash
   docker-compose down
   ```

### Option 3: Kubernetes

1. **Apply manifests:**
   ```bash
   kubectl apply -f k8s/
   ```

2. **Check status:**
   ```bash
   kubectl get pods
   kubectl get svc
   ```

## API Documentation

Detailed documentation is available in [API.md](./API.md)

### Quick Start

**Upload a file for transcription:**
```bash
curl -X POST "http://localhost:3000/transcribe?chat_id=12345" \
     -F "file=@audio.ogg"
```

Response:
```json
{
  "task_id": "4d1db26d-2d4e-4f3a-96e5-4f3183592161",
  "status": "queued",
  "file_path": "/tmp/transcribe/4d1db26d-2d4e-4f3a-96e5-4f3183592161_audio.ogg"
}
```

**Get transcription result:**
```bash
curl -X GET "http://localhost:3000/result/4d1db26d-2d4e-4f3a-96e5-4f3183592161"
```

Response:
```json
{
  "transcription": [
    {
      "timestamps": {"from": "00:00:00,000", "to": "00:00:02,000"},
      "offsets": {"from": 0, "to": 2000},
      "text": " Hello, this is a test recording."
    }
  ]
}
```

**Check service status:**
```bash
curl -X GET "http://localhost:3000/health"
```

## Environment Variables

```env
# RabbitMQ credentials
RABBITMQ_USER=root
RABBITMQ_PASS=<your_password>

# Optional (if RabbitMQ is not on localhost)
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_VHOST=/

# API config
API_HOST=0.0.0.0
API_PORT=3000

# File paths
UPLOAD_DIR=/tmp/transcribe
RESULTS_DIR=/tmp/transcribe/results
```

## Configuration

- **Max upload size**: 500 MB
- **Supported formats**: OGG, MP3, WAV, WebM, M4A
- **Queue**: RabbitMQ with queue `transcribe_tasks`
- **Results storage**: JSON files in the `RESULTS_DIR` directory

## Development

### Code Structure

- `shared/` — common types, errors, config
- `api/` — HTTP layer, request handling
- `worker/` — RabbitMQ consumer, whisper.cpp integration

### Running Tests

```bash
cargo test
```

### Linting and Formatting

```bash
cargo fmt
cargo clippy
```

## Building Docker Image

```bash
docker build -t extera-v2t-server .
docker run -p 3000:3000 --env-file .env extera-v2t-server
```

## Production Deployment

For production use:
- **Kubernetes** for orchestration (see `k8s/`)
- **RabbitMQ cluster** for high availability
- Configure **worker scaling** based on load
- Use **persistent volumes** for file storage
- Set up **healthchecks** and **monitoring**

## Troubleshooting

### RabbitMQ connection error
```
Ensure RabbitMQ is running and accessible at the address from RABBITMQ_HOST
Check credentials in .env
```

### whisper-cli not found
```
Install whisper.cpp: https://github.com/ggerganov/whisper.cpp
Ensure whisper-cli is available in PATH
```

### Task stuck in queue
```
Check worker logs: docker-compose logs worker
Ensure whisper.cpp can process the audio file
```

## License

MIT

## Contributing

Pull requests are welcome. For significant changes, please open an issue first.

## Support

If you find a bug or have questions, please open an issue on GitHub.