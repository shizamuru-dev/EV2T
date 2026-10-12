FROM rust:1.85-slim AS builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y git make g++ wget cmake

RUN git clone https://github.com/ggerganov/whisper.cpp.git /whisper && \
    cd /whisper && \
    cmake -B build -DBUILD_SHARED_LIBS=OFF && cmake --build build --config Release && \
    bash ./models/download-ggml-model.sh small

COPY Cargo.toml Cargo.lock ./
COPY shared/Cargo.toml shared/
COPY api/Cargo.toml api/
COPY worker/Cargo.toml worker/

RUN mkdir -p shared/src api/src worker/src && \
    touch shared/src/lib.rs api/src/main.rs worker/src/main.rs

RUN cargo fetch

COPY shared/src ./shared/src
COPY api/src ./api/src
COPY worker/src ./worker/src

RUN cargo build --release --bin api
RUN cargo build --release --bin worker

RUN mkdir -p /whisper_libs && \
    cp /whisper/build/src/libwhisper.so* /whisper_libs/ 2>/dev/null || true

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y ca-certificates ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/app/target/release/api /usr/local/bin/api
COPY --from=builder /usr/src/app/target/release/worker /usr/local/bin/worker

COPY --from=builder /whisper/build/bin/whisper-cli /usr/local/bin/whisper-cli
COPY --from=builder /whisper_libs/ /usr/local/lib/
RUN ldconfig

COPY --from=builder /whisper/models/ggml-small.bin /models/ggml-small.bin

RUN mkdir -p /tmp/transcribe && chmod 777 /tmp/transcribe

EXPOSE 3000
