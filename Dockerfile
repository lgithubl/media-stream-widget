FROM rust:1.98-bookworm AS builder

WORKDIR /src

COPY Cargo.toml /src/Cargo.toml
COPY src /src/src
RUN cargo build --release

FROM debian:bookworm-slim

ENV MEDIA_STREAM_HOST=0.0.0.0 \
    MEDIA_STREAM_PORT=8080 \
    MEDIA_STREAM_DATA_DIR=/tmp/media-stream-widget \
    MEDIA_STREAM_UPLOAD_DIR=/tmp/media-stream-widget/uploads \
    MEDIA_STREAM_UPLOAD_ENABLED=false \
    MEDIA_STREAM_TCP_NODELAY=true \
    MEDIA_STREAM_SOCKET_SEND_BUFFER_BYTES=0 \
    MEDIA_STREAM_CACHE_CONTROL=no-store \
    MEDIA_STREAM_INITIAL_CHUNK_BYTES=262144 \
    MEDIA_STREAM_READ_CHUNK_BYTES=1048576 \
    MEDIA_STREAM_PREFETCH_BYTES=8388608 \
    MEDIA_STREAM_PREFETCH_MAX_TASKS=2 \
    MEDIA_STREAM_SENDFILE_ENABLED=true

WORKDIR /app

COPY --from=builder /src/target/release/media-stream-widget /app/media-stream-widget
COPY public /app/public

EXPOSE 8080

CMD ["/app/media-stream-widget"]
