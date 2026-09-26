use std::collections::HashSet;
use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Multipart, Path as AxumPath, State};
use axum::http::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, HeaderMap,
    HeaderValue, RANGE,
};
use axum::http::{Method, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use futures_util::{TryStreamExt, stream};
use serde::Serialize;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Mutex, Semaphore};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};

const MEDIA_EXTENSIONS: &[&str] = &[
    "aac", "flac", "m4a", "mp3", "ogg", "opus", "wav", "webm", "m4v", "mkv", "mov", "mp4",
];
const DEFAULT_INITIAL_CHUNK_BYTES: usize = 256 * 1024;
const DEFAULT_READ_CHUNK_BYTES: usize = 1024 * 1024;
const MIN_INITIAL_CHUNK_BYTES: usize = 16 * 1024;
const MIN_READ_CHUNK_BYTES: usize = 64 * 1024;
const MAX_READ_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_PREFETCH_MAX_TASKS: usize = 2;
const MAX_PREFETCH_BYTES: u64 = 512 * 1024 * 1024;
const SLOW_FIRST_CHUNK_US: u64 = 500_000;
const SLOW_STREAM_US: u64 = 5_000_000;

#[derive(Clone)]
struct AppState {
    data_dir: Arc<PathBuf>,
    upload_dir: Arc<PathBuf>,
    stats: Arc<Stats>,
    sendfile_enabled: bool,
    upload_enabled: bool,
    tcp_nodelay: bool,
    socket_send_buffer_bytes: u32,
    stream_cache_control: Arc<String>,
    initial_chunk_bytes: usize,
    read_chunk_bytes: usize,
    prefetch_bytes: u64,
    prefetch_max_tasks: usize,
    prefetch_semaphore: Arc<Semaphore>,
    prefetch_paths: Arc<Mutex<HashSet<String>>>,
}

#[derive(Default)]
struct Stats {
    started_at_unix: AtomicU64,
    stream_requests: AtomicU64,
    stream_active: AtomicU64,
    stream_active_max: AtomicU64,
    stream_head_requests: AtomicU64,
    stream_range_requests: AtomicU64,
    stream_full_requests: AtomicU64,
    stream_status_200: AtomicU64,
    stream_status_206: AtomicU64,
    stream_status_4xx: AtomicU64,
    stream_status_5xx: AtomicU64,
    stream_requested_bytes: AtomicU64,
    stream_errors: AtomicU64,
    stream_canceled: AtomicU64,
    stream_completed: AtomicU64,
    stream_bytes: AtomicU64,
    stream_chunks: AtomicU64,
    stream_read_us_total: AtomicU64,
    stream_duration_us_total: AtomicU64,
    stream_first_chunk_us_total: AtomicU64,
    stream_open_us_total: AtomicU64,
    stream_seek_us_total: AtomicU64,
    stream_setup_us_total: AtomicU64,
    stream_first_chunk_us_max: AtomicU64,
    stream_read_us_max: AtomicU64,
    stream_duration_us_max: AtomicU64,
    stream_open_us_max: AtomicU64,
    stream_seek_us_max: AtomicU64,
    stream_setup_us_max: AtomicU64,
    stream_slow_first_chunk: AtomicU64,
    stream_slow_completed: AtomicU64,
    upload_requests: AtomicU64,
    upload_bytes: AtomicU64,
    meta_requests: AtomicU64,
    file_list_requests: AtomicU64,
    prefetch_scheduled: AtomicU64,
    prefetch_skipped_disabled: AtomicU64,
    prefetch_skipped_eof: AtomicU64,
    prefetch_skipped_duplicate: AtomicU64,
    prefetch_skipped_busy: AtomicU64,
    prefetch_active: AtomicU64,
    prefetch_completed: AtomicU64,
    prefetch_errors: AtomicU64,
    prefetch_bytes: AtomicU64,
    prefetch_read_us_total: AtomicU64,
    sendfile_requests: AtomicU64,
    sendfile_bytes: AtomicU64,
    sendfile_calls: AtomicU64,
    sendfile_us_total: AtomicU64,
    sendfile_us_max: AtomicU64,
    userspace_stream_requests: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        let stats = Self::default();
        stats.reset();
        stats
    }

    fn reset(&self) {
        self.started_at_unix.store(now_unix(), Ordering::Relaxed);
        self.stream_requests.store(0, Ordering::Relaxed);
        self.stream_active.store(0, Ordering::Relaxed);
        self.stream_active_max.store(0, Ordering::Relaxed);
        self.stream_head_requests.store(0, Ordering::Relaxed);
        self.stream_range_requests.store(0, Ordering::Relaxed);
        self.stream_full_requests.store(0, Ordering::Relaxed);
        self.stream_status_200.store(0, Ordering::Relaxed);
        self.stream_status_206.store(0, Ordering::Relaxed);
        self.stream_status_4xx.store(0, Ordering::Relaxed);
        self.stream_status_5xx.store(0, Ordering::Relaxed);
        self.stream_requested_bytes.store(0, Ordering::Relaxed);
        self.stream_errors.store(0, Ordering::Relaxed);
        self.stream_canceled.store(0, Ordering::Relaxed);
        self.stream_completed.store(0, Ordering::Relaxed);
        self.stream_bytes.store(0, Ordering::Relaxed);
        self.stream_chunks.store(0, Ordering::Relaxed);
        self.stream_read_us_total.store(0, Ordering::Relaxed);
        self.stream_duration_us_total.store(0, Ordering::Relaxed);
        self.stream_first_chunk_us_total.store(0, Ordering::Relaxed);
        self.stream_open_us_total.store(0, Ordering::Relaxed);
        self.stream_seek_us_total.store(0, Ordering::Relaxed);
        self.stream_setup_us_total.store(0, Ordering::Relaxed);
        self.stream_first_chunk_us_max.store(0, Ordering::Relaxed);
        self.stream_read_us_max.store(0, Ordering::Relaxed);
        self.stream_duration_us_max.store(0, Ordering::Relaxed);
        self.stream_open_us_max.store(0, Ordering::Relaxed);
        self.stream_seek_us_max.store(0, Ordering::Relaxed);
        self.stream_setup_us_max.store(0, Ordering::Relaxed);
        self.stream_slow_first_chunk.store(0, Ordering::Relaxed);
        self.stream_slow_completed.store(0, Ordering::Relaxed);
        self.upload_requests.store(0, Ordering::Relaxed);
        self.upload_bytes.store(0, Ordering::Relaxed);
        self.meta_requests.store(0, Ordering::Relaxed);
        self.file_list_requests.store(0, Ordering::Relaxed);
        self.prefetch_scheduled.store(0, Ordering::Relaxed);
        self.prefetch_skipped_disabled.store(0, Ordering::Relaxed);
        self.prefetch_skipped_eof.store(0, Ordering::Relaxed);
        self.prefetch_skipped_duplicate.store(0, Ordering::Relaxed);
        self.prefetch_skipped_busy.store(0, Ordering::Relaxed);
        self.prefetch_active.store(0, Ordering::Relaxed);
        self.prefetch_completed.store(0, Ordering::Relaxed);
        self.prefetch_errors.store(0, Ordering::Relaxed);
        self.prefetch_bytes.store(0, Ordering::Relaxed);
        self.prefetch_read_us_total.store(0, Ordering::Relaxed);
        self.sendfile_requests.store(0, Ordering::Relaxed);
        self.sendfile_bytes.store(0, Ordering::Relaxed);
        self.sendfile_calls.store(0, Ordering::Relaxed);
        self.sendfile_us_total.store(0, Ordering::Relaxed);
        self.sendfile_us_max.store(0, Ordering::Relaxed);
        self.userspace_stream_requests.store(0, Ordering::Relaxed);
    }
}

struct StreamGuard {
    stats: Arc<Stats>,
    request_started: Instant,
    done: AtomicBool,
}

impl StreamGuard {
    fn new(stats: Arc<Stats>, request_started: Instant) -> Self {
        Self {
            stats,
            request_started,
            done: AtomicBool::new(false),
        }
    }

    fn finish(&self) {
        if self
            .done
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            complete_stream(&self.stats, self.request_started);
        }
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if !self.done.load(Ordering::Relaxed) {
            self.stats.stream_canceled.fetch_add(1, Ordering::Relaxed);
            self.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Serialize)]
struct MediaMeta {
    id: String,
    path: String,
    name: String,
    #[serde(rename = "contentType")]
    content_type: String,
    size: u64,
    mtime: u64,
    #[serde(rename = "streamUrl")]
    stream_url: String,
}

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    detail: String,
    headers: HeaderMap,
}

impl AppError {
    fn new(status: StatusCode, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
            headers: HeaderMap::new(),
        }
    }

    fn with_header(mut self, name: axum::http::header::HeaderName, value: String) -> Self {
        if let Ok(value) = HeaderValue::from_str(&value) {
            self.headers.insert(name, value);
        }
        self
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(serde_json::json!({ "detail": self.detail })),
        )
            .into_response();
        response.headers_mut().extend(self.headers);
        response
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = env::current_dir().context("resolve current directory")?;
    let public_dir = root.join("public");
    let data_dir = env_var("MEDIA_STREAM_DATA_DIR", "AUDIO_WIDGET_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/media-stream-widget"));
    let upload_dir = env_var("MEDIA_STREAM_UPLOAD_DIR", "AUDIO_WIDGET_UPLOAD_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| data_dir.join("uploads"));
    let upload_enabled = env_bool("MEDIA_STREAM_UPLOAD_ENABLED", "AUDIO_WIDGET_UPLOAD_ENABLED", false);
    let read_chunk_bytes = env_usize(
        "MEDIA_STREAM_READ_CHUNK_BYTES",
        "AUDIO_WIDGET_READ_CHUNK_BYTES",
        DEFAULT_READ_CHUNK_BYTES,
        MIN_READ_CHUNK_BYTES,
        MAX_READ_CHUNK_BYTES,
    );
    let initial_chunk_bytes = env_usize(
        "MEDIA_STREAM_INITIAL_CHUNK_BYTES",
        "AUDIO_WIDGET_INITIAL_CHUNK_BYTES",
        DEFAULT_INITIAL_CHUNK_BYTES,
        MIN_INITIAL_CHUNK_BYTES,
        read_chunk_bytes,
    );
    let sendfile_enabled = sendfile_supported()
        && env_bool(
            "MEDIA_STREAM_SENDFILE_ENABLED",
            "AUDIO_WIDGET_SENDFILE_ENABLED",
            true,
        );
    let tcp_nodelay = env_bool("MEDIA_STREAM_TCP_NODELAY", "AUDIO_WIDGET_TCP_NODELAY", true);
    let socket_send_buffer_bytes = env_u32(
        "MEDIA_STREAM_SOCKET_SEND_BUFFER_BYTES",
        "AUDIO_WIDGET_SOCKET_SEND_BUFFER_BYTES",
        0,
        0,
        64 * 1024 * 1024,
    );
    let stream_cache_control =
        env_var("MEDIA_STREAM_CACHE_CONTROL", "AUDIO_WIDGET_STREAM_CACHE_CONTROL")
            .unwrap_or_else(|_| "no-store".to_string());
    let prefetch_max_tasks = env_usize(
        "MEDIA_STREAM_PREFETCH_MAX_TASKS",
        "AUDIO_WIDGET_PREFETCH_MAX_TASKS",
        DEFAULT_PREFETCH_MAX_TASKS,
        1,
        64,
    );

    if upload_enabled {
        fs::create_dir_all(&upload_dir)
            .await
            .context("create upload dir")?;
    }

    let state = AppState {
        data_dir: Arc::new(data_dir),
        upload_dir: Arc::new(upload_dir),
        stats: Arc::new(Stats::new()),
        sendfile_enabled,
        upload_enabled,
        tcp_nodelay,
        socket_send_buffer_bytes,
        stream_cache_control: Arc::new(stream_cache_control),
        initial_chunk_bytes,
        read_chunk_bytes,
        prefetch_bytes: env_u64(
            "MEDIA_STREAM_PREFETCH_BYTES",
            "AUDIO_WIDGET_PREFETCH_BYTES",
            8 * 1024 * 1024,
            0,
            MAX_PREFETCH_BYTES,
        ),
        prefetch_max_tasks,
        prefetch_semaphore: Arc::new(Semaphore::new(prefetch_max_tasks)),
        prefetch_paths: Arc::new(Mutex::new(HashSet::new())),
    };

    let host =
        env_var("MEDIA_STREAM_HOST", "AUDIO_WIDGET_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env_var("MEDIA_STREAM_PORT", "AUDIO_WIDGET_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .context("parse bind address")?;
    if state.sendfile_enabled {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            return run_sendfile_http(state, public_dir, addr).await;
        }
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/config", get(config))
        .route("/api/stats", get(stats))
        .route("/api/stats/reset", post(reset_stats))
        .route("/api/files", get(list_files))
        .route("/api/meta/{encoded_path}", get(meta))
        .route("/api/upload", post(upload_file))
        .route("/upload", post(upload_file))
        .route(
            "/api/stream/{encoded_path}",
            get(stream_path).head(stream_path),
        )
        .route(
            "/files/{encoded_path}/stream",
            get(stream_path).head(stream_path),
        )
        .route_service(
            "/media-stream-widget.js",
            ServeFile::new(public_dir.join("media-stream-widget.js")),
        )
        .route_service(
            "/media-stream-widget.css",
            ServeFile::new(public_dir.join("media-stream-widget.css")),
        )
        .nest_service("/assets", ServeDir::new(&public_dir))
        .route("/", get(index))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind server")?;
    axum::serve(listener, app).await.context("serve app")?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "ok": true,
        "mode": "path-stream",
        "runtime": "rust",
        "sendfileEnabled": state.sendfile_enabled,
        "dataDir": state.data_dir.to_string_lossy(),
        "uploadEnabled": state.upload_enabled,
        "uploadDir": state.upload_dir.to_string_lossy(),
        "tcpNodelay": state.tcp_nodelay,
        "socketSendBufferBytes": state.socket_send_buffer_bytes,
        "streamCacheControl": state.stream_cache_control.as_str(),
        "initialChunkBytes": state.initial_chunk_bytes,
        "readChunkBytes": state.read_chunk_bytes,
        "prefetchBytes": state.prefetch_bytes,
        "prefetchMaxTasks": state.prefetch_max_tasks,
    }))
}

async fn config(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "apiVersion": "0.3.0",
        "mode": "path-stream",
        "runtime": "rust",
        "sendfileSupported": sendfile_supported(),
        "sendfileEnabled": state.sendfile_enabled,
        "uploadEnabled": state.upload_enabled,
        "uploadDir": state.upload_dir.to_string_lossy(),
        "tcpNodelay": state.tcp_nodelay,
        "socketSendBufferBytes": state.socket_send_buffer_bytes,
        "streamCacheControl": state.stream_cache_control.as_str(),
        "initialChunkBytes": state.initial_chunk_bytes,
        "readChunkBytes": state.read_chunk_bytes,
        "prefetchBytes": state.prefetch_bytes,
        "prefetchMaxTasks": state.prefetch_max_tasks,
        "mediaExtensions": MEDIA_EXTENSIONS,
        "audioExtensions": ["aac", "flac", "m4a", "mp3", "ogg", "opus", "wav", "webm"],
        "videoExtensions": ["m4v", "mkv", "mov", "mp4", "webm"],
        "endpoints": {
            "stream": "/api/stream/{base64urlPath}",
            "meta": "/api/meta/{base64urlPath}",
            "stats": "/api/stats",
            "resetStats": "/api/stats/reset",
            "upload": "/api/upload",
            "demoFiles": "/api/files"
        }
    }))
}

async fn stats(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(stats_snapshot(&state))
}

async fn reset_stats(State(state): State<AppState>) -> Json<serde_json::Value> {
    state.stats.reset();
    Json(stats_snapshot(&state))
}

async fn index() -> Result<Html<String>, AppError> {
    let body = fs::read_to_string("public/index.html")
        .await
        .map_err(|_| AppError::new(StatusCode::NOT_FOUND, "index not found"))?;
    Ok(Html(body))
}

async fn list_files(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    state
        .stats
        .file_list_requests
        .fetch_add(1, Ordering::Relaxed);
    if !state.upload_enabled {
        return Ok(Json(serde_json::json!({ "files": [] })));
    }
    let mut files = Vec::new();
    let mut entries = fs::read_dir(&*state.upload_dir)
        .await
        .map_err(|_| AppError::new(StatusCode::NOT_FOUND, "upload dir not found"))?;
    while let Some(entry) = entries.next_entry().await.map_err(|_| {
        AppError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to read upload dir",
        )
    })? {
        let path = entry.path();
        if path.is_file() && is_media_name(&path) {
            if let Ok(meta) = file_meta(path).await {
                files.push(meta);
            }
        }
    }
    files.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    Ok(Json(serde_json::json!({ "files": files })))
}

async fn meta(
    State(state): State<AppState>,
    AxumPath(encoded_path): AxumPath<String>,
) -> Result<Json<MediaMeta>, AppError> {
    state.stats.meta_requests.fetch_add(1, Ordering::Relaxed);
    let path = ensure_streamable_path(&encoded_path).await?;
    Ok(Json(file_meta(path).await?))
}

async fn upload_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<MediaMeta>, AppError> {
    if !state.upload_enabled {
        return Err(AppError::new(
            StatusCode::FORBIDDEN,
            "demo upload is disabled",
        ));
    }
    state.stats.upload_requests.fetch_add(1, Ordering::Relaxed);
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| AppError::new(StatusCode::BAD_REQUEST, "invalid multipart body"))?
    {
        if field.name() != Some("file") {
            continue;
        }
        let filename = safe_name(field.file_name().unwrap_or("upload"));
        if !is_media_name(Path::new(&filename)) {
            return Err(AppError::new(
                StatusCode::BAD_REQUEST,
                "unsupported media extension",
            ));
        }
        let target = unique_upload_path(&state.upload_dir, &filename).await?;
        let mut output = File::create(&target).await.map_err(|_| {
            AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "failed to create upload")
        })?;
        let mut stream = field.into_stream();
        while let Some(chunk) = stream
            .try_next()
            .await
            .map_err(|_| AppError::new(StatusCode::BAD_REQUEST, "failed to read upload"))?
        {
            state
                .stats
                .upload_bytes
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            output.write_all(&chunk).await.map_err(|_| {
                AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "failed to write upload")
            })?;
        }
        let canonical = target.canonicalize().map_err(|_| {
            AppError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to resolve upload",
            )
        })?;
        return Ok(Json(file_meta(canonical).await?));
    }
    Err(AppError::new(
        StatusCode::BAD_REQUEST,
        "file field is required",
    ))
}

async fn stream_path(
    State(state): State<AppState>,
    method: Method,
    AxumPath(encoded_path): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let request_started = Instant::now();
    state.stats.stream_requests.fetch_add(1, Ordering::Relaxed);
    state
        .stats
        .userspace_stream_requests
        .fetch_add(1, Ordering::Relaxed);
    let active = state.stats.stream_active.fetch_add(1, Ordering::Relaxed) + 1;
    atomic_max(&state.stats.stream_active_max, active);
    if method == Method::HEAD {
        state
            .stats
            .stream_head_requests
            .fetch_add(1, Ordering::Relaxed);
    }
    let path = match ensure_streamable_path(&encoded_path).await {
        Ok(path) => path,
        Err(error) => {
            record_stream_status(&state.stats, error.status);
            state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return Err(error);
        }
    };
    let metadata = match fs::metadata(&path).await {
        Ok(metadata) => metadata,
        Err(_) => {
            record_stream_status(&state.stats, StatusCode::NOT_FOUND);
            state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return Err(AppError::new(StatusCode::NOT_FOUND, "media file not found"));
        }
    };
    let total = metadata.len();
    let range_header = headers.get(RANGE).and_then(|value| value.to_str().ok());
    if range_header.is_some() {
        state
            .stats
            .stream_range_requests
            .fetch_add(1, Ordering::Relaxed);
    } else {
        state
            .stats
            .stream_full_requests
            .fetch_add(1, Ordering::Relaxed);
    }
    let (status, start, end) = match parse_range(range_header, total) {
        Ok(range) => range,
        Err(error) => {
            record_stream_status(&state.stats, error.status);
            state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return Err(error);
        }
    };
    let length = end
        .saturating_sub(start)
        .saturating_add(if total == 0 { 0 } else { 1 });
    state
        .stats
        .stream_requested_bytes
        .fetch_add(length, Ordering::Relaxed);
    let content_type = mime_guess::from_path(&path)
        .first_or_octet_stream()
        .to_string();

    let mut response_headers = HeaderMap::new();
    response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(CACHE_CONTROL, header_value(&state.stream_cache_control)?);
    response_headers.insert(CONTENT_TYPE, header_value(&content_type)?);
    response_headers.insert(CONTENT_LENGTH, header_value(&length.to_string())?);
    response_headers.insert(
        "x-media-path-encoded",
        header_value(&encode_path(path.to_string_lossy().as_ref()))?,
    );
    if status == StatusCode::PARTIAL_CONTENT {
        response_headers.insert(
            CONTENT_RANGE,
            header_value(&format!("bytes {start}-{end}/{total}"))?,
        );
    }

    if method == Method::HEAD || length == 0 {
        let setup_us = elapsed_us(request_started);
        state
            .stats
            .stream_setup_us_total
            .fetch_add(setup_us, Ordering::Relaxed);
        atomic_max(&state.stats.stream_setup_us_max, setup_us);
        record_stream_status(&state.stats, status);
        complete_stream(&state.stats, request_started);
        return Ok((status, response_headers, Body::empty()).into_response());
    }

    let open_started = Instant::now();
    let mut file = match File::open(&path).await {
        Ok(file) => file,
        Err(_) => {
            record_stream_status(&state.stats, StatusCode::NOT_FOUND);
            state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return Err(AppError::new(StatusCode::NOT_FOUND, "media file not found"));
        }
    };
    let open_us = elapsed_us(open_started);
    state
        .stats
        .stream_open_us_total
        .fetch_add(open_us, Ordering::Relaxed);
    atomic_max(&state.stats.stream_open_us_max, open_us);
    let seek_started = Instant::now();
    if file.seek(SeekFrom::Start(start)).await.is_err() {
        record_stream_status(&state.stats, StatusCode::INTERNAL_SERVER_ERROR);
        state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
        state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
        return Err(AppError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to seek media file",
        ));
    }
    let seek_us = elapsed_us(seek_started);
    state
        .stats
        .stream_seek_us_total
        .fetch_add(seek_us, Ordering::Relaxed);
    atomic_max(&state.stats.stream_seek_us_max, seek_us);
    spawn_prefetch(state.clone(), path.clone(), end.saturating_add(1), total).await;
    let setup_us = elapsed_us(request_started);
    state
        .stats
        .stream_setup_us_total
        .fetch_add(setup_us, Ordering::Relaxed);
    atomic_max(&state.stats.stream_setup_us_max, setup_us);
    record_stream_status(&state.stats, status);
    let stream_guard = Arc::new(StreamGuard::new(state.stats.clone(), request_started));
    let stream = stream_file_range(
        file,
        length,
        state.initial_chunk_bytes,
        state.read_chunk_bytes,
        stream_guard,
    );
    Ok((status, response_headers, Body::from_stream(stream)).into_response())
}

fn stream_file_range(
    file: File,
    length: u64,
    initial_chunk_size: usize,
    chunk_size: usize,
    guard: Arc<StreamGuard>,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    stream::try_unfold(
        (
            file,
            length,
            initial_chunk_size.max(1),
            chunk_size.max(1),
            true,
            guard,
        ),
        |(mut file, remaining, initial_chunk_size, chunk_size, is_first, guard)| async move {
            let stats = guard.stats.clone();
            if remaining == 0 {
                return Ok(None);
            }
            let current_chunk_size = if is_first {
                initial_chunk_size
            } else {
                chunk_size
            };
            let read_len = (current_chunk_size as u64).min(remaining) as usize;
            let mut buffer = vec![0; read_len];
            let read_started = Instant::now();
            let bytes_read = match file.read(&mut buffer).await {
                Ok(bytes_read) => bytes_read,
                Err(error) => {
                    stats.stream_errors.fetch_add(1, Ordering::Relaxed);
                    stats.stream_active.fetch_sub(1, Ordering::Relaxed);
                    guard.done.store(true, Ordering::Relaxed);
                    return Err(error);
                }
            };
            let read_us = elapsed_us(read_started);
            stats
                .stream_read_us_total
                .fetch_add(read_us, Ordering::Relaxed);
            atomic_max(&stats.stream_read_us_max, read_us);
            if bytes_read == 0 {
                guard.finish();
                return Ok(None);
            }
            if is_first {
                let first_chunk_us = elapsed_us(guard.request_started);
                stats
                    .stream_first_chunk_us_total
                    .fetch_add(first_chunk_us, Ordering::Relaxed);
                atomic_max(&stats.stream_first_chunk_us_max, first_chunk_us);
                if first_chunk_us >= SLOW_FIRST_CHUNK_US {
                    stats
                        .stream_slow_first_chunk
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            stats
                .stream_bytes
                .fetch_add(bytes_read as u64, Ordering::Relaxed);
            stats.stream_chunks.fetch_add(1, Ordering::Relaxed);
            buffer.truncate(bytes_read);
            let next_remaining = remaining.saturating_sub(bytes_read as u64);
            if next_remaining == 0 {
                guard.finish();
            }
            Ok(Some((
                Bytes::from(buffer),
                (
                    file,
                    next_remaining,
                    initial_chunk_size,
                    chunk_size,
                    false,
                    guard,
                ),
            )))
        },
    )
}

async fn spawn_prefetch(state: AppState, path: PathBuf, start: u64, total: u64) {
    if state.prefetch_bytes == 0 {
        state
            .stats
            .prefetch_skipped_disabled
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    if start >= total {
        state
            .stats
            .prefetch_skipped_eof
            .fetch_add(1, Ordering::Relaxed);
        return;
    }

    let key = path.to_string_lossy().to_string();
    {
        let mut paths = state.prefetch_paths.lock().await;
        if !paths.insert(key.clone()) {
            state
                .stats
                .prefetch_skipped_duplicate
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
    }
    state
        .stats
        .prefetch_scheduled
        .fetch_add(1, Ordering::Relaxed);

    tokio::spawn(async move {
        let permit = state.prefetch_semaphore.clone().try_acquire_owned();
        if permit.is_err() {
            state
                .stats
                .prefetch_skipped_busy
                .fetch_add(1, Ordering::Relaxed);
            let mut paths = state.prefetch_paths.lock().await;
            paths.remove(&key);
            return;
        }
        let _permit = permit.ok();
        state.stats.prefetch_active.fetch_add(1, Ordering::Relaxed);
        let length = state.prefetch_bytes.min(total.saturating_sub(start));
        let prefetch_started = Instant::now();
        match prefetch_range(&path, start, length, state.read_chunk_bytes).await {
            Ok(bytes_read) => {
                state
                    .stats
                    .prefetch_completed
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .stats
                    .prefetch_bytes
                    .fetch_add(bytes_read, Ordering::Relaxed);
                state
                    .stats
                    .prefetch_read_us_total
                    .fetch_add(elapsed_us(prefetch_started), Ordering::Relaxed);
            }
            Err(_) => {
                state.stats.prefetch_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        state.stats.prefetch_active.fetch_sub(1, Ordering::Relaxed);
        let mut paths = state.prefetch_paths.lock().await;
        paths.remove(&key);
    });
}

async fn prefetch_range(
    path: &Path,
    start: u64,
    length: u64,
    chunk_size: usize,
) -> Result<u64, std::io::Error> {
    if length == 0 {
        return Ok(0);
    }
    let mut file = File::open(path).await?;
    file.seek(SeekFrom::Start(start)).await?;
    let mut remaining = length;
    let mut total_read = 0;
    let mut buffer = vec![0; chunk_size.max(1)];
    while remaining > 0 {
        let read_len = (buffer.len() as u64).min(remaining) as usize;
        let bytes_read = file.read(&mut buffer[..read_len]).await?;
        if bytes_read == 0 {
            break;
        }
        total_read += bytes_read as u64;
        remaining = remaining.saturating_sub(bytes_read as u64);
    }
    Ok(total_read)
}

fn header_value(value: &str) -> Result<HeaderValue, AppError> {
    HeaderValue::from_str(value)
        .map_err(|_| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid response header"))
}

fn record_stream_status(stats: &Stats, status: StatusCode) {
    match status.as_u16() {
        200 => {
            stats.stream_status_200.fetch_add(1, Ordering::Relaxed);
        }
        206 => {
            stats.stream_status_206.fetch_add(1, Ordering::Relaxed);
        }
        400..=499 => {
            stats.stream_status_4xx.fetch_add(1, Ordering::Relaxed);
        }
        500..=599 => {
            stats.stream_status_5xx.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

fn complete_stream(stats: &Stats, request_started: Instant) {
    let duration_us = elapsed_us(request_started);
    stats.stream_completed.fetch_add(1, Ordering::Relaxed);
    stats
        .stream_duration_us_total
        .fetch_add(duration_us, Ordering::Relaxed);
    atomic_max(&stats.stream_duration_us_max, duration_us);
    if duration_us >= SLOW_STREAM_US {
        stats.stream_slow_completed.fetch_add(1, Ordering::Relaxed);
    }
    stats.stream_active.fetch_sub(1, Ordering::Relaxed);
}

fn stats_snapshot(state: &AppState) -> serde_json::Value {
    let stats = &state.stats;
    let started_at = stats.started_at_unix.load(Ordering::Relaxed);
    let stream_requests = stats.stream_requests.load(Ordering::Relaxed);
    let stream_completed = stats.stream_completed.load(Ordering::Relaxed);
    let stream_chunks = stats.stream_chunks.load(Ordering::Relaxed);
    let stream_bytes = stats.stream_bytes.load(Ordering::Relaxed);
    let stream_duration_us = stats.stream_duration_us_total.load(Ordering::Relaxed);
    let prefetch_completed = stats.prefetch_completed.load(Ordering::Relaxed);
    serde_json::json!({
        "startedAtUnix": started_at,
        "uptimeSeconds": now_unix().saturating_sub(started_at),
        "config": {
            "sendfileSupported": sendfile_supported(),
            "sendfileEnabled": state.sendfile_enabled,
            "uploadEnabled": state.upload_enabled,
            "uploadDir": state.upload_dir.to_string_lossy(),
            "tcpNodelay": state.tcp_nodelay,
            "socketSendBufferBytes": state.socket_send_buffer_bytes,
            "streamCacheControl": state.stream_cache_control.as_str(),
            "initialChunkBytes": state.initial_chunk_bytes,
            "readChunkBytes": state.read_chunk_bytes,
            "prefetchBytes": state.prefetch_bytes,
            "prefetchMaxTasks": state.prefetch_max_tasks,
        },
        "requests": {
            "fileList": stats.file_list_requests.load(Ordering::Relaxed),
            "meta": stats.meta_requests.load(Ordering::Relaxed),
            "upload": stats.upload_requests.load(Ordering::Relaxed),
            "uploadBytes": stats.upload_bytes.load(Ordering::Relaxed),
        },
        "stream": {
            "requests": stream_requests,
            "userspaceRequests": stats.userspace_stream_requests.load(Ordering::Relaxed),
            "sendfileRequests": stats.sendfile_requests.load(Ordering::Relaxed),
            "active": stats.stream_active.load(Ordering::Relaxed),
            "activeMax": stats.stream_active_max.load(Ordering::Relaxed),
            "headRequests": stats.stream_head_requests.load(Ordering::Relaxed),
            "rangeRequests": stats.stream_range_requests.load(Ordering::Relaxed),
            "fullRequests": stats.stream_full_requests.load(Ordering::Relaxed),
            "status200": stats.stream_status_200.load(Ordering::Relaxed),
            "status206": stats.stream_status_206.load(Ordering::Relaxed),
            "status4xx": stats.stream_status_4xx.load(Ordering::Relaxed),
            "status5xx": stats.stream_status_5xx.load(Ordering::Relaxed),
            "errors": stats.stream_errors.load(Ordering::Relaxed),
            "canceled": stats.stream_canceled.load(Ordering::Relaxed),
            "completed": stream_completed,
            "requestedBytes": stats.stream_requested_bytes.load(Ordering::Relaxed),
            "bytes": stream_bytes,
            "chunks": stream_chunks,
            "avgBytesPerRequest": avg_u64(stream_bytes, stream_completed),
            "avgBytesPerChunk": avg_u64(stream_bytes, stream_chunks),
            "avgThroughputBytesPerSecond": throughput_bps(stream_bytes, stream_duration_us),
            "avgFirstChunkUs": avg_u64(stats.stream_first_chunk_us_total.load(Ordering::Relaxed), stream_chunks.min(stream_requests)),
            "avgOpenUs": avg_u64(stats.stream_open_us_total.load(Ordering::Relaxed), stream_requests),
            "avgSeekUs": avg_u64(stats.stream_seek_us_total.load(Ordering::Relaxed), stream_requests),
            "avgSetupUs": avg_u64(stats.stream_setup_us_total.load(Ordering::Relaxed), stream_requests),
            "avgReadUs": avg_u64(stats.stream_read_us_total.load(Ordering::Relaxed), stream_chunks),
            "avgDurationUs": avg_u64(stream_duration_us, stream_completed),
            "maxFirstChunkUs": stats.stream_first_chunk_us_max.load(Ordering::Relaxed),
            "maxOpenUs": stats.stream_open_us_max.load(Ordering::Relaxed),
            "maxSeekUs": stats.stream_seek_us_max.load(Ordering::Relaxed),
            "maxSetupUs": stats.stream_setup_us_max.load(Ordering::Relaxed),
            "maxReadUs": stats.stream_read_us_max.load(Ordering::Relaxed),
            "maxDurationUs": stats.stream_duration_us_max.load(Ordering::Relaxed),
            "slowFirstChunk": stats.stream_slow_first_chunk.load(Ordering::Relaxed),
            "slowCompleted": stats.stream_slow_completed.load(Ordering::Relaxed),
        },
        "prefetch": {
            "scheduled": stats.prefetch_scheduled.load(Ordering::Relaxed),
            "active": stats.prefetch_active.load(Ordering::Relaxed),
            "completed": prefetch_completed,
            "errors": stats.prefetch_errors.load(Ordering::Relaxed),
            "bytes": stats.prefetch_bytes.load(Ordering::Relaxed),
            "skippedDisabled": stats.prefetch_skipped_disabled.load(Ordering::Relaxed),
            "skippedEof": stats.prefetch_skipped_eof.load(Ordering::Relaxed),
            "skippedDuplicate": stats.prefetch_skipped_duplicate.load(Ordering::Relaxed),
            "skippedBusy": stats.prefetch_skipped_busy.load(Ordering::Relaxed),
            "avgBytes": avg_u64(stats.prefetch_bytes.load(Ordering::Relaxed), prefetch_completed),
            "avgReadUs": avg_u64(stats.prefetch_read_us_total.load(Ordering::Relaxed), prefetch_completed),
        },
        "sendfile": {
            "supported": sendfile_supported(),
            "enabled": state.sendfile_enabled,
            "requests": stats.sendfile_requests.load(Ordering::Relaxed),
            "bytes": stats.sendfile_bytes.load(Ordering::Relaxed),
            "calls": stats.sendfile_calls.load(Ordering::Relaxed),
            "avgBytesPerCall": avg_u64(stats.sendfile_bytes.load(Ordering::Relaxed), stats.sendfile_calls.load(Ordering::Relaxed)),
            "avgCallUs": avg_u64(stats.sendfile_us_total.load(Ordering::Relaxed), stats.sendfile_calls.load(Ordering::Relaxed)),
            "maxCallUs": stats.sendfile_us_max.load(Ordering::Relaxed),
        }
    })
}

fn avg_u64(total: u64, count: u64) -> u64 {
    if count == 0 { 0 } else { total / count }
}

fn throughput_bps(bytes: u64, duration_us: u64) -> u64 {
    if duration_us == 0 {
        0
    } else {
        bytes.saturating_mul(1_000_000) / duration_us
    }
}

fn atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(previous) => current = previous,
        }
    }
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn env_var(name: &str, fallback_name: &str) -> std::result::Result<String, env::VarError> {
    env::var(name).or_else(|_| env::var(fallback_name))
}

fn env_usize(name: &str, fallback_name: &str, default: usize, min: usize, max: usize) -> usize {
    env_var(name, fallback_name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map(|value| value.clamp(min, max))
        .unwrap_or(default)
}

fn env_u64(name: &str, fallback_name: &str, default: u64, min: u64, max: u64) -> u64 {
    env_var(name, fallback_name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.clamp(min, max))
        .unwrap_or(default)
}

fn env_u32(name: &str, fallback_name: &str, default: u32, min: u32, max: u32) -> u32 {
    env_var(name, fallback_name)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .map(|value| value.clamp(min, max))
        .unwrap_or(default)
}

fn env_bool(name: &str, fallback_name: &str, default: bool) -> bool {
    env_var(name, fallback_name)
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn sendfile_supported() -> bool {
    cfg!(all(target_os = "linux", target_arch = "x86_64"))
}

fn safe_name(value: &str) -> String {
    let basename = Path::new(value)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("upload");
    let cleaned = basename
        .chars()
        .map(|ch| match ch {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '-',
            _ => ch,
        })
        .collect::<String>()
        .trim_matches('.')
        .trim()
        .to_string();
    if cleaned.is_empty() {
        "upload".to_string()
    } else {
        cleaned
    }
}

fn is_media_name(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| MEDIA_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
}

fn encode_path(path: &str) -> String {
    URL_SAFE_NO_PAD.encode(path.as_bytes())
}

fn decode_path(value: &str) -> Result<PathBuf, AppError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .map_err(|_| AppError::new(StatusCode::BAD_REQUEST, "invalid encoded path"))?;
    let path = String::from_utf8(decoded)
        .map_err(|_| AppError::new(StatusCode::BAD_REQUEST, "invalid encoded path"))?;
    if path.is_empty() {
        return Err(AppError::new(StatusCode::BAD_REQUEST, "empty path"));
    }
    Ok(PathBuf::from(path))
}

async fn ensure_streamable_path(encoded_path: &str) -> Result<PathBuf, AppError> {
    let path = decode_path(encoded_path)?;
    if !is_media_name(&path) {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "unsupported media extension",
        ));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| AppError::new(StatusCode::NOT_FOUND, "media file not found"))?;
    let metadata = fs::metadata(&canonical)
        .await
        .map_err(|_| AppError::new(StatusCode::NOT_FOUND, "media file not found"))?;
    if !metadata.is_file() {
        return Err(AppError::new(StatusCode::NOT_FOUND, "media file not found"));
    }
    Ok(canonical)
}

async fn file_meta(path: PathBuf) -> Result<MediaMeta, AppError> {
    let metadata = fs::metadata(&path)
        .await
        .map_err(|_| AppError::new(StatusCode::NOT_FOUND, "media file not found"))?;
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let path_string = path.to_string_lossy().to_string();
    let id = encode_path(&path_string);
    Ok(MediaMeta {
        id: id.clone(),
        path: path_string,
        name: path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("media")
            .to_string(),
        content_type: mime_guess::from_path(&path)
            .first_or_octet_stream()
            .to_string(),
        size: metadata.len(),
        mtime,
        stream_url: format!("/api/stream/{id}"),
    })
}

async fn unique_upload_path(upload_dir: &Path, filename: &str) -> Result<PathBuf, AppError> {
    let stem = Path::new(filename)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("media");
    let suffix = Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| format!(".{value}"))
        .unwrap_or_default();
    let mut candidate = upload_dir.join(format!("{stem}{suffix}"));
    let mut index = 2;
    while fs::try_exists(&candidate).await.map_err(|_| {
        AppError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to check upload path",
        )
    })? {
        candidate = upload_dir.join(format!("{stem}-{index}{suffix}"));
        index += 1;
    }
    Ok(candidate)
}

fn parse_range(range_header: Option<&str>, total: u64) -> Result<(StatusCode, u64, u64), AppError> {
    if total == 0 {
        return Ok((StatusCode::OK, 0, 0));
    }
    let Some(range) = range_header else {
        return Ok((StatusCode::OK, 0, total - 1));
    };
    let Some(raw) = range.trim().strip_prefix("bytes=") else {
        return Err(
            AppError::new(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range")
                .with_header(CONTENT_RANGE, format!("bytes */{total}")),
        );
    };
    let Some((raw_start, raw_end)) = raw.split_once('-') else {
        return Err(
            AppError::new(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range")
                .with_header(CONTENT_RANGE, format!("bytes */{total}")),
        );
    };

    let (start, end) = if raw_start.is_empty() {
        let length = raw_end.parse::<u64>().unwrap_or(0);
        if length == 0 {
            return Err(
                AppError::new(StatusCode::RANGE_NOT_SATISFIABLE, "range not satisfiable")
                    .with_header(CONTENT_RANGE, format!("bytes */{total}")),
            );
        }
        (total.saturating_sub(length), total - 1)
    } else {
        let start = raw_start.parse::<u64>().map_err(|_| {
            AppError::new(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range")
                .with_header(CONTENT_RANGE, format!("bytes */{total}"))
        })?;
        let end = if raw_end.is_empty() {
            total - 1
        } else {
            raw_end.parse::<u64>().map_err(|_| {
                AppError::new(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range")
                    .with_header(CONTENT_RANGE, format!("bytes */{total}"))
            })?
        };
        (start, end.min(total - 1))
    };

    if start >= total || end < start {
        return Err(
            AppError::new(StatusCode::RANGE_NOT_SATISFIABLE, "range not satisfiable")
                .with_header(CONTENT_RANGE, format!("bytes */{total}")),
        );
    }
    Ok((StatusCode::PARTIAL_CONTENT, start, end))
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
async fn run_sendfile_http(state: AppState, public_dir: PathBuf, addr: SocketAddr) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind sendfile server")?;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("accept sendfile connection")?;
        let state = state.clone();
        let public_dir = public_dir.clone();
        tokio::task::spawn_blocking(move || {
            if let Ok(stream) = stream.into_std() {
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_nodelay(state.tcp_nodelay);
                if state.socket_send_buffer_bytes > 0 {
                    set_socket_send_buffer(&stream, state.socket_send_buffer_bytes);
                }
                let _ = handle_sendfile_connection(stream, state, public_dir);
            }
        });
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
struct SimpleRequest {
    method: String,
    path: String,
    headers: std::collections::HashMap<String, String>,
    body: Vec<u8>,
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn handle_sendfile_connection(
    mut stream: std::net::TcpStream,
    state: AppState,
    public_dir: PathBuf,
) -> std::io::Result<()> {
    let request = match read_simple_request(&mut stream) {
        Ok(request) => request,
        Err(_) => {
            write_text_response(&mut stream, StatusCode::BAD_REQUEST, "bad request\n")?;
            return Ok(());
        }
    };
    let path = request.path.split('?').next().unwrap_or("/").to_string();
    match (request.method.as_str(), path.as_str()) {
        ("OPTIONS", _) => write_empty_response(&mut stream, StatusCode::NO_CONTENT),
        ("GET", "/health") => write_json_response(&mut stream, health_json(&state)),
        ("GET", "/api/config") => write_json_response(&mut stream, config_json(&state)),
        ("GET", "/api/stats") => write_json_response(&mut stream, stats_snapshot(&state)),
        ("POST", "/api/stats/reset") => {
            state.stats.reset();
            write_json_response(&mut stream, stats_snapshot(&state))
        }
        ("GET", "/api/files") => {
            state
                .stats
                .file_list_requests
                .fetch_add(1, Ordering::Relaxed);
            if !state.upload_enabled {
                return write_json_response(&mut stream, serde_json::json!({ "files": [] }));
            }
            write_json_response(
                &mut stream,
                serde_json::json!({ "files": list_demo_uploads_sync(&state.upload_dir) }),
            )
        }
        ("POST", "/api/upload") | ("POST", "/upload") => {
            handle_sendfile_upload(&mut stream, &state, &request)
        }
        ("GET", "/") => send_static_file(&mut stream, &public_dir.join("index.html"), false),
        ("GET", "/media-stream-widget.js") => {
            send_static_file(&mut stream, &public_dir.join("media-stream-widget.js"), false)
        }
        ("GET", "/media-stream-widget.css") => {
            send_static_file(&mut stream, &public_dir.join("media-stream-widget.css"), false)
        }
        ("HEAD", _) if path.starts_with("/api/stream/") => {
            handle_sendfile_stream(&mut stream, &state, &request, true)
        }
        ("GET", _) if path.starts_with("/api/stream/") => {
            handle_sendfile_stream(&mut stream, &state, &request, false)
        }
        ("HEAD", _) if path.starts_with("/files/") && path.ends_with("/stream") => {
            handle_sendfile_stream(&mut stream, &state, &request, true)
        }
        ("GET", _) if path.starts_with("/files/") && path.ends_with("/stream") => {
            handle_sendfile_stream(&mut stream, &state, &request, false)
        }
        ("GET", _) if path.starts_with("/api/meta/") => {
            let encoded = path.trim_start_matches("/api/meta/");
            match ensure_streamable_path_sync(encoded).and_then(file_meta_sync) {
                Ok(meta) => {
                    write_json_response(&mut stream, serde_json::to_value(meta).unwrap_or_default())
                }
                Err(error) => write_json_error(&mut stream, error.status, &error.detail),
            }
        }
        ("GET", _) if path.starts_with("/assets/") => {
            let relative = path.trim_start_matches("/assets/").trim_start_matches('/');
            let target = public_dir.join(relative);
            match target.canonicalize() {
                Ok(canonical) if canonical.starts_with(&public_dir) => {
                    send_static_file(&mut stream, &canonical, false)
                }
                _ => write_json_error(&mut stream, StatusCode::NOT_FOUND, "asset not found"),
            }
        }
        _ => write_json_error(&mut stream, StatusCode::NOT_FOUND, "not found"),
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn read_simple_request(stream: &mut std::net::TcpStream) -> std::io::Result<SimpleRequest> {
    use std::io::Read;

    let mut buffer = Vec::with_capacity(8192);
    let mut temp = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut temp)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof",
            ));
        }
        buffer.extend_from_slice(&temp[..read]);
        if let Some(index) = find_bytes(&buffer, b"\r\n\r\n") {
            break index + 4;
        }
        if buffer.len() > 64 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "headers too large",
            ));
        }
    };
    let headers_text = String::from_utf8_lossy(&buffer[..header_end]);
    let mut lines = headers_text.split("\r\n");
    let request_line = lines.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing request line")
    })?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or("").to_string();
    let path = request_parts.next().unwrap_or("/").to_string();
    let mut headers = std::collections::HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buffer[header_end..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut temp)?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&temp[..read]);
    }
    body.truncate(content_length);
    Ok(SimpleRequest {
        method,
        path,
        headers,
        body,
    })
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn handle_sendfile_stream(
    stream: &mut std::net::TcpStream,
    state: &AppState,
    request: &SimpleRequest,
    is_head: bool,
) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::fd::AsRawFd;

    let request_started = Instant::now();
    state.stats.stream_requests.fetch_add(1, Ordering::Relaxed);
    state
        .stats
        .sendfile_requests
        .fetch_add(1, Ordering::Relaxed);
    let active = state.stats.stream_active.fetch_add(1, Ordering::Relaxed) + 1;
    atomic_max(&state.stats.stream_active_max, active);
    if is_head {
        state
            .stats
            .stream_head_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    let encoded = encoded_from_stream_path(&request.path);
    let Some(encoded) = encoded else {
        record_stream_status(&state.stats, StatusCode::NOT_FOUND);
        state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
        state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
        return write_json_error(stream, StatusCode::NOT_FOUND, "not found");
    };
    let path = match ensure_streamable_path_sync(encoded) {
        Ok(path) => path,
        Err(error) => {
            record_stream_status(&state.stats, error.status);
            state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return write_json_error(stream, error.status, &error.detail);
        }
    };
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(_) => {
            record_stream_status(&state.stats, StatusCode::NOT_FOUND);
            state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return write_json_error(stream, StatusCode::NOT_FOUND, "media file not found");
        }
    };
    let total = metadata.len();
    let range_header = request.headers.get("range").map(String::as_str);
    if range_header.is_some() {
        state
            .stats
            .stream_range_requests
            .fetch_add(1, Ordering::Relaxed);
    } else {
        state
            .stats
            .stream_full_requests
            .fetch_add(1, Ordering::Relaxed);
    }
    let (status, start, end) = match parse_range(range_header, total) {
        Ok(range) => range,
        Err(error) => {
            record_stream_status(&state.stats, error.status);
            state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return write_json_error(stream, error.status, &error.detail);
        }
    };
    let length = end
        .saturating_sub(start)
        .saturating_add(if total == 0 { 0 } else { 1 });
    state
        .stats
        .stream_requested_bytes
        .fetch_add(length, Ordering::Relaxed);
    let content_type = mime_guess::from_path(&path)
        .first_or_octet_stream()
        .to_string();
    let mut headers = vec![
        ("Accept-Ranges".to_string(), "bytes".to_string()),
        (
            "Cache-Control".to_string(),
            state.stream_cache_control.to_string(),
        ),
        ("Content-Type".to_string(), content_type),
        ("Content-Length".to_string(), length.to_string()),
        ("Connection".to_string(), "close".to_string()),
        (
            "X-Media-Path-Encoded".to_string(),
            encode_path(path.to_string_lossy().as_ref()),
        ),
    ];
    if status == StatusCode::PARTIAL_CONTENT {
        headers.push((
            "Content-Range".to_string(),
            format!("bytes {start}-{end}/{total}"),
        ));
    }
    write_response_head(stream, status, &headers)?;
    record_stream_status(&state.stats, status);
    let setup_us = elapsed_us(request_started);
    state
        .stats
        .stream_setup_us_total
        .fetch_add(setup_us, Ordering::Relaxed);
    atomic_max(&state.stats.stream_setup_us_max, setup_us);
    if is_head || length == 0 {
        complete_stream(&state.stats, request_started);
        return Ok(());
    }

    let open_started = Instant::now();
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(_) => {
            state.stats.stream_errors.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return Ok(());
        }
    };
    let open_us = elapsed_us(open_started);
    state
        .stats
        .stream_open_us_total
        .fetch_add(open_us, Ordering::Relaxed);
    atomic_max(&state.stats.stream_open_us_max, open_us);
    spawn_prefetch_sync(state, path.clone(), end.saturating_add(1), total);

    let out_fd = stream.as_raw_fd();
    let in_fd = file.as_raw_fd();
    let mut offset = start as libc::off_t;
    let mut remaining = length;
    let mut first = true;
    while remaining > 0 {
        let current_chunk = if first {
            state.initial_chunk_bytes
        } else {
            state.read_chunk_bytes
        };
        let to_send = remaining.min(current_chunk as u64) as usize;
        let call_started = Instant::now();
        let sent = unsafe { libc::sendfile(out_fd, in_fd, &mut offset, to_send) };
        let call_us = elapsed_us(call_started);
        state
            .stats
            .sendfile_us_total
            .fetch_add(call_us, Ordering::Relaxed);
        atomic_max(&state.stats.sendfile_us_max, call_us);
        state.stats.sendfile_calls.fetch_add(1, Ordering::Relaxed);
        if sent < 0 {
            let error = std::io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(libc::EINTR) | Some(libc::EAGAIN)) {
                continue;
            }
            state.stats.stream_canceled.fetch_add(1, Ordering::Relaxed);
            state.stats.stream_active.fetch_sub(1, Ordering::Relaxed);
            return Ok(());
        }
        if sent == 0 {
            break;
        }
        let sent = sent as u64;
        if first {
            let first_chunk_us = elapsed_us(request_started);
            state
                .stats
                .stream_first_chunk_us_total
                .fetch_add(first_chunk_us, Ordering::Relaxed);
            atomic_max(&state.stats.stream_first_chunk_us_max, first_chunk_us);
            if first_chunk_us >= SLOW_FIRST_CHUNK_US {
                state
                    .stats
                    .stream_slow_first_chunk
                    .fetch_add(1, Ordering::Relaxed);
            }
            first = false;
        }
        state.stats.stream_bytes.fetch_add(sent, Ordering::Relaxed);
        state
            .stats
            .sendfile_bytes
            .fetch_add(sent, Ordering::Relaxed);
        state.stats.stream_chunks.fetch_add(1, Ordering::Relaxed);
        state
            .stats
            .stream_read_us_total
            .fetch_add(call_us, Ordering::Relaxed);
        atomic_max(&state.stats.stream_read_us_max, call_us);
        remaining = remaining.saturating_sub(sent);
    }
    stream.flush()?;
    complete_stream(&state.stats, request_started);
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn encoded_from_stream_path(path: &str) -> Option<&str> {
    if let Some(encoded) = path.strip_prefix("/api/stream/") {
        return Some(encoded);
    }
    path.strip_prefix("/files/")
        .and_then(|rest| rest.strip_suffix("/stream"))
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn spawn_prefetch_sync(state: &AppState, path: PathBuf, start: u64, total: u64) {
    if state.prefetch_bytes == 0 {
        state
            .stats
            .prefetch_skipped_disabled
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    if start >= total {
        state
            .stats
            .prefetch_skipped_eof
            .fetch_add(1, Ordering::Relaxed);
        return;
    }

    let key = path.to_string_lossy().to_string();
    {
        let mut paths = state.prefetch_paths.blocking_lock();
        if !paths.insert(key.clone()) {
            state
                .stats
                .prefetch_skipped_duplicate
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
    }
    state
        .stats
        .prefetch_scheduled
        .fetch_add(1, Ordering::Relaxed);

    let state = state.clone();
    std::thread::spawn(move || {
        let permit = match state.prefetch_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                state
                    .stats
                    .prefetch_skipped_busy
                    .fetch_add(1, Ordering::Relaxed);
                let mut paths = state.prefetch_paths.blocking_lock();
                paths.remove(&key);
                return;
            }
        };
        state.stats.prefetch_active.fetch_add(1, Ordering::Relaxed);
        let length = state.prefetch_bytes.min(total.saturating_sub(start));
        let prefetch_started = Instant::now();
        match prefetch_range_sync(&path, start, length, state.read_chunk_bytes) {
            Ok(bytes_read) => {
                state
                    .stats
                    .prefetch_completed
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .stats
                    .prefetch_bytes
                    .fetch_add(bytes_read, Ordering::Relaxed);
                state
                    .stats
                    .prefetch_read_us_total
                    .fetch_add(elapsed_us(prefetch_started), Ordering::Relaxed);
            }
            Err(_) => {
                state.stats.prefetch_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(permit);
        state.stats.prefetch_active.fetch_sub(1, Ordering::Relaxed);
        let mut paths = state.prefetch_paths.blocking_lock();
        paths.remove(&key);
    });
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn prefetch_range_sync(
    path: &Path,
    start: u64,
    length: u64,
    chunk_size: usize,
) -> std::io::Result<u64> {
    use std::io::{Read, Seek};

    if length == 0 {
        return Ok(0);
    }
    let mut file = std::fs::File::open(path)?;
    file.seek(std::io::SeekFrom::Start(start))?;
    let mut remaining = length;
    let mut total_read = 0;
    let mut buffer = vec![0; chunk_size.max(1)];
    while remaining > 0 {
        let read_len = (buffer.len() as u64).min(remaining) as usize;
        let bytes_read = file.read(&mut buffer[..read_len])?;
        if bytes_read == 0 {
            break;
        }
        total_read += bytes_read as u64;
        remaining = remaining.saturating_sub(bytes_read as u64);
    }
    Ok(total_read)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn set_socket_send_buffer(stream: &std::net::TcpStream, bytes: u32) {
    use std::os::fd::AsRawFd;

    let value = bytes as libc::c_int;
    unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            (&value as *const libc::c_int).cast(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        );
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn handle_sendfile_upload(
    stream: &mut std::net::TcpStream,
    state: &AppState,
    request: &SimpleRequest,
) -> std::io::Result<()> {
    state.stats.upload_requests.fetch_add(1, Ordering::Relaxed);
    if !state.upload_enabled {
        return write_json_error(stream, StatusCode::FORBIDDEN, "demo upload is disabled");
    }
    let content_type = request
        .headers
        .get("content-type")
        .map(String::as_str)
        .unwrap_or("");
    let Some(boundary) = content_type
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("boundary="))
    else {
        return write_json_error(
            stream,
            StatusCode::BAD_REQUEST,
            "multipart boundary is required",
        );
    };
    let marker = format!("--{boundary}").into_bytes();
    let Some(marker_start) = find_bytes(&request.body, &marker) else {
        return write_json_error(
            stream,
            StatusCode::BAD_REQUEST,
            "multipart file is required",
        );
    };
    let part_start = marker_start + marker.len();
    let part = request.body.get(part_start..).unwrap_or_default();
    let Some(header_end) = find_bytes(part, b"\r\n\r\n") else {
        return write_json_error(stream, StatusCode::BAD_REQUEST, "invalid multipart file");
    };
    let part_headers = String::from_utf8_lossy(&part[..header_end]);
    if !part_headers.contains("name=\"file\"") {
        return write_json_error(stream, StatusCode::BAD_REQUEST, "file field is required");
    }
    let filename = part_headers
        .split("filename=\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .map(safe_name)
        .unwrap_or_else(|| "upload".to_string());
    if !is_media_name(Path::new(&filename)) {
        return write_json_error(
            stream,
            StatusCode::BAD_REQUEST,
            "unsupported media extension",
        );
    }
    let mut data = &part[header_end + 4..];
    if let Some(end) = find_bytes(data, &marker) {
        data = &data[..end];
    }
    if data.ends_with(b"\r\n") {
        data = &data[..data.len().saturating_sub(2)];
    }
    let target = unique_upload_path_sync(&state.upload_dir, &filename)?;
    std::fs::write(&target, data)?;
    state
        .stats
        .upload_bytes
        .fetch_add(data.len() as u64, Ordering::Relaxed);
    let canonical = target.canonicalize()?;
    match file_meta_sync(canonical) {
        Ok(meta) => write_json_response(stream, serde_json::to_value(meta).unwrap_or_default()),
        Err(error) => write_json_error(stream, error.status, &error.detail),
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn send_static_file(
    stream: &mut std::net::TcpStream,
    path: &Path,
    is_head: bool,
) -> std::io::Result<()> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => metadata,
        _ => return write_json_error(stream, StatusCode::NOT_FOUND, "file not found"),
    };
    let content_type = mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string();
    let headers = vec![
        ("Content-Type".to_string(), content_type),
        ("Content-Length".to_string(), metadata.len().to_string()),
        ("Connection".to_string(), "close".to_string()),
    ];
    write_response_head(stream, StatusCode::OK, &headers)?;
    if is_head {
        return Ok(());
    }
    let body = std::fs::read(path)?;
    std::io::Write::write_all(stream, &body)?;
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn write_json_response(
    stream: &mut std::net::TcpStream,
    value: serde_json::Value,
) -> std::io::Result<()> {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    write_response(stream, StatusCode::OK, "application/json", &body)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn write_json_error(
    stream: &mut std::net::TcpStream,
    status: StatusCode,
    detail: &str,
) -> std::io::Result<()> {
    let body = serde_json::to_vec(&serde_json::json!({ "detail": detail }))
        .unwrap_or_else(|_| b"{\"detail\":\"error\"}".to_vec());
    write_response(stream, status, "application/json", &body)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn write_text_response(
    stream: &mut std::net::TcpStream,
    status: StatusCode,
    text: &str,
) -> std::io::Result<()> {
    write_response(stream, status, "text/plain; charset=utf-8", text.as_bytes())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn write_empty_response(
    stream: &mut std::net::TcpStream,
    status: StatusCode,
) -> std::io::Result<()> {
    write_response(stream, status, "text/plain", b"")
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn write_response(
    stream: &mut std::net::TcpStream,
    status: StatusCode,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let headers = vec![
        ("Content-Type".to_string(), content_type.to_string()),
        ("Content-Length".to_string(), body.len().to_string()),
        ("Connection".to_string(), "close".to_string()),
    ];
    write_response_head(stream, status, &headers)?;
    std::io::Write::write_all(stream, body)?;
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn write_response_head(
    stream: &mut std::net::TcpStream,
    status: StatusCode,
    headers: &[(String, String)],
) -> std::io::Result<()> {
    use std::io::Write;

    write!(
        stream,
        "HTTP/1.1 {} {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, HEAD, POST, OPTIONS\r\nAccess-Control-Allow-Headers: *\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("OK")
    )?;
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn health_json(state: &AppState) -> serde_json::Value {
    serde_json::json!({
        "ok": true,
        "mode": "path-stream",
        "runtime": "rust",
        "sendfileEnabled": state.sendfile_enabled,
        "dataDir": state.data_dir.to_string_lossy(),
        "uploadEnabled": state.upload_enabled,
        "uploadDir": state.upload_dir.to_string_lossy(),
        "initialChunkBytes": state.initial_chunk_bytes,
        "readChunkBytes": state.read_chunk_bytes,
        "prefetchBytes": state.prefetch_bytes,
        "prefetchMaxTasks": state.prefetch_max_tasks,
    })
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn config_json(state: &AppState) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "0.3.0",
        "mode": "path-stream",
        "runtime": "rust",
        "sendfileSupported": sendfile_supported(),
        "sendfileEnabled": state.sendfile_enabled,
        "uploadEnabled": state.upload_enabled,
        "uploadDir": state.upload_dir.to_string_lossy(),
        "initialChunkBytes": state.initial_chunk_bytes,
        "readChunkBytes": state.read_chunk_bytes,
        "prefetchBytes": state.prefetch_bytes,
        "prefetchMaxTasks": state.prefetch_max_tasks,
        "mediaExtensions": MEDIA_EXTENSIONS,
        "audioExtensions": ["aac", "flac", "m4a", "mp3", "ogg", "opus", "wav", "webm"],
        "videoExtensions": ["m4v", "mkv", "mov", "mp4", "webm"],
        "endpoints": {
            "stream": "/api/stream/{base64urlPath}",
            "meta": "/api/meta/{base64urlPath}",
            "stats": "/api/stats",
            "resetStats": "/api/stats/reset",
            "upload": "/api/upload",
            "demoFiles": "/api/files"
        }
    })
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn list_demo_uploads_sync(upload_dir: &Path) -> Vec<MediaMeta> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(upload_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && is_media_name(&path) {
                if let Ok(meta) = file_meta_sync(path) {
                    files.push(meta);
                }
            }
        }
    }
    files.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    files
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn ensure_streamable_path_sync(encoded_path: &str) -> Result<PathBuf, AppError> {
    let path = decode_path(encoded_path)?;
    if !is_media_name(&path) {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "unsupported media extension",
        ));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| AppError::new(StatusCode::NOT_FOUND, "media file not found"))?;
    let metadata = std::fs::metadata(&canonical)
        .map_err(|_| AppError::new(StatusCode::NOT_FOUND, "media file not found"))?;
    if !metadata.is_file() {
        return Err(AppError::new(StatusCode::NOT_FOUND, "media file not found"));
    }
    Ok(canonical)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn file_meta_sync(path: PathBuf) -> Result<MediaMeta, AppError> {
    let metadata = std::fs::metadata(&path)
        .map_err(|_| AppError::new(StatusCode::NOT_FOUND, "media file not found"))?;
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let path_string = path.to_string_lossy().to_string();
    let id = encode_path(&path_string);
    Ok(MediaMeta {
        id: id.clone(),
        path: path_string,
        name: path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("media")
            .to_string(),
        content_type: mime_guess::from_path(&path)
            .first_or_octet_stream()
            .to_string(),
        size: metadata.len(),
        mtime,
        stream_url: format!("/api/stream/{id}"),
    })
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn unique_upload_path_sync(upload_dir: &Path, filename: &str) -> std::io::Result<PathBuf> {
    let stem = Path::new(filename)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("media");
    let suffix = Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| format!(".{value}"))
        .unwrap_or_default();
    let mut candidate = upload_dir.join(format!("{stem}{suffix}"));
    let mut index = 2;
    while candidate.exists() {
        candidate = upload_dir.join(format!("{stem}-{index}{suffix}"));
        index += 1;
    }
    Ok(candidate)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
