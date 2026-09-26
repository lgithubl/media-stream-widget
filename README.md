# Media Stream Widget

Embeddable audio frontend plus a deployable Rust streaming backend.

The frontend uses native `<audio>` playback. Large files are streamed by URL; the
server supports byte ranges so MP3/WAV files can seek without downloading the
whole file. The same backend stream API also supports common video containers;
an embedding site can use the returned stream URL in a native `<video>` element.

## Local

```bash
cd media-stream-widget
cargo run
```

Open `http://127.0.0.1:8080`.

## Embed

```html
<link rel="stylesheet" href="/media-stream-widget.css">
<div id="media-stream-widget"></div>
<script src="/media-stream-widget.js"></script>
<script>
MediaStreamWidget.mount("#media-stream-widget", {
  apiBase: "http://127.0.0.1:8080",
  tracks: [
    { name: "example.mp3", path: "/absolute/path/to/example.mp3" },
    { name: "example.mp4", path: "/absolute/path/to/example.mp4" }
  ],
  uploadEnabled: true
});
</script>
```

## Video Example

Video uses the same backend stream API. If the file extension is `mp4`, `m4v`,
`mov`, `mkv`, or `webm`, the widget renders a native `<video controls>` player.

```html
<link rel="stylesheet" href="/media-stream-widget.css">
<div id="video-widget"></div>
<script src="/media-stream-widget.js"></script>
<script>
MediaStreamWidget.mount("#video-widget", {
  apiBase: "http://127.0.0.1:8080",
  tracks: [
    {
      name: "movie.mp4",
      path: "/absolute/path/to/movie.mp4"
    },
    {
      name: "clip.webm",
      path: "/absolute/path/to/clip.webm"
    }
  ],
  uploadEnabled: false
});
</script>
```

For best browser compatibility, use codecs the browser can play natively. MP4
with front-loaded metadata, often called faststart, gives faster first playback.

External sites can host `media-stream-widget.js` and `media-stream-widget.css` themselves, or
load them from this backend. Only `apiBase` has to point at the deployed backend.
If the upper layer already encoded the path, pass it as `id` instead of `path`.

## Docker

```bash
docker run --rm -p 8080:8080 -v "$PWD/audio-data:/media:ro" ghcr.io/lgithubl/media-stream-widget:latest
```

Production media directories are read-only inputs. Mount the directory that
contains real audio/video files as read-only and pass those absolute paths from
the upper layer; this service must not create, rename, delete, or write files in
that media directory.

Useful runtime tuning variables:

- `MEDIA_STREAM_HOST`: bind host. Defaults to `0.0.0.0`.
- `MEDIA_STREAM_PORT`: bind port. Defaults to `8080`.
- `MEDIA_STREAM_DATA_DIR`: logical data directory used by the demo file list.
  Defaults to `/tmp/media-stream-widget`.
- `MEDIA_STREAM_INITIAL_CHUNK_BYTES`: first response chunk size. Defaults to
  `262144` bytes so slow disks can start playback sooner.
- `MEDIA_STREAM_READ_CHUNK_BYTES`: later stream chunk size. Defaults to
  `1048576` bytes and is clamped between 64KB and 16MB.
- `MEDIA_STREAM_PREFETCH_BYTES`: bytes to read ahead after the requested range.
  Defaults to `8388608` bytes. Set to `0` to disable prefetch.
- `MEDIA_STREAM_PREFETCH_MAX_TASKS`: max concurrent background prefetch tasks.
  Defaults to `2`.
- `MEDIA_STREAM_SENDFILE_ENABLED`: Linux x86_64 plain-HTTP mode that serves
  `/api/stream/*` with `sendfile(2)`. Defaults to `true` on supported Linux
  x86_64 HTTP deployments. Set to `false` to fall back to userspace streaming.
- `MEDIA_STREAM_UPLOAD_ENABLED`: enables the demo upload endpoint. Defaults to
  `false`; keep it disabled for production media directories.
- `MEDIA_STREAM_UPLOAD_DIR`: writable directory for demo uploads when uploads
  are enabled. Defaults to `/tmp/media-stream-widget/uploads` in the image.
- `MEDIA_STREAM_TCP_NODELAY`: toggles `TCP_NODELAY` for the Linux sendfile
  server. Defaults to `true`; set to `false` to let the OS coalesce writes.
- `MEDIA_STREAM_SOCKET_SEND_BUFFER_BYTES`: optional `SO_SNDBUF` size for the
  Linux sendfile server. Defaults to `0`, which leaves the OS default.
- `MEDIA_STREAM_CACHE_CONTROL`: `Cache-Control` header for media streams.
  Defaults to `no-store`.

Older `AUDIO_WIDGET_*` variables are still accepted as fallbacks. If both names
are set, `MEDIA_STREAM_*` wins.

The GitHub workflow publishes:

```text
ghcr.io/lgithubl/media-stream-widget:latest
```

## API

- `GET /health`
- `GET /api/stream/{base64urlPath}`
- `GET /api/meta/{base64urlPath}`
- `GET /api/stats`
- `POST /api/stats/reset`
- `POST /api/upload` with multipart field `file`
- `GET /api/files` for demo uploads

The included backend is a stateless streaming service. The upper layer owns
security, indexes, permissions, and metadata. The backend accepts a base64url
encoded absolute file path and streams that file with HTTP Range support.
Runtime stats are kept in memory only and reset on process restart. They include
stream byte counts, chunk counts, average open/seek/setup/first-chunk timings,
active stream counts, HTTP status buckets, requested vs sent bytes, read
latency, duration, throughput, canceled streams, upload bytes, and prefetch
activity so production runs can expose whether the bottleneck is request setup,
disk reads, network transfer, client cancellation, or prefetch pressure.

Supported media extensions include `mp3`, `wav`, `flac`, `m4a`, `aac`, `ogg`,
`opus`, `webm`, `mp4`, `m4v`, `mov`, and `mkv`.

Demo uploads are disabled by default. If enabled, they must use
`MEDIA_STREAM_UPLOAD_DIR`; they are not the production data model and should not
point at the real media directory.

For faster first playback, keep `MEDIA_STREAM_INITIAL_CHUNK_BYTES` modest,
enable a small prefetch window such as 8MB or 16MB for slow NAS disks, and make
sure MP4 files are written in faststart form so their metadata is near the front
of the file.
