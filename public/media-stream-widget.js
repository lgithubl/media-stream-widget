(function () {
  "use strict";

  const MEDIA_EXTENSIONS = /\.(mp3|wav|m4a|aac|flac|ogg|opus|webm|mp4|m4v|mov|mkv)$/i;
  const VIDEO_EXTENSIONS = /\.(mp4|m4v|mov|mkv|webm)$/i;

  function formatBytes(value) {
    const size = Number(value || 0);
    if (!size) return "";
    if (size < 1024) return `${size} B`;
    const units = ["KB", "MB", "GB", "TB"];
    let scaled = size / 1024;
    let index = 0;
    while (scaled >= 1024 && index < units.length - 1) {
      scaled /= 1024;
      index += 1;
    }
    return `${scaled.toFixed(scaled >= 10 ? 1 : 2)} ${units[index]}`;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function joinUrl(base, path) {
    return `${String(base || "").replace(/\/+$/, "")}${path}`;
  }

  function encodePath(path) {
    const bytes = new TextEncoder().encode(path);
    let binary = "";
    bytes.forEach((byte) => {
      binary += String.fromCharCode(byte);
    });
    return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  }

  async function requestJson(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(`HTTP ${response.status}: ${text || response.statusText}`);
    }
    return response.json();
  }

  function createWidget(target, options = {}) {
    const root = typeof target === "string" ? document.querySelector(target) : target;
    if (!root) throw new Error("MediaStreamWidget target not found");

    const config = {
      apiBase: options.apiBase || location.origin,
      autoUploadEnabled: typeof options.uploadEnabled !== "boolean",
      uploadEnabled: typeof options.uploadEnabled === "boolean" ? options.uploadEnabled : false,
      tracks: Array.isArray(options.tracks) ? options.tracks : null,
    };
    const state = {
      files: [],
      selected: null,
      busy: false,
      message: "",
    };

    function streamUrl(file) {
      const encodedPath = file.id || encodePath(file.path || "");
      return joinUrl(config.apiBase, `/api/stream/${encodeURIComponent(encodedPath)}`);
    }

    function normalizeTrack(track) {
      const path = track.path || track.filePath || "";
      return {
        id: track.id || (path ? encodePath(path) : ""),
        name: track.name || track.title || (path ? path.split(/[\\/]/).pop() : "audio"),
        path,
        size: track.size || 0,
        contentType: track.contentType || "",
      };
    }

    function mediaElement(file) {
      const tag = VIDEO_EXTENSIONS.test(file.name || file.path || "") ? "video" : "audio";
      return `<${tag} controls preload="metadata" src="${escapeHtml(streamUrl(file))}"></${tag}>`;
    }

    function render() {
      const selected = state.selected;
      root.innerHTML = `
        <div class="aw-root">
          <div class="aw-toolbar">
            <input type="file" data-aw-file accept="audio/*,video/*" ${config.uploadEnabled ? "" : "disabled"}>
            <button class="aw-button" data-aw-upload ${state.busy || !config.uploadEnabled ? "disabled" : ""}>Upload</button>
            <button class="aw-button" data-aw-refresh ${state.busy ? "disabled" : ""}>Refresh</button>
          </div>
          <div class="aw-message">${escapeHtml(state.message)}</div>
          ${selected ? `
            <section class="aw-player">
              <strong>${escapeHtml(selected.name)}</strong>
              ${mediaElement(selected)}
            </section>
          ` : ""}
          <section class="aw-list">
            ${state.files.map((file) => `
              <div class="aw-row">
                <span class="aw-name" title="${escapeHtml(file.name)}">${escapeHtml(file.name)}</span>
                <span class="aw-meta">${escapeHtml(formatBytes(file.size))}</span>
                <button class="aw-button" data-aw-play="${escapeHtml(file.id)}">Play</button>
              </div>
            `).join("") || '<div class="aw-message">No media files yet.</div>'}
          </section>
        </div>
      `;

      root.querySelector("[data-aw-refresh]")?.addEventListener("click", refresh);
      root.querySelector("[data-aw-upload]")?.addEventListener("click", upload);
      root.querySelectorAll("[data-aw-play]").forEach((button) => {
        button.addEventListener("click", () => {
          state.selected = state.files.find((file) => file.id === button.dataset.awPlay) || null;
          render();
        });
      });
    }

    async function refresh() {
      state.busy = true;
      state.message = "Loading files...";
      render();
      try {
        if (config.autoUploadEnabled) {
          const health = await requestJson(joinUrl(config.apiBase, "/health"));
          config.uploadEnabled = !!health.uploadEnabled;
        }
        if (config.tracks) {
          state.files = config.tracks.map(normalizeTrack).filter((file) => file.id && MEDIA_EXTENSIONS.test(file.name || file.path || ""));
        } else {
          const data = await requestJson(joinUrl(config.apiBase, "/api/files"));
          state.files = (data.files || []).filter((file) => MEDIA_EXTENSIONS.test(file.name || ""));
        }
        if (state.selected && !state.files.some((file) => file.id === state.selected.id)) {
          state.selected = null;
        }
        state.message = `Loaded ${state.files.length} media file(s).`;
      } catch (error) {
        state.message = error.message || String(error);
      } finally {
        state.busy = false;
        render();
      }
    }

    async function upload() {
      const input = root.querySelector("[data-aw-file]");
      const file = input?.files?.[0];
      if (!file) {
        state.message = "Choose a media file first.";
        render();
        return;
      }
      state.busy = true;
      state.message = `Uploading ${file.name}...`;
      render();
      try {
        const form = new FormData();
        form.append("file", file);
        await requestJson(joinUrl(config.apiBase, "/api/upload"), {
          method: "POST",
          body: form,
        });
        state.message = `Uploaded ${file.name}.`;
        await refresh();
      } catch (error) {
        state.message = error.message || String(error);
      } finally {
        state.busy = false;
        render();
      }
    }

    render();
    refresh();
    return { refresh, upload };
  }

  window.MediaStreamWidget = {
    mount: createWidget,
  };
})();
