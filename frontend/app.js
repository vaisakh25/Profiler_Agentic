// ── State ────────────────────────────────────────────────
let ws = null;
let sessionId = crypto.randomUUID();

const messagesEl = document.getElementById("messages");
const chatInput = document.getElementById("chat-input");
const btnSend = document.getElementById("btn-send");
const statusDot = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");

// Progress bar elements
const progressContainer = document.getElementById("progress-container");
const progressBarFill = document.getElementById("progress-bar-fill");
const progressPercent = document.getElementById("progress-percent");
const progressStage = document.getElementById("progress-stage");
const progressStep = document.getElementById("progress-step");

// Step tracker
const stepTrackerEl = document.getElementById("step-tracker");

// Live stats elements
const liveStatsEl = document.getElementById("live-stats");
const statTablesEl = document.getElementById("stat-tables");
const statRowsEl = document.getElementById("stat-rows");
const statColumnsEl = document.getElementById("stat-columns");
const statFkEl = document.getElementById("stat-fk");
const statElapsedEl = document.getElementById("stat-elapsed");

// Quick actions
const quickActionsEl = document.getElementById("quick-actions");

// Toast container
const toastContainer = document.getElementById("toast-container");

// Drop overlay
const dropOverlay = document.getElementById("drop-overlay");
let fileUploadInput = document.getElementById("file-upload-input");

// Session history
const sessionHistoryList = document.getElementById("session-history-list");

// Mermaid is initialised in loadTheme() at boot

const SUPPORTED_UPLOAD_EXTENSIONS = new Set([
  "csv", "tsv", "dat", "psv",
  "parquet", "pq", "parq",
  "json", "jsonl", "ndjson",
  "xlsx", "xls",
  "gz", "zip",
  "duckdb", "db", "sqlite",
]);

function bindUploadInputListener(inputEl) {
  if (!inputEl || inputEl.dataset.uploadBound === "1") return;
  inputEl.dataset.uploadBound = "1";
  inputEl.addEventListener("change", async (e) => {
    const files = Array.from(e.target.files || []);
    if (files.length > 0) {
      await handleSelectedFiles(files);
    }
    e.target.value = "";
  });
}

function ensureUploadControls() {
  if (!fileUploadInput) {
    const input = document.createElement("input");
    input.id = "file-upload-input";
    input.type = "file";
    input.multiple = true;
    input.accept = ".csv,.tsv,.dat,.psv,.parquet,.pq,.parq,.json,.jsonl,.ndjson,.xlsx,.xls,.gz,.zip,.duckdb,.db,.sqlite";
    input.hidden = true;
    document.body.appendChild(input);
    fileUploadInput = input;
  }
  bindUploadInputListener(fileUploadInput);

  const sidebar = document.getElementById("sidebar");
  if (sidebar && !document.getElementById("btn-upload")) {
    const sidebarBtn = document.createElement("button");
    sidebarBtn.id = "btn-upload";
    sidebarBtn.type = "button";
    sidebarBtn.textContent = "Upload Files";
    sidebarBtn.addEventListener("click", openFilePicker);

    const connectionsBtn = document.getElementById("btn-connections");
    if (connectionsBtn && connectionsBtn.parentElement === sidebar) {
      sidebar.insertBefore(sidebarBtn, connectionsBtn);
    } else {
      sidebar.appendChild(sidebarBtn);
    }
  }

  const chatForm = document.getElementById("chat-form");
  if (chatForm && !document.getElementById("btn-upload-visible")) {
    const inlineBtn = document.createElement("button");
    inlineBtn.id = "btn-upload-visible";
    inlineBtn.type = "button";
    inlineBtn.innerHTML = '<span class="btn-upload-visible-icon">&#128228;</span><span>Upload</span>';
    inlineBtn.addEventListener("click", openFilePicker);

    const sendBtn = document.getElementById("btn-send");
    if (sendBtn && sendBtn.parentElement === chatForm) {
      chatForm.insertBefore(inlineBtn, sendBtn);
    } else {
      chatForm.appendChild(inlineBtn);
    }
  }
}

// Initialise marked with custom renderer for mermaid blocks
const renderer = new marked.Renderer();
const originalCodeRenderer = renderer.code.bind(renderer);

marked.setOptions({
  renderer,
  gfm: true,
  breaks: true,
});

// ── Live stats state ────────────────────────────────────
let liveStats = { tables: 0, rows: 0, columns: 0, fk: 0 };
let elapsedTimerInterval = null;
let turnStartTime = null;

function resetLiveStats() {
  liveStats = { tables: 0, rows: 0, columns: 0, fk: 0 };
  statTablesEl.textContent = "0";
  statRowsEl.textContent = "0";
  statColumnsEl.textContent = "0";
  statFkEl.textContent = "0";
  statElapsedEl.textContent = "0s";
  stopElapsedTimer();
}

function showLiveStats(visible) {
  if (visible) {
    liveStatsEl.classList.add("visible");
  } else {
    liveStatsEl.classList.remove("visible");
  }
}

function updateLiveStat(id, el, newValue) {
  if (el.textContent !== String(newValue)) {
    animateCountUp(el, String(newValue));
    // Flash green on update
    el.classList.add("updated");
    setTimeout(() => el.classList.remove("updated"), 600);
  }
}

function formatNumber(n) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(1) + "K";
  return String(n);
}

function parseLiveStatsFromResult(toolName, summary) {
  if (toolName === "profile_file") {
    // DB files return "N tables profiled (X total rows)" format
    const dbTableMatch = summary.match(/(\d+)\s*tables?\s*profiled/);
    if (dbTableMatch) {
      liveStats.tables += parseInt(dbTableMatch[1], 10);
      const dbRowMatch = summary.match(/([\d,]+)\s*total\s*rows/);
      if (dbRowMatch) liveStats.rows += parseInt(dbRowMatch[1].replace(/,/g, ""), 10);
    } else {
      // Single file: "name: N rows, M columns"
      const rowMatch = summary.match(/([\d,]+)\s*rows/);
      const colMatch = summary.match(/(\d+)\s*columns?/);
      if (rowMatch) liveStats.rows += parseInt(rowMatch[1].replace(/,/g, ""), 10);
      if (colMatch) liveStats.columns += parseInt(colMatch[1], 10);
      liveStats.tables += 1;
    }
  } else if (toolName === "profile_directory") {
    const tableMatch = summary.match(/(\d+)\s*tables?\s*profiled/);
    const rowMatch = summary.match(/([\d,]+)\s*total\s*rows/);
    if (tableMatch) liveStats.tables = parseInt(tableMatch[1], 10);
    if (rowMatch) liveStats.rows = parseInt(rowMatch[1].replace(/,/g, ""), 10);
  } else if (toolName === "detect_relationships") {
    const fkMatch = summary.match(/(\d+)\s*FK\s*candidates/);
    if (fkMatch) liveStats.fk = parseInt(fkMatch[1], 10);
  } else if (toolName === "enrich_relationships") {
    const tableMatch = summary.match(/(\d+)\s*tables/);
    if (tableMatch) liveStats.tables = parseInt(tableMatch[1], 10);
    const relMatch = summary.match(/(\d+)\s*deterministic\s*rels/);
    const derivedMatch = summary.match(/(\d+)\s*cluster-derived\s*rels/);
    if (relMatch) liveStats.fk = parseInt(relMatch[1], 10);
    if (derivedMatch) liveStats.fk += parseInt(derivedMatch[1], 10);
  }

  updateLiveStat("tables", statTablesEl, formatNumber(liveStats.tables));
  updateLiveStat("rows", statRowsEl, formatNumber(liveStats.rows));
  updateLiveStat("columns", statColumnsEl, formatNumber(liveStats.columns));
  updateLiveStat("fk", statFkEl, formatNumber(liveStats.fk));
}

// ── Elapsed timer ───────────────────────────────────────
function startElapsedTimer() {
  turnStartTime = Date.now();
  stopElapsedTimer();
  elapsedTimerInterval = setInterval(() => {
    const elapsed = (Date.now() - turnStartTime) / 1000;
    statElapsedEl.textContent = formatElapsed(elapsed);
  }, 500);
}

function stopElapsedTimer() {
  if (elapsedTimerInterval) {
    clearInterval(elapsedTimerInterval);
    elapsedTimerInterval = null;
  }
}

function formatElapsed(seconds) {
  if (seconds < 60) return Math.floor(seconds) + "s";
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return m + "m " + s + "s";
}

// ── Auto-scroll lock ────────────────────────────────────
let userScrolledUp = false;

messagesEl.addEventListener("scroll", () => {
  const threshold = 60;
  const atBottom =
    messagesEl.scrollHeight - messagesEl.scrollTop - messagesEl.clientHeight < threshold;
  userScrolledUp = !atBottom;
});

// ── Thinking indicator ──────────────────────────────────
let thinkingEl = null;

function showThinking() {
  removeThinking();
  thinkingEl = document.createElement("div");
  thinkingEl.className = "message message-thinking";
  thinkingEl.id = "thinking-indicator";
  thinkingEl.innerHTML =
    '<span class="thinking-dot"></span>' +
    '<span class="thinking-dot"></span>' +
    '<span class="thinking-dot"></span>';
  messagesEl.appendChild(thinkingEl);
  scrollToBottom();
}

function removeThinking() {
  if (thinkingEl) {
    thinkingEl.remove();
    thinkingEl = null;
  }
  const existing = document.getElementById("thinking-indicator");
  if (existing) existing.remove();
}

// ── Quick actions ───────────────────────────────────────
function hideQuickActions() {
  if (quickActionsEl) quickActionsEl.classList.add("hidden");
}

function showQuickActions() {
  if (quickActionsEl) quickActionsEl.classList.remove("hidden");
}

document.querySelectorAll(".quick-action-card").forEach((card) => {
  card.addEventListener("click", () => {
    const message = card.dataset.message;
    if (message && ws && ws.readyState === WebSocket.OPEN) {
      hideQuickActions();
      addUserMessage(message);
      ws.send(JSON.stringify({ type: "message", content: message }));
      chatInput.value = "";
      setInputEnabled(false);
      showProgress(false);
      hideStepTracker();
    }
  });
});

const quickUploadAction = document.getElementById("quick-action-upload");
if (quickUploadAction) {
  quickUploadAction.addEventListener("click", () => {
    openFilePicker();
  });
}

// ── Toast notifications ─────────────────────────────────
function showToast(message, type = "info", duration = 4000) {
  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;

  const icons = { success: "&#10003;", info: "&#9432;", warning: "&#9888;" };
  toast.innerHTML =
    `<span class="toast-icon">${icons[type] || icons.info}</span>` +
    `<span>${escapeHtml(message)}</span>`;

  toastContainer.appendChild(toast);

  setTimeout(() => {
    toast.classList.add("removing");
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

// ── Drag-and-drop file upload ───────────────────────────
let dragCounter = 0;

document.addEventListener("dragenter", (e) => {
  e.preventDefault();
  dragCounter++;
  if (dragCounter === 1) dropOverlay.classList.add("visible");
});

document.addEventListener("dragleave", (e) => {
  e.preventDefault();
  dragCounter--;
  if (dragCounter <= 0) {
    dragCounter = 0;
    dropOverlay.classList.remove("visible");
  }
});

document.addEventListener("dragover", (e) => {
  e.preventDefault();
});

document.addEventListener("drop", async (e) => {
  e.preventDefault();
  dragCounter = 0;
  dropOverlay.classList.remove("visible");

  const files = e.dataTransfer.files;
  if (!files || files.length === 0) return;

  await handleSelectedFiles(Array.from(files));
});

function openFilePicker() {
  if (!fileUploadInput) return;
  fileUploadInput.click();
}

bindUploadInputListener(fileUploadInput);

function getFileExtension(fileName) {
  const parts = fileName.toLowerCase().split(".");
  if (parts.length < 2) return "";
  return parts[parts.length - 1];
}

function isSupportedUploadFile(file) {
  return SUPPORTED_UPLOAD_EXTENSIONS.has(getFileExtension(file.name));
}

async function handleSelectedFiles(files) {
  const supportedFiles = files.filter(isSupportedUploadFile);
  const unsupportedFiles = files.filter((f) => !isSupportedUploadFile(f));

  if (unsupportedFiles.length > 0) {
    const previewNames = unsupportedFiles.slice(0, 3).map((f) => f.name).join(", ");
    const moreSuffix = unsupportedFiles.length > 3 ? ` (+${unsupportedFiles.length - 3} more)` : "";
    showToast(`Skipped unsupported files: ${previewNames}${moreSuffix}`, "warning", 5000);
  }

  if (supportedFiles.length === 0) {
    return;
  }

  const batchId = (crypto.randomUUID ? crypto.randomUUID() : String(Date.now()))
    .replace(/-/g, "")
    .slice(0, 12);

  const uploaded = [];
  for (const file of supportedFiles) {
    const result = await uploadFile(file, { autoProfile: false, batchId });
    if (result && result.server_path) {
      uploaded.push(result);
    }
  }

  if (uploaded.length === 0) {
    showToast("No files were uploaded successfully", "warning", 5000);
    return;
  }

  showToast(`Uploaded ${uploaded.length}/${supportedFiles.length} files`, "success", 4000);

  if (ws && ws.readyState === WebSocket.OPEN) {
    hideQuickActions();
    let msg = [
      `Run list_supported_files on ${uploaded[0].server_path}.`,
      `Then run profile_file on ${uploaded[0].server_path}.`,
      "Skip enrichment unless I explicitly ask for it.",
      "Return concise output only: table name, rows, columns, top 3 quality issues, next action.",
      "Maximum 6 bullet points.",
    ].join(" ");

    if (uploaded.length > 1) {
      const uploadDir = uploaded[0].upload_dir || (() => {
        const pathParts = uploaded[0].server_path.split("/");
        return pathParts.slice(0, -1).join("/") || uploaded[0].server_path;
      })();
      msg = [
        `Run list_supported_files on ${uploadDir} first and include all supported files found.`,
        `Then run profile_directory on ${uploadDir}.`,
        "Skip enrichment unless I explicitly ask for it.",
        "Return a compact summary only: total tables, total rows, key quality issues, and recommended next step.",
        "Maximum 8 bullet points.",
      ].join(" ");
    }

    addUserMessage(msg);
    ws.send(JSON.stringify({ type: "message", content: msg }));
    setInputEnabled(false);
  }
}

async function uploadFile(file, options = {}) {
  const autoProfile = options.autoProfile ?? true;
  const batchId = options.batchId || "";
  showToast(`Uploading ${file.name}...`, "info", 3000);

  const formData = new FormData();
  formData.append("file", file);

  try {
    const url = batchId
      ? `/api/upload?batch_id=${encodeURIComponent(batchId)}`
      : "/api/upload";
    const resp = await fetch(url, { method: "POST", body: formData });
    const result = await resp.json();

    if (!resp.ok) {
      showToast(result.error || "Upload failed", "warning", 5000);
      return null;
    }

    showToast(`${result.file_name} uploaded (${formatBytes(result.size_bytes)})`, "success");

    // Auto-send a profile message via chat
    if (autoProfile && ws && ws.readyState === WebSocket.OPEN) {
      hideQuickActions();
      const msg = [
        `Run list_supported_files on ${result.server_path}.`,
        `Then run profile_file on ${result.server_path}.`,
        "Skip enrichment unless I explicitly ask for it.",
        "Return concise output only: table name, rows, columns, top 3 quality issues, next action.",
        "Maximum 6 bullet points.",
      ].join(" ");
      addUserMessage(msg);
      ws.send(JSON.stringify({ type: "message", content: msg }));
      setInputEnabled(false);
    }
    return result;
  } catch (err) {
    showToast("Upload failed: " + err.message, "warning", 5000);
    return null;
  }
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

function formatTokenUsage(usage) {
  if (!usage || typeof usage !== "object") return "";
  const input = Number(usage.input_tokens || 0);
  const output = Number(usage.output_tokens || 0);
  const total = Number(usage.total_tokens || (input + output));
  if (!Number.isFinite(total) || total <= 0) return "";
  return `LLM tokens: ${total.toLocaleString()} (in ${input.toLocaleString()}, out ${output.toLocaleString()})`;
}

// ── Table preview cards ─────────────────────────────────
function addPreviewCard(preview) {
  if (!preview) return;

  if (preview.kind === "table") {
    addTablePreviewCard(preview);
  } else if (preview.kind === "directory") {
    addDirectoryPreviewCard(preview);
  } else if (preview.kind === "relationships") {
    addRelationshipPreviewCard(preview);
  } else if (preview.kind === "charts") {
    addChartPreviewCard(preview);
  }
}

function addTablePreviewCard(p) {
  const el = document.createElement("div");
  el.className = "preview-card";

  const issuesBadge = p.quality && p.quality.issues > 0
    ? `<span class="preview-badge preview-badge-issues">${p.quality.issues} issues</span>`
    : "";

  el.innerHTML =
    `<div class="preview-card-header" onclick="this.classList.toggle('expanded');this.nextElementSibling.classList.toggle('expanded')">` +
      `<span class="preview-card-title">${escapeHtml(p.table_name)}</span>` +
      `<div class="preview-card-badges">` +
        `<span class="preview-badge preview-badge-rows">${formatNumber(p.row_count)} rows</span>` +
        `<span class="preview-badge preview-badge-cols">${p.col_count} cols</span>` +
        `<span class="preview-badge preview-badge-format">${escapeHtml(p.format)}</span>` +
        issuesBadge +
        `<span class="preview-card-toggle">&#9660;</span>` +
      `</div>` +
    `</div>` +
    `<div class="preview-card-body">` +
      buildColumnTable(p.columns || []) +
    `</div>`;

  messagesEl.appendChild(el);
  scrollToBottom();
}

function buildColumnTable(columns) {
  if (columns.length === 0) return "";

  let html = '<table class="preview-columns-table">';
  html += "<thead><tr><th>Column</th><th>Type</th><th>Nulls</th><th>Distinct</th><th>Flags</th></tr></thead>";
  html += "<tbody>";
  for (const c of columns) {
    const nullPct = c.null_pct || 0;
    const barClass = nullPct > 30 ? "high" : nullPct > 10 ? "medium" : "low";
    const flagHtml = (c.flags || [])
      .filter((f) => f)
      .map((f) => `<span class="preview-flag">${escapeHtml(f)}</span>`)
      .join("");
    html +=
      `<tr>` +
        `<td><strong>${escapeHtml(c.name)}</strong></td>` +
        `<td>${escapeHtml(c.type)}</td>` +
        `<td><span class="null-bar"><span class="null-bar-fill ${barClass}" style="width:${nullPct}%"></span></span>${nullPct}%</td>` +
        `<td>${formatNumber(c.distinct)}</td>` +
        `<td>${flagHtml || "—"}</td>` +
      `</tr>`;
  }
  html += "</tbody></table>";
  return html;
}

function addDirectoryPreviewCard(p) {
  // Render each table as its own expandable preview card
  for (const t of p.tables) {
    addTablePreviewCard({
      table_name: t.table_name,
      row_count: t.row_count,
      col_count: t.col_count,
      format: t.format,
      columns: t.columns || [],
      quality: t.quality || null,
    });
  }
}

function addRelationshipPreviewCard(p) {
  const el = document.createElement("div");
  el.className = "preview-card";

  let relRows = "";
  for (const c of p.candidates) {
    const pct = Math.round(c.confidence * 100);
    relRows +=
      `<tr>` +
        `<td>${escapeHtml(c.fk)}</td>` +
        `<td>&#10132;</td>` +
        `<td>${escapeHtml(c.pk)}</td>` +
        `<td><span class="confidence-bar"><span class="confidence-bar-fill" style="width:${pct}%"></span></span>${pct}%</td>` +
      `</tr>`;
  }

  el.innerHTML =
    `<div class="preview-card-header" onclick="this.classList.toggle('expanded');this.nextElementSibling.classList.toggle('expanded')">` +
      `<span class="preview-card-title">${p.candidates.length} FK Candidates</span>` +
      `<div class="preview-card-badges">` +
        `<span class="preview-card-toggle">&#9660;</span>` +
      `</div>` +
    `</div>` +
    `<div class="preview-card-body">` +
      `<table class="preview-rel-table">` +
        `<thead><tr><th>FK Column</th><th></th><th>PK Column</th><th>Confidence</th></tr></thead>` +
        `<tbody>${relRows}</tbody>` +
      `</table>` +
    `</div>`;

  messagesEl.appendChild(el);
  scrollToBottom();
}

// ── Chart preview cards ──────────────────────────────────
function addChartPreviewCard(p) {
  const el = document.createElement("div");
  el.className = "preview-card preview-card-charts";

  let chartsHtml = "";
  for (const chart of p.charts) {
    chartsHtml +=
      `<div class="chart-card">` +
        `<div class="chart-card-title">${escapeHtml(chart.title)}</div>` +
        `<img class="chart-card-img" src="${chart.url}" alt="${escapeHtml(chart.title)}" ` +
             `loading="lazy" onclick="openChartFullscreen(this)" />` +
      `</div>`;
  }

  const tableName = escapeHtml(p.table_name || "Charts");

  el.innerHTML =
    `<div class="preview-card-header charts-header" onclick="this.classList.toggle('expanded');this.nextElementSibling.classList.toggle('expanded')">` +
      `<span class="preview-card-title">${p.charts.length} Chart${p.charts.length !== 1 ? "s" : ""} — ${tableName}</span>` +
      `<div class="preview-card-badges">` +
        `<span class="preview-badge preview-badge-format">PNG</span>` +
        `<span class="preview-card-toggle">&#9660;</span>` +
      `</div>` +
    `</div>` +
    `<div class="preview-card-body expanded">` +
      `<div class="charts-grid">${chartsHtml}</div>` +
      `<button class="artifact-trigger" onclick="openArtifact('${tableName}', this.closest('.preview-card-body').querySelector('.charts-grid').innerHTML)">` +
        `<span class="artifact-trigger-icon">&#9670;</span> Open in panel</button>` +
    `</div>`;

  messagesEl.appendChild(el);
  scrollToBottom();
}

function openChartFullscreen(img) {
  const overlay = document.createElement("div");
  overlay.className = "chart-fullscreen-overlay";
  overlay.onclick = () => overlay.remove();
  overlay.innerHTML =
    `<img src="${img.src}" alt="${img.alt}" />` +
    `<div class="chart-fullscreen-hint">Click anywhere to close</div>`;
  document.body.appendChild(overlay);
}

// ── Session history (localStorage + server sync) ────────
const SESSION_HISTORY_KEY = "profiler_sessions";
const MAX_SESSIONS = 20;

function loadSessionHistory() {
  try {
    return JSON.parse(localStorage.getItem(SESSION_HISTORY_KEY) || "[]");
  } catch {
    return [];
  }
}

function saveSessionToHistory(label) {
  // Save to localStorage (instant)
  const sessions = loadSessionHistory();
  const existing = sessions.findIndex((s) => s.id === sessionId);
  if (existing >= 0) {
    sessions[existing].label = label;
    sessions[existing].time = Date.now();
  } else {
    sessions.unshift({
      id: sessionId,
      label: label,
      time: Date.now(),
    });
  }
  while (sessions.length > MAX_SESSIONS) sessions.pop();
  localStorage.setItem(SESSION_HISTORY_KEY, JSON.stringify(sessions));
  renderSessionHistory();

  // Sync to server (fire-and-forget)
  fetch("/api/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId, label }),
  }).catch(() => {});
}

async function syncSessionsFromServer() {
  try {
    const resp = await fetch("/api/sessions");
    if (!resp.ok) return;
    const serverSessions = await resp.json();
    if (!Array.isArray(serverSessions) || serverSessions.length === 0) return;

    // Merge server sessions into localStorage
    const local = loadSessionHistory();
    const localMap = new Map(local.map((s) => [s.id, s]));

    for (const ss of serverSessions) {
      const id = ss.session_id;
      const serverTime = new Date(ss.updated_at).getTime();
      if (!localMap.has(id)) {
        localMap.set(id, {
          id,
          label: ss.label || "Session",
          time: serverTime,
          message_count: ss.message_count || 0,
        });
      } else {
        const ls = localMap.get(id);
        // Server may have a better label or newer timestamp
        if (ss.label && !ls.label) ls.label = ss.label;
        if (serverTime > (ls.time || 0)) ls.time = serverTime;
        if (ss.message_count) ls.message_count = ss.message_count;
      }
    }

    // Sort by time descending, cap at MAX_SESSIONS
    const merged = Array.from(localMap.values())
      .sort((a, b) => (b.time || 0) - (a.time || 0))
      .slice(0, MAX_SESSIONS);

    localStorage.setItem(SESSION_HISTORY_KEY, JSON.stringify(merged));
    renderSessionHistory();
  } catch {
    // Server not reachable — use localStorage only
  }
}

function renderSessionHistory() {
  const sessions = loadSessionHistory();
  sessionHistoryList.innerHTML = "";

  if (sessions.length === 0) {
    sessionHistoryList.innerHTML =
      '<div style="font-size:11px;color:var(--text-muted);padding:4px 0;">No sessions yet</div>';
    return;
  }

  for (const s of sessions) {
    const btn = document.createElement("button");
    btn.className = "session-history-item" + (s.id === sessionId ? " active" : "");
    const timeStr = formatSessionTime(s.time);
    const msgCount = s.message_count ? ` (${s.message_count} msgs)` : "";
    btn.innerHTML =
      `<span class="session-history-time">${timeStr}</span>` +
      `<span class="session-history-label">${escapeHtml(s.label || "Session")}${msgCount}</span>`;
    btn.addEventListener("click", () => {
      // Switch to this session — server will load checkpoint automatically
      sessionId = s.id;
      messagesEl.innerHTML = "";
      showQuickActions();
      messagesEl.appendChild(quickActionsEl);
      quickActionsEl.classList.remove("hidden");
      renderSessionHistory();
      if (ws) ws.close();
      setTimeout(connect, 100);
    });
    sessionHistoryList.appendChild(btn);
  }
}

function formatSessionTime(ts) {
  const d = new Date(ts);
  const now = new Date();
  const diffMs = now - d;
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return "just now";
  if (diffMin < 60) return diffMin + "m ago";
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return diffHr + "h ago";
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

// ── ER diagram interactivity (zoom/pan) ─────────────────
let erZoomLevel = 1;

function addERDiagramMessage(content) {
  const el = document.createElement("div");
  el.className = "message message-er-diagram";

  const header = document.createElement("div");
  header.className = "er-diagram-header";
  header.innerHTML = '<span class="er-icon">&#9670;</span> ER Diagram';

  const body = document.createElement("div");
  body.className = "er-diagram-body";

  // Wrap mermaid in a viewport div for zoom/pan
  const viewport = document.createElement("div");
  viewport.className = "er-diagram-viewport";
  viewport.innerHTML = renderMarkdown(content);
  body.appendChild(viewport);

  // Zoom/pan controls
  const controls = document.createElement("div");
  controls.className = "er-diagram-controls";

  const btnZoomIn = document.createElement("button");
  btnZoomIn.textContent = "+";
  btnZoomIn.title = "Zoom in";
  btnZoomIn.addEventListener("click", () => {
    erZoomLevel = Math.min(erZoomLevel + 0.2, 3);
    viewport.style.transform = `scale(${erZoomLevel})`;
  });

  const btnZoomOut = document.createElement("button");
  btnZoomOut.textContent = "\u2212";
  btnZoomOut.title = "Zoom out";
  btnZoomOut.addEventListener("click", () => {
    erZoomLevel = Math.max(erZoomLevel - 0.2, 0.3);
    viewport.style.transform = `scale(${erZoomLevel})`;
  });

  const btnReset = document.createElement("button");
  btnReset.textContent = "Reset";
  btnReset.title = "Reset zoom";
  btnReset.addEventListener("click", () => {
    erZoomLevel = 1;
    viewport.style.transform = "scale(1)";
  });

  const btnFit = document.createElement("button");
  btnFit.textContent = "Fit";
  btnFit.title = "Fit to width";
  btnFit.addEventListener("click", () => {
    // Auto-fit: measure rendered SVG width vs container
    const svg = viewport.querySelector("svg");
    if (svg) {
      const svgW = svg.getBoundingClientRect().width / erZoomLevel;
      const containerW = body.clientWidth - 32;
      erZoomLevel = Math.min(containerW / svgW, 2);
      viewport.style.transform = `scale(${erZoomLevel})`;
    }
  });

  controls.appendChild(btnZoomIn);
  controls.appendChild(btnZoomOut);
  controls.appendChild(btnReset);
  controls.appendChild(btnFit);

  el.appendChild(header);
  el.appendChild(body);
  el.appendChild(controls);
  messagesEl.appendChild(el);

  renderMermaidBlocks(viewport);
  erZoomLevel = 1;
  scrollToBottom();
}

// ── WebSocket ────────────────────────────────────────────
function connect() {
  const host = window.location.host;
  const wsScheme = window.location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${wsScheme}://${host}/ws/chat`);

  ws.onopen = () => {
    setStatus("connected", "Connected");
    ws.send(
      JSON.stringify({
        type: "config",
        session_id: sessionId,
        mcp_url: document.getElementById("mcp-url").value,
        provider: document.getElementById("provider").value || null,
      })
    );
  };

  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    handleServerMessage(data);
  };

  ws.onclose = () => {
    setStatus("disconnected", "Disconnected");
    setTimeout(connect, 3000);
  };

  ws.onerror = () => {
    setStatus("disconnected", "Error");
  };
}

// ── Handle incoming messages ─────────────────────────────
function handleServerMessage(data) {
  switch (data.type) {
    case "connected":
      setStatus("connected", `Connected (${data.tools} tools)`);
      if (data.has_history) {
        showToast("Loading conversation history...", "info", 2000);
      }
      break;

    case "history":
      // Render restored conversation messages
      if (data.messages && data.messages.length > 0) {
        hideQuickActions();
        for (const m of data.messages) {
          if (m.role === "user") {
            addUserMessage(m.content);
          } else if (m.role === "assistant") {
            addAssistantMessage(m.content);
          } else if (m.role === "tool") {
            const label = m.tool ? `${m.tool} — ${m.content}` : m.content;
            addHistoryToolMessage(label);
          }
        }
        showToast(`Restored ${data.messages.length} messages`, "success", 3000);
      }
      break;

    case "tool_start":
      hideQuickActions();
      removeThinking();
      setStatus("working", `Running ${data.tool}...`);
      {
        const usageText = formatTokenUsage(data.llm_usage);
        const label = usageText ? `Calling ${data.tool}... ${usageText}` : `Calling ${data.tool}...`;
        addToolMessage(label);
      }
      updateProgress(data.percent, data.stage, `Step ${data.tool_index}`, false);
      showProgress(true);
      if (!liveStatsEl.classList.contains("visible")) {
        resetLiveStats();
        startElapsedTimer();
      }
      showLiveStats(true);
      break;

    case "pipeline_steps":
      showStepTracker(data.tool, data.steps);
      break;

    case "step_update":
      updateStepTracker(data.active_step);
      break;

    case "step_complete":
      completeStepTracker(data.success);
      break;

    case "tool_result": {
      removeThinking();
      setStatus("working", "Thinking...");
      showThinking();
      const icon = data.success ? "done" : "failed";
      {
        const usageText = formatTokenUsage(data.llm_usage);
        const label = usageText
          ? `${data.tool} — ${icon} — ${data.summary} — ${usageText}`
          : `${data.tool} — ${icon} — ${data.summary}`;
        updateLastToolMessage(label);
      }
      updateProgress(data.percent, data.summary, `Step ${data.tool_index}`, false);
      parseLiveStatsFromResult(data.tool, data.summary);

      // Render preview card if available
      if (data.preview) {
        addPreviewCard(data.preview);
        // Update column count from preview data (more accurate)
        if (data.preview.kind === "directory" && data.preview.tables) {
          let totalCols = 0;
          for (const t of data.preview.tables) totalCols += (t.col_count || 0);
          liveStats.columns = totalCols;
          updateLiveStat("columns", statColumnsEl, formatNumber(liveStats.columns));
        }
      }

      // Toast for key results
      if (data.success) {
        if (data.tool === "profile_directory") {
          showToast(data.summary, "success");
        } else if (data.tool === "detect_relationships") {
          showToast(data.summary, "success");
        } else if (data.tool === "enrich_relationships") {
          showToast("Enrichment complete", "success");
        }
      }
      break;
    }

    case "progress":
      updateProgress(data.percent, data.stage, `Step ${data.tool_index}`, data.percent >= 100);
      // Update live stats from structured stats in progress events
      if (data.stats) {
        const s = data.stats;
        if (s.tables_done !== undefined) {
          liveStats.tables = s.tables_done;
          updateLiveStat("tables", statTablesEl, formatNumber(liveStats.tables));
        }
        if (s.rows !== undefined) {
          liveStats.rows = s.rows;
          updateLiveStat("rows", statRowsEl, formatNumber(liveStats.rows));
        }
        if (s.columns !== undefined) {
          liveStats.columns = s.columns;
          updateLiveStat("columns", statColumnsEl, formatNumber(liveStats.columns));
        }
        if (s.fk !== undefined) {
          liveStats.fk = s.fk;
          updateLiveStat("fk", statFkEl, formatNumber(liveStats.fk));
        }
      }
      break;

    case "assistant":
      removeThinking();
      setStatus("connected", "Connected");
      addAssistantMessage(data.content);
      setInputEnabled(true);
      stopElapsedTimer();
      setTimeout(() => {
        showProgress(false);
        hideStepTracker();
      }, 2000);
      setTimeout(() => {
        showLiveStats(false);
      }, 5000);
      break;

    case "error":
      removeThinking();
      setStatus("connected", "Connected");
      addErrorMessage(data.content);
      setInputEnabled(true);
      stopElapsedTimer();
      setTimeout(() => {
        showProgress(false);
        hideStepTracker();
        showLiveStats(false);
      }, 2000);
      break;

    case "er_diagram":
      addERDiagramMessage(data.content);
      // Also open in artifact panel for better viewing
      openArtifact("ER Diagram", renderMarkdown(data.content), data.content);
      break;

    case "thinking":
      setStatus("working", "Thinking...");
      showThinking();
      if (data.percent !== undefined) {
        updateProgress(data.percent, "LLM is thinking...", "", false);
      }
      break;
  }
}

// ── Send message ─────────────────────────────────────────
function sendMessage(e) {
  e.preventDefault();
  const text = chatInput.value.trim();
  if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;

  hideQuickActions();
  addUserMessage(text);
  // Save first user message as session label
  saveSessionToHistory(text.slice(0, 60));

  ws.send(JSON.stringify({ type: "message", content: text }));
  chatInput.value = "";
  chatInput.style.height = "auto";
  setInputEnabled(false);
  showProgress(false);
  hideStepTracker();
  showLiveStats(false);
  resetLiveStats();
}

// ── DOM helpers ──────────────────────────────────────────
function addUserMessage(text) {
  const el = document.createElement("div");
  el.className = "message message-user";
  el.textContent = text;
  messagesEl.appendChild(el);
  scrollToBottom();
}

function addAssistantMessage(text) {
  const el = document.createElement("div");
  el.className = "message message-assistant";

  // Main content
  let content = renderMarkdown(text);

  // Response action bar (copy)
  content += buildResponseActionBar();

  // Follow-up action chips
  const chips = getFollowUpChips(text);
  content += renderActionChips(chips);

  el.innerHTML = content;
  messagesEl.appendChild(el);
  renderMermaidBlocks(el);
  attachChipListeners(el);
  scrollToBottom();
}

function addToolMessage(text) {
  const el = document.createElement("div");
  el.className = "message message-tool";
  el.innerHTML = `<div class="tool-label">${escapeHtml(text)}</div>`;
  el.id = "tool-msg-latest";
  messagesEl.appendChild(el);
  scrollToBottom();
}

function addHistoryToolMessage(text) {
  const el = document.createElement("div");
  el.className = "message message-tool history-tool";
  el.innerHTML = `<div class="tool-label">${escapeHtml(text)}</div>`;
  messagesEl.appendChild(el);
}

function updateLastToolMessage(text) {
  const el = document.getElementById("tool-msg-latest");
  if (el) {
    el.querySelector(".tool-label").textContent = text;
    el.removeAttribute("id");
  }
}

function addErrorMessage(text) {
  const el = document.createElement("div");
  el.className = "message message-error";
  el.textContent = text;
  messagesEl.appendChild(el);
  scrollToBottom();
}

function scrollToBottom() {
  if (!userScrolledUp) {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }
}

function setInputEnabled(enabled) {
  chatInput.disabled = !enabled;
  btnSend.disabled = !enabled;
  if (enabled) chatInput.focus();
}

function setStatus(state, text) {
  statusDot.className = `dot dot-${state}`;
  statusText.textContent = text;
}

// ── Markdown rendering ───────────────────────────────────
function renderMarkdown(text) {
  return marked.parse(text);
}

function renderMermaidBlocks(container) {
  const codeBlocks = container.querySelectorAll("code.language-mermaid");
  codeBlocks.forEach((block) => {
    const pre = block.parentElement;
    const mermaidDiv = document.createElement("div");
    mermaidDiv.className = "mermaid";
    mermaidDiv.textContent = block.textContent;
    pre.replaceWith(mermaidDiv);
  });

  mermaid.run({ nodes: container.querySelectorAll(".mermaid") });
}

function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// ── Step tracker ─────────────────────────────────────────
let currentStepIndex = -1;

function showStepTracker(toolName, steps) {
  currentStepIndex = 0;

  const header = document.createElement("div");
  header.className = "step-tracker-header";
  header.innerHTML =
    'Pipeline Steps &mdash; <span class="step-tool-name">' +
    escapeHtml(toolName) +
    "</span>";

  const list = document.createElement("div");
  list.className = "step-list";

  steps.forEach((step, i) => {
    const item = document.createElement("div");
    item.className = "step-item" + (i === 0 ? " active" : "");
    item.dataset.index = i;

    const indicator = document.createElement("div");
    indicator.className = "step-indicator";
    const icon = document.createElement("span");
    icon.className = "step-indicator-icon";
    icon.textContent = i === 0 ? "" : "";
    indicator.appendChild(icon);

    const content = document.createElement("div");
    content.className = "step-content";
    const name = document.createElement("div");
    name.className = "step-name";
    name.textContent = step.name;
    content.appendChild(name);

    const miniBar = document.createElement("div");
    miniBar.className = "step-mini-bar";
    const miniBarFill = document.createElement("div");
    miniBarFill.className = "step-mini-bar-fill";
    miniBar.appendChild(miniBarFill);
    content.appendChild(miniBar);

    item.appendChild(indicator);
    item.appendChild(content);
    list.appendChild(item);
  });

  stepTrackerEl.innerHTML = "";
  stepTrackerEl.appendChild(header);
  stepTrackerEl.appendChild(list);
  stepTrackerEl.classList.add("visible");
}

function updateStepTracker(activeStep) {
  if (!stepTrackerEl.classList.contains("visible")) return;

  const items = stepTrackerEl.querySelectorAll(".step-item");
  items.forEach((item, i) => {
    const icon = item.querySelector(".step-indicator-icon");
    if (i < activeStep) {
      item.className = "step-item done";
      icon.innerHTML = "&#10003;";
    } else if (i === activeStep) {
      item.className = "step-item active";
      icon.textContent = "";
    } else {
      item.className = "step-item";
      icon.textContent = "";
    }
  });
  currentStepIndex = activeStep;
}

function completeStepTracker(success) {
  if (!stepTrackerEl.classList.contains("visible")) return;

  const items = stepTrackerEl.querySelectorAll(".step-item");
  items.forEach((item) => {
    const icon = item.querySelector(".step-indicator-icon");
    if (success) {
      item.className = "step-item done";
      icon.innerHTML = "&#10003;";
    } else {
      item.className = "step-item error";
      icon.innerHTML = "&#10007;";
    }
  });
}

function hideStepTracker() {
  stepTrackerEl.classList.remove("visible");
  setTimeout(() => {
    stepTrackerEl.innerHTML = "";
    currentStepIndex = -1;
  }, 400);
}

// ── Progress bar ─────────────────────────────────────────
function showProgress(visible) {
  if (visible) {
    progressContainer.classList.add("visible");
  } else {
    progressContainer.classList.remove("visible");
    setTimeout(() => {
      progressBarFill.style.width = "0%";
      progressBarFill.classList.remove("complete");
      progressPercent.textContent = "0%";
      progressStage.textContent = "";
      progressStep.textContent = "";
    }, 300);
  }
}

function updateProgress(percent, stage, step, complete) {
  progressBarFill.style.width = `${percent}%`;
  progressPercent.textContent = `${Math.round(percent)}%`;
  progressStage.textContent = stage || "";
  progressStep.textContent = step || "";

  if (complete) {
    progressBarFill.classList.add("complete");
  } else {
    progressBarFill.classList.remove("complete");
  }
}

// ── Clear chat ───────────────────────────────────────────
function clearChat() {
  messagesEl.innerHTML = "";
  sessionId = crypto.randomUUID();
  resetLiveStats();
  showLiveStats(false);
  showQuickActions();
  messagesEl.appendChild(quickActionsEl);
  quickActionsEl.classList.remove("hidden");
  renderSessionHistory();
  if (ws) ws.close();
  setTimeout(connect, 100);
}

// ── Theme ────────────────────────────────────────────────
function toggleTheme() {
  const isDark = document.getElementById("theme-switch").checked;
  const theme = isDark ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem("theme", theme);
  mermaid.initialize({
    startOnLoad: false,
    theme: theme === "dark" ? "default" : "neutral",
  });
}

function loadTheme() {
  const saved = localStorage.getItem("theme") || "light";
  document.documentElement.setAttribute("data-theme", saved);
  document.getElementById("theme-switch").checked = saved === "dark";
  mermaid.initialize({
    startOnLoad: false,
    theme: saved === "dark" ? "default" : "neutral",
  });
}

// ── Connection modal ─────────────────────────────────────

const CREDENTIAL_FIELDS = {
  s3: [
    { key: "aws_access_key_id", label: "Access Key ID", type: "text" },
    { key: "aws_secret_access_key", label: "Secret Access Key", type: "password" },
    { key: "region", label: "Region", type: "text", placeholder: "us-east-1" },
    { key: "profile_name", label: "AWS Profile (optional)", type: "text" },
  ],
  minio: [
    { key: "endpoint_url", label: "Endpoint URL", type: "text", placeholder: "http://localhost:9000" },
    { key: "access_key", label: "Access Key", type: "text" },
    { key: "secret_key", label: "Secret Key", type: "password" },
    { key: "region", label: "Region (optional)", type: "text", placeholder: "us-east-1" },
    { key: "test_bucket", label: "Test Bucket", type: "text", placeholder: "my-bucket or my-bucket/prefix" },
  ],
  abfss: [
    { key: "account_name", label: "Storage Account", type: "text" },
    { key: "connection_string", label: "Connection String", type: "password" },
    { key: "tenant_id", label: "Tenant ID", type: "text" },
    { key: "client_id", label: "Client ID", type: "text" },
    { key: "client_secret", label: "Client Secret", type: "password" },
  ],
  gs: [
    { key: "service_account_json", label: "Service Account JSON Path", type: "text" },
  ],
  snowflake: [
    { key: "account", label: "Account", type: "text" },
    { key: "user", label: "User", type: "text" },
    { key: "password", label: "Password", type: "password" },
    { key: "warehouse", label: "Warehouse", type: "text" },
    { key: "role", label: "Role (optional)", type: "text" },
  ],
  postgresql: [
    { key: "connection_string", label: "Connection String (or use fields below)", type: "text" },
    { key: "host", label: "Host", type: "text", placeholder: "localhost" },
    { key: "port", label: "Port", type: "text", placeholder: "5432" },
    { key: "user", label: "User", type: "text" },
    { key: "password", label: "Password", type: "password" },
    { key: "dbname", label: "Database", type: "text" },
  ],
};

function openConnectionModal() {
  document.getElementById("connection-modal").classList.add("visible");
  loadConnectionList();
}

function closeConnectionModal() {
  document.getElementById("connection-modal").classList.remove("visible");
}

function updateCredentialFields() {
  const scheme = document.getElementById("conn-scheme").value;
  const container = document.getElementById("conn-cred-fields");
  container.innerHTML = "";

  const fields = CREDENTIAL_FIELDS[scheme] || [];
  for (const f of fields) {
    const row = document.createElement("div");
    row.className = "conn-form-row";
    row.innerHTML =
      `<label>${escapeHtml(f.label)}</label>` +
      `<input id="cred-${f.key}" type="${f.type}" placeholder="${f.placeholder || ""}" />`;
    container.appendChild(row);
  }
}

function _gatherCredentials() {
  const scheme = document.getElementById("conn-scheme").value;
  const fields = CREDENTIAL_FIELDS[scheme] || [];
  const creds = {};
  for (const f of fields) {
    const val = document.getElementById(`cred-${f.key}`)?.value?.trim();
    if (val) creds[f.key] = val;
  }
  return creds;
}

async function saveConnection() {
  const connectionId = document.getElementById("conn-id").value.trim();
  const scheme = document.getElementById("conn-scheme").value;
  const displayName = document.getElementById("conn-display-name").value.trim();
  const credentials = _gatherCredentials();

  if (!connectionId || !scheme) {
    showToast("Connection ID and Source Type are required", "warning");
    return;
  }

  try {
    const resp = await fetch("/api/connections", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        connection_id: connectionId,
        scheme,
        display_name: displayName,
        credentials,
      }),
    });
    const result = await resp.json();
    if (!resp.ok) {
      showToast(result.error || "Failed to save connection", "warning");
      return;
    }
    showToast(`Connection "${connectionId}" saved`, "success");
    _clearConnectionForm();
    loadConnectionList();
  } catch (err) {
    showToast("Error saving connection: " + err.message, "warning");
  }
}

async function testNewConnection() {
  const connectionId = document.getElementById("conn-id").value.trim();
  if (!connectionId) {
    showToast("Connection ID is required", "warning");
    return;
  }
  await saveConnection();
  await testConnection(connectionId);
}

async function testConnection(connectionId) {
  showToast(`Testing ${connectionId}...`, "info", 3000);
  try {
    const resp = await fetch(`/api/connections/${encodeURIComponent(connectionId)}/test`, {
      method: "POST",
    });
    const result = await resp.json();
    if (result.success) {
      showToast(`${connectionId}: OK (${result.latency_ms}ms)`, "success");
    } else {
      showToast(`${connectionId}: ${result.message}`, "warning", 6000);
    }
    loadConnectionList();
  } catch (err) {
    showToast("Test failed: " + err.message, "warning");
  }
}

async function deleteConnection(connectionId) {
  try {
    await fetch(`/api/connections/${encodeURIComponent(connectionId)}`, {
      method: "DELETE",
    });
    showToast(`Connection "${connectionId}" removed`, "info");
    loadConnectionList();
  } catch (err) {
    showToast("Delete failed: " + err.message, "warning");
  }
}

async function loadConnectionList() {
  const container = document.getElementById("conn-list");
  try {
    const resp = await fetch("/api/connections");
    const connections = await resp.json();

    if (!connections || connections.length === 0) {
      container.innerHTML =
        '<div class="conn-empty">No connections configured</div>';
      return;
    }

    let html = "";
    for (const c of connections) {
      const healthDot = c.is_healthy === true
        ? '<span class="conn-health conn-healthy"></span>'
        : c.is_healthy === false
          ? '<span class="conn-health conn-unhealthy"></span>'
          : '<span class="conn-health conn-unknown"></span>';

      const tested = c.last_tested
        ? new Date(c.last_tested * 1000).toLocaleString()
        : "never";

      html +=
        `<div class="conn-item">` +
          `<div class="conn-item-info">` +
            `${healthDot}` +
            `<span class="conn-item-id">${escapeHtml(c.connection_id)}</span>` +
            `<span class="conn-item-scheme">${escapeHtml(c.scheme)}</span>` +
            `<span class="conn-item-tested">Tested: ${escapeHtml(tested)}</span>` +
          `</div>` +
          `<div class="conn-item-actions">` +
            `<button onclick="testConnection('${escapeHtml(c.connection_id)}')">Test</button>` +
            `<button class="btn-danger" onclick="deleteConnection('${escapeHtml(c.connection_id)}')">Remove</button>` +
          `</div>` +
        `</div>`;
    }
    container.innerHTML = html;
  } catch {
    container.innerHTML =
      '<div class="conn-empty">Could not load connections</div>';
  }
}

function _clearConnectionForm() {
  document.getElementById("conn-id").value = "";
  document.getElementById("conn-display-name").value = "";
  document.getElementById("conn-scheme").value = "";
  document.getElementById("conn-cred-fields").innerHTML = "";
}

// ── Artifact side panel ──────────────────────────────────
const artifactPanel = document.getElementById("artifact-panel");
const artifactTitle = document.getElementById("artifact-title");
const artifactBody = document.getElementById("artifact-body");

let _artifactRawContent = "";

function openArtifact(title, htmlContent, rawContent) {
  artifactTitle.textContent = title;
  artifactBody.innerHTML = htmlContent;
  _artifactRawContent = rawContent || htmlContent;
  artifactPanel.classList.add("open");
  // Re-render mermaid if present
  renderMermaidBlocks(artifactBody);
}

function closeArtifact() {
  artifactPanel.classList.remove("open");
}

function copyArtifact() {
  const btn = document.getElementById("artifact-btn-copy");
  navigator.clipboard.writeText(_artifactRawContent).then(() => {
    const orig = btn.innerHTML;
    btn.innerHTML = "&#10003;";
    setTimeout(() => { btn.innerHTML = orig; }, 1500);
  });
}

function popOutArtifact() {
  const w = window.open("", "_blank", "width=800,height=600");
  if (!w) return;
  const theme = document.documentElement.getAttribute("data-theme") || "light";
  w.document.write(`<!DOCTYPE html><html data-theme="${theme}"><head>
    <title>${escapeHtml(artifactTitle.textContent)}</title>
    <link rel="stylesheet" href="/static/style.css" />
    <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"><\/script>
  </head><body style="padding:24px;background:var(--bg);color:var(--text);">
    ${artifactBody.innerHTML}
    <script>mermaid.initialize({startOnLoad:true,theme:'${theme==="dark"?"default":"neutral"}'});<\/script>
  </body></html>`);
  w.document.close();
}

// ── Command palette ──────────────────────────────────────
const cmdPaletteOverlay = document.getElementById("command-palette");
const cmdPaletteInput = document.getElementById("cmd-palette-input");
const cmdPaletteResults = document.getElementById("cmd-palette-results");

const COMMAND_ACTIONS = [
  { group: "Actions", name: "Profile Directory", desc: "Scan and profile all data tables", icon: "&#9776;", message: "Profile all files in the data directory" },
  { group: "Actions", name: "Detect Relationships", desc: "Find FK candidates across tables", icon: "&#10132;", message: "Detect relationships between all profiled tables" },
  { group: "Actions", name: "Enrich & Analyze", desc: "LLM-powered analysis with ER diagram", icon: "&#9830;", message: "Enrich relationships with LLM analysis and generate the ER diagram" },
  { group: "Actions", name: "List Files", desc: "Discover supported data files", icon: "&#128269;", message: "List all supported files in the data directory" },
  { group: "Actions", name: "Reset Vector Store", desc: "Clear stale enrichment data", icon: "&#128260;", message: "Reset the vector store" },
  { group: "Navigation", name: "Clear Chat", desc: "Start a fresh conversation", icon: "&#128465;", action: () => clearChat(), shortcut: "Ctrl+L" },
  { group: "Navigation", name: "Connections", desc: "Manage data source connections", icon: "&#128279;", action: () => openConnectionModal() },
  { group: "Navigation", name: "Toggle Theme", desc: "Switch light/dark mode", icon: "&#127763;", action: () => { document.getElementById("theme-switch").checked = !document.getElementById("theme-switch").checked; toggleTheme(); }, shortcut: "Ctrl+T" },
];

let cmdActiveIndex = 0;

function openCommandPalette() {
  cmdPaletteInput.value = "";
  cmdActiveIndex = 0;
  renderCommandResults("");
  cmdPaletteOverlay.classList.add("visible");
  setTimeout(() => cmdPaletteInput.focus(), 50);
}

function closeCommandPalette() {
  cmdPaletteOverlay.classList.remove("visible");
  chatInput.focus();
}

function renderCommandResults(query) {
  const q = query.toLowerCase().trim();
  let filtered = COMMAND_ACTIONS;
  if (q) {
    filtered = COMMAND_ACTIONS.filter(
      (a) => a.name.toLowerCase().includes(q) || a.desc.toLowerCase().includes(q)
    );
  }

  if (filtered.length === 0) {
    cmdPaletteResults.innerHTML = '<div class="cmd-palette-empty">No matching actions</div>';
    return;
  }

  // Group by category
  const groups = {};
  for (const item of filtered) {
    (groups[item.group] ||= []).push(item);
  }

  let html = "";
  let globalIdx = 0;
  for (const [group, items] of Object.entries(groups)) {
    html += `<div class="cmd-palette-group"><div class="cmd-palette-group-title">${escapeHtml(group)}</div>`;
    for (const item of items) {
      const activeClass = globalIdx === cmdActiveIndex ? " active" : "";
      const shortcutHtml = item.shortcut
        ? `<div class="cmd-palette-item-shortcut">${item.shortcut.split("+").map(k => `<kbd>${k}</kbd>`).join("")}</div>`
        : "";
      html +=
        `<button class="cmd-palette-item${activeClass}" data-cmd-index="${globalIdx}">` +
          `<div class="cmd-palette-item-icon">${item.icon}</div>` +
          `<div class="cmd-palette-item-text">` +
            `<div class="cmd-palette-item-name">${escapeHtml(item.name)}</div>` +
            `<div class="cmd-palette-item-desc">${escapeHtml(item.desc)}</div>` +
          `</div>` +
          shortcutHtml +
        `</button>`;
      globalIdx++;
    }
    html += "</div>";
  }

  cmdPaletteResults.innerHTML = html;

  // Click handlers
  cmdPaletteResults.querySelectorAll(".cmd-palette-item").forEach((btn) => {
    btn.addEventListener("click", () => {
      const idx = parseInt(btn.dataset.cmdIndex);
      executeCommand(filtered[idx]);
    });
  });
}

function executeCommand(cmd) {
  closeCommandPalette();
  if (cmd.action) {
    cmd.action();
  } else if (cmd.message && ws && ws.readyState === WebSocket.OPEN) {
    hideQuickActions();
    addUserMessage(cmd.message);
    ws.send(JSON.stringify({ type: "message", content: cmd.message }));
    chatInput.value = "";
    setInputEnabled(false);
    showProgress(false);
    hideStepTracker();
  }
}

cmdPaletteInput.addEventListener("input", () => {
  cmdActiveIndex = 0;
  renderCommandResults(cmdPaletteInput.value);
});

cmdPaletteInput.addEventListener("keydown", (e) => {
  const items = cmdPaletteResults.querySelectorAll(".cmd-palette-item");
  if (e.key === "ArrowDown") {
    e.preventDefault();
    cmdActiveIndex = Math.min(cmdActiveIndex + 1, items.length - 1);
    updateCmdActive(items);
  } else if (e.key === "ArrowUp") {
    e.preventDefault();
    cmdActiveIndex = Math.max(cmdActiveIndex - 1, 0);
    updateCmdActive(items);
  } else if (e.key === "Enter") {
    e.preventDefault();
    if (items[cmdActiveIndex]) items[cmdActiveIndex].click();
  } else if (e.key === "Escape") {
    closeCommandPalette();
  }
});

function updateCmdActive(items) {
  items.forEach((el, i) => {
    el.classList.toggle("active", i === cmdActiveIndex);
    if (i === cmdActiveIndex) el.scrollIntoView({ block: "nearest" });
  });
}

cmdPaletteOverlay.addEventListener("click", (e) => {
  if (e.target === cmdPaletteOverlay) closeCommandPalette();
});

// ── Follow-up action chips ───────────────────────────────
function getFollowUpChips(assistantText) {
  const lower = assistantText.toLowerCase();
  const chips = [];

  if (lower.includes("profil") && !lower.includes("relationship") && !lower.includes("enrich")) {
    chips.push({ label: "Detect Relationships", icon: "&#10132;", message: "Detect relationships between all profiled tables" });
    chips.push({ label: "Enrich & Analyze", icon: "&#9830;", message: "Enrich relationships with LLM analysis and generate the ER diagram" });
  }
  if (lower.includes("relationship") && lower.includes("candidate") && !lower.includes("enrich")) {
    chips.push({ label: "Enrich & Analyze", icon: "&#9830;", message: "Enrich relationships with LLM analysis and generate the ER diagram" });
  }
  if (lower.includes("er diagram") || lower.includes("enrich")) {
    chips.push({ label: "Profile Another Directory", icon: "&#9776;", message: "Profile all files in the data directory" });
  }
  if (lower.includes("error") || lower.includes("failed")) {
    chips.push({ label: "Reset Vector Store", icon: "&#128260;", message: "Reset the vector store" });
  }

  // Always offer these generic ones if fewer than 2 chips
  if (chips.length < 2) {
    chips.push({ label: "List Files", icon: "&#128269;", message: "List all supported files in the data directory" });
  }

  return chips.slice(0, 3);
}

function renderActionChips(chips) {
  if (chips.length === 0) return "";
  let html = '<div class="action-chips">';
  for (const chip of chips) {
    html += `<button class="action-chip" data-chip-message="${escapeHtml(chip.message)}">` +
      `<span class="action-chip-icon">${chip.icon}</span>${escapeHtml(chip.label)}</button>`;
  }
  html += "</div>";
  return html;
}

function attachChipListeners(container) {
  container.querySelectorAll(".action-chip").forEach((btn) => {
    btn.addEventListener("click", () => {
      const message = btn.dataset.chipMessage;
      if (message && ws && ws.readyState === WebSocket.OPEN) {
        hideQuickActions();
        addUserMessage(message);
        ws.send(JSON.stringify({ type: "message", content: message }));
        chatInput.value = "";
        setInputEnabled(false);
        showProgress(false);
        hideStepTracker();
      }
    });
  });
}

// ── Response action bar ──────────────────────────────────
function buildResponseActionBar() {
  return `<div class="response-actions">` +
    `<button class="response-action-btn" onclick="copyResponseFromMsg(this)" title="Copy">` +
      `<span class="response-action-icon">&#128203;</span> Copy</button>` +
    `</div>`;
}

function copyResponseFromMsg(btn) {
  const msg = btn.closest(".message-assistant");
  if (!msg) return;
  // Get text content excluding action chips and action bar
  const clone = msg.cloneNode(true);
  clone.querySelectorAll(".response-actions, .action-chips").forEach(el => el.remove());
  const text = clone.textContent.trim();
  navigator.clipboard.writeText(text).then(() => {
    btn.classList.add("copied");
    const orig = btn.innerHTML;
    btn.innerHTML = '<span class="response-action-icon">&#10003;</span> Copied';
    setTimeout(() => {
      btn.innerHTML = orig;
      btn.classList.remove("copied");
    }, 1500);
  }).catch(() => {});
}

// ── Animated count-up ────────────────────────────────────
function animateCountUp(el, targetText) {
  if (el.textContent === targetText) return;
  el.textContent = targetText;
  el.classList.add("counting");
  setTimeout(() => el.classList.remove("counting"), 300);
}

// ── Keyboard shortcuts ───────────────────────────────────
document.addEventListener("keydown", (e) => {
  // Cmd/Ctrl+K → command palette
  if ((e.ctrlKey || e.metaKey) && e.key === "k") {
    e.preventDefault();
    if (cmdPaletteOverlay.classList.contains("visible")) {
      closeCommandPalette();
    } else {
      openCommandPalette();
    }
  }

  // Ctrl+L → clear chat
  if ((e.ctrlKey || e.metaKey) && e.key === "l") {
    e.preventDefault();
    clearChat();
  }

  // Ctrl+T → toggle theme
  if ((e.ctrlKey || e.metaKey) && e.key === "t") {
    e.preventDefault();
    document.getElementById("theme-switch").checked = !document.getElementById("theme-switch").checked;
    toggleTheme();
  }

  // Escape → close artifact panel
  if (e.key === "Escape") {
    if (cmdPaletteOverlay.classList.contains("visible")) {
      closeCommandPalette();
    } else if (artifactPanel.classList.contains("open")) {
      closeArtifact();
    }
  }
});

// ── Textarea auto-resize + Enter to send ─────────────────
chatInput.addEventListener("input", () => {
  chatInput.style.height = "auto";
  chatInput.style.height = Math.min(chatInput.scrollHeight, 160) + "px";
});

chatInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    document.getElementById("chat-form").dispatchEvent(new Event("submit", { cancelable: true }));
  }
});

// ── Boot ─────────────────────────────────────────────────
ensureUploadControls();
loadTheme();
renderSessionHistory();
syncSessionsFromServer();  // merge server sessions into sidebar
connect();
chatInput.focus();
