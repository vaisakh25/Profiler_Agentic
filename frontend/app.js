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

// Mermaid is initialised in loadTheme() at boot

// Initialise marked with custom renderer for mermaid blocks
const renderer = new marked.Renderer();
const originalCodeRenderer = renderer.code.bind(renderer);

marked.setOptions({
  renderer,
  gfm: true,
  breaks: true,
});

// ── WebSocket ────────────────────────────────────────────
function connect() {
  const host = window.location.host;
  ws = new WebSocket(`ws://${host}/ws/chat`);

  ws.onopen = () => {
    setStatus("connected", "Connected");
    // Send config
    ws.send(
      JSON.stringify({
        type: "config",
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
    // Reconnect after 3s
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
      break;

    case "tool_start":
      setStatus("working", `Running ${data.tool}...`);
      addToolMessage(`Calling ${data.tool}...`);
      updateProgress(data.percent, data.stage, `Step ${data.tool_index}`, false);
      showProgress(true);
      break;

    case "tool_result": {
      setStatus("working", "Thinking...");
      const icon = data.success ? "done" : "failed";
      updateLastToolMessage(`${data.tool} — ${icon} — ${data.summary}`);
      updateProgress(data.percent, data.summary, `Step ${data.tool_index}`, false);
      break;
    }

    case "progress":
      updateProgress(data.percent, data.stage, `Step ${data.tool_index}`, data.percent >= 100);
      break;

    case "assistant":
      setStatus("connected", "Connected");
      addAssistantMessage(data.content);
      setInputEnabled(true);
      // Fade out progress bar after a short delay
      setTimeout(() => showProgress(false), 2000);
      break;

    case "error":
      setStatus("connected", "Connected");
      addErrorMessage(data.content);
      setInputEnabled(true);
      setTimeout(() => showProgress(false), 2000);
      break;

    case "milestone":
      addMilestoneMessage(data.percent, data.message);
      break;

    case "thinking":
      setStatus("working", "Thinking...");
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

  addUserMessage(text);
  ws.send(JSON.stringify({ type: "message", content: text }));
  chatInput.value = "";
  setInputEnabled(false);
  // Reset progress for new turn
  showProgress(false);
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
  el.innerHTML = renderMarkdown(text);
  messagesEl.appendChild(el);
  renderMermaidBlocks(el);
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

function updateLastToolMessage(text) {
  const el = document.getElementById("tool-msg-latest");
  if (el) {
    el.querySelector(".tool-label").textContent = text;
    el.removeAttribute("id");
  }
}

function addMilestoneMessage(percent, message) {
  const el = document.createElement("div");
  const isComplete = percent >= 100;
  el.className = `message message-milestone${isComplete ? " complete" : ""}`;
  el.innerHTML =
    `<span class="milestone-bar"><span class="milestone-bar-fill" style="width:${percent}%"></span></span>` +
    `<span>${percent}% — ${escapeHtml(message)}</span>`;
  messagesEl.appendChild(el);
  scrollToBottom();
}

function addErrorMessage(text) {
  const el = document.createElement("div");
  el.className = "message message-error";
  el.textContent = text;
  messagesEl.appendChild(el);
  scrollToBottom();
}

function scrollToBottom() {
  messagesEl.scrollTop = messagesEl.scrollHeight;
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
  // marked.parse handles GFM, tables, code blocks, etc.
  return marked.parse(text);
}

function renderMermaidBlocks(container) {
  // Find code blocks with class "language-mermaid" and render them
  const codeBlocks = container.querySelectorAll("code.language-mermaid");
  codeBlocks.forEach((block) => {
    const pre = block.parentElement;
    const mermaidDiv = document.createElement("div");
    mermaidDiv.className = "mermaid";
    mermaidDiv.textContent = block.textContent;
    pre.replaceWith(mermaidDiv);
  });

  // Run mermaid on new elements
  mermaid.run({ nodes: container.querySelectorAll(".mermaid") });
}

function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// ── Progress bar ─────────────────────────────────────────
function showProgress(visible) {
  if (visible) {
    progressContainer.classList.add("visible");
  } else {
    progressContainer.classList.remove("visible");
    // Reset after transition
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
  // Reconnect with fresh session
  if (ws) ws.close();
  setTimeout(connect, 100);
}

// ── Theme ────────────────────────────────────────────────
function toggleTheme() {
  const isLight = document.getElementById("theme-switch").checked;
  const theme = isLight ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem("theme", theme);
  // Re-init mermaid with matching theme
  mermaid.initialize({
    startOnLoad: false,
    theme: theme === "dark" ? "default" : "neutral",
  });
}

function loadTheme() {
  const saved = localStorage.getItem("theme") || "dark";
  document.documentElement.setAttribute("data-theme", saved);
  document.getElementById("theme-switch").checked = saved === "light";
  mermaid.initialize({
    startOnLoad: false,
    theme: saved === "dark" ? "default" : "neutral",
  });
}

// ── Boot ─────────────────────────────────────────────────
loadTheme();
connect();
chatInput.focus();
