/**
 * SAGE Chrome Extension — Content Script
 *
 * Runs on chatgpt.com / chat.openai.com.
 * Injects sidebar, monitors responses for [SAGE_CALL:] patterns,
 * and provides manual tool execution.
 */

(function () {
  "use strict";

  if (document.querySelector(".sage-fab")) return; // Already injected

  let sidebarOpen = false;
  let connected = false;
  let toolLog = [];

  // --- Create UI Elements ---

  // Floating action button
  const fab = document.createElement("button");
  fab.className = "sage-fab";
  fab.textContent = "\u{1F9E0}";
  fab.title = "SAGE Memory";
  document.body.appendChild(fab);

  // Sidebar
  const sidebar = document.createElement("div");
  sidebar.className = "sage-sidebar";
  sidebar.innerHTML = `
    <div class="sage-sidebar-header">
      <span class="sage-sidebar-title">SAGE</span>
      <button class="sage-sidebar-close">&times;</button>
    </div>
    <div class="sage-sidebar-body">
      <div class="sage-conn-status">
        <div class="sage-conn-dot" id="sage-conn-dot"></div>
        <div class="sage-conn-label" id="sage-conn-label">Checking...</div>
      </div>

      <div class="sage-sidebar-stats" id="sage-stats-row" style="display:none">
        <div class="sage-sidebar-stat">
          <div class="sage-sidebar-stat-val" id="sage-mem-count">0</div>
          <div class="sage-sidebar-stat-lbl">Memories</div>
        </div>
        <div class="sage-sidebar-stat">
          <div class="sage-sidebar-stat-val" id="sage-dom-count">0</div>
          <div class="sage-sidebar-stat-lbl">Domains</div>
        </div>
      </div>

      <div class="sage-section-label">Quick Actions</div>
      <div class="sage-actions">
        <button class="sage-action-btn" data-tool="sage_inception">
          <span class="sage-action-icon">\u{1F48A}</span>
          <div class="sage-action-text">
            <div class="sage-action-name">Wake Up</div>
            <div class="sage-action-desc">Initialize persistent memory</div>
          </div>
        </button>
        <button class="sage-action-btn" data-tool="sage_turn">
          <span class="sage-action-icon">\u{1F504}</span>
          <div class="sage-action-text">
            <div class="sage-action-name">Turn</div>
            <div class="sage-action-desc">Record turn + recall memories</div>
          </div>
        </button>
        <button class="sage-action-btn" data-tool="sage_status">
          <span class="sage-action-icon">\u{1F4CA}</span>
          <div class="sage-action-text">
            <div class="sage-action-name">Status</div>
            <div class="sage-action-desc">Memory store statistics</div>
          </div>
        </button>
        <button class="sage-action-btn" data-tool="sage_recall">
          <span class="sage-action-icon">\u{1F50D}</span>
          <div class="sage-action-text">
            <div class="sage-action-name">Recall</div>
            <div class="sage-action-desc">Search memories</div>
          </div>
        </button>
        <button class="sage-action-btn" id="sage-inject-prompt-btn">
          <span class="sage-action-icon">\u{1F4DD}</span>
          <div class="sage-action-text">
            <div class="sage-action-name">Inject Prompt</div>
            <div class="sage-action-desc">Tell ChatGPT about SAGE tools</div>
          </div>
        </button>
      </div>

      <div class="sage-section-label">Tool Call Log</div>
      <div class="sage-log" id="sage-log"></div>
    </div>
  `;
  document.body.appendChild(sidebar);

  // --- Event Handlers ---

  fab.addEventListener("click", () => {
    sidebarOpen = !sidebarOpen;
    sidebar.classList.toggle("open", sidebarOpen);
    fab.classList.toggle("active", sidebarOpen);
    if (sidebarOpen) refreshStatus();
  });

  sidebar.querySelector(".sage-sidebar-close").addEventListener("click", () => {
    sidebarOpen = false;
    sidebar.classList.remove("open");
    fab.classList.remove("active");
  });

  // Quick action buttons
  sidebar.querySelectorAll(".sage-action-btn[data-tool]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tool = btn.dataset.tool;
      handleQuickAction(tool);
    });
  });

  document.getElementById("sage-inject-prompt-btn").addEventListener("click", injectSystemPrompt);

  // --- Connection & Stats ---

  async function refreshStatus() {
    try {
      const resp = await chrome.runtime.sendMessage({ action: "getStatus" });
      connected = resp && resp.connected;
      document.getElementById("sage-conn-dot").classList.toggle("connected", connected);
      document.getElementById("sage-conn-label").textContent = connected ? "Connected" : "Not connected";

      if (connected) {
        const statsResp = await chrome.runtime.sendMessage({ action: "getStats" });
        if (statsResp && statsResp.ok) {
          document.getElementById("sage-stats-row").style.display = "grid";
          document.getElementById("sage-mem-count").textContent = statsResp.data.total_memories || 0;
          document.getElementById("sage-dom-count").textContent =
            statsResp.data.domain_count || Object.keys(statsResp.data.domains || {}).length || 0;
        }
      }
    } catch (e) {
      connected = false;
      document.getElementById("sage-conn-dot").classList.remove("connected");
      document.getElementById("sage-conn-label").textContent = "Extension error";
    }
  }

  // --- Quick Actions ---

  async function handleQuickAction(tool) {
    let params = {};

    if (tool === "sage_turn") {
      const topic = prompt("Topic for this turn:");
      if (!topic) return;
      const observation = prompt("Observation (what happened):");
      params = { topic, observation: observation || "", domain: "chatgpt" };
    } else if (tool === "sage_recall") {
      const query = prompt("Search query:");
      if (!query) return;
      params = { query };
    }

    await executeAndLog(tool, params);
  }

  async function executeAndLog(tool, params) {
    const entry = { tool, time: new Date(), status: "executing" };
    toolLog.unshift(entry);
    renderLog();

    try {
      const resp = await chrome.runtime.sendMessage({ action: "callTool", tool, params });
      if (resp && resp.ok) {
        entry.status = "success";
        entry.result = JSON.stringify(resp.data, null, 2);
        pasteResultToChat(tool, resp.data);
      } else {
        entry.status = "error";
        entry.result = resp?.error || "Unknown error";
      }
    } catch (e) {
      entry.status = "error";
      entry.result = e.message;
    }

    renderLog();
    refreshStatus();
  }

  function renderLog() {
    const logEl = document.getElementById("sage-log");
    logEl.innerHTML = toolLog
      .slice(0, 20)
      .map((e) => {
        const time = e.time.toLocaleTimeString();
        const cls = e.status === "error" ? "error" : e.status === "success" ? "success" : "";
        return `<div class="sage-log-entry">
          <span class="sage-log-tool">${e.tool}</span>
          <span class="sage-log-time">${time}</span>
          ${e.result ? `<div class="sage-log-result ${cls}">${escapeHtml(truncate(e.result, 200))}</div>` : ""}
        </div>`;
      })
      .join("");
  }

  // --- Chat Interaction ---

  function getTextarea() {
    // ChatGPT uses a contenteditable div or textarea — try both
    return (
      document.querySelector("#prompt-textarea") ||
      document.querySelector('textarea[data-id="root"]') ||
      document.querySelector("textarea") ||
      document.querySelector('[contenteditable="true"][id="prompt-textarea"]')
    );
  }

  function pasteResultToChat(tool, data) {
    const textarea = getTextarea();
    if (!textarea) return;

    const formatted = `[SAGE Result for ${tool}]:\n${JSON.stringify(data, null, 2)}`;

    if (textarea.tagName === "TEXTAREA") {
      const nativeSetter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value").set;
      nativeSetter.call(textarea, textarea.value + "\n" + formatted);
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
    } else {
      // Contenteditable div
      textarea.focus();
      const text = textarea.innerText || "";
      textarea.innerText = text + "\n" + formatted;
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
    }
  }

  function injectSystemPrompt() {
    const textarea = getTextarea();
    if (!textarea) {
      alert("Cannot find ChatGPT input box. Make sure you are on a chat page.");
      return;
    }

    const prompt = window.SAGE_SYSTEM_PROMPT;
    if (textarea.tagName === "TEXTAREA") {
      const nativeSetter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value").set;
      nativeSetter.call(textarea, prompt);
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
    } else {
      textarea.focus();
      textarea.innerText = prompt;
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
    }

    addLogEntry("inject_prompt", "success", "System prompt injected into chat input");
  }

  function addLogEntry(tool, status, result) {
    toolLog.unshift({ tool, time: new Date(), status, result });
    renderLog();
  }

  // --- Response Monitoring (MutationObserver) ---

  let observer = null;

  function startMonitoring() {
    if (observer) return;

    observer = new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        for (const node of mutation.addedNodes) {
          if (node.nodeType !== Node.ELEMENT_NODE) continue;
          // Look for assistant messages
          const texts = node.querySelectorAll
            ? node.querySelectorAll('[data-message-author-role="assistant"] .markdown, .agent-turn .markdown')
            : [];
          texts.forEach(checkForSageCalls);
          // Also check the node itself
          if (node.matches && node.matches('[data-message-author-role="assistant"]')) {
            const md = node.querySelector(".markdown");
            if (md) checkForSageCalls(md);
          }
        }
      }
    });

    observer.observe(document.body, { childList: true, subtree: true });
  }

  const processedCalls = new Set();

  function checkForSageCalls(element) {
    const text = element.textContent || "";
    const calls = window.parseSageCalls(text);
    if (calls.length === 0) return;

    for (const call of calls) {
      const key = call.raw + "|" + element.closest("[data-message-id]")?.dataset?.messageId;
      if (processedCalls.has(key)) continue;
      processedCalls.add(key);

      // Execute the tool call
      executeAndLog(call.tool, call.params);
    }
  }

  // --- Utilities ---

  function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function truncate(str, len) {
    return str.length > len ? str.substring(0, len) + "..." : str;
  }

  // --- Init ---

  refreshStatus();
  startMonitoring();

  // Re-check connection periodically
  setInterval(refreshStatus, 30000);
})();
