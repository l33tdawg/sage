const $ = (id) => document.getElementById(id);

const DEFAULT_URL = "http://localhost:8080";

async function init() {
  const stored = await chrome.storage.local.get(["sageServerUrl"]);
  const url = stored.sageServerUrl || DEFAULT_URL;
  $("serverUrl").value = url;
  checkConnection(url);
}

async function checkConnection(url) {
  $("statusText").textContent = "Checking connection...";
  $("statusDot").classList.remove("connected");
  $("statsGrid").style.display = "none";

  try {
    const response = await chrome.runtime.sendMessage({
      action: "checkConnection",
      url: url
    });

    if (response && response.ok) {
      $("statusDot").classList.add("connected");
      $("statusText").textContent = "Connected to SAGE";
      showMessage("Connected", "success");
      loadStats(url);
    } else {
      $("statusText").textContent = "Not connected";
      showMessage(response?.error || "Cannot reach SAGE server", "error");
    }
  } catch (e) {
    $("statusText").textContent = "Not connected";
    showMessage("Extension error: " + e.message, "error");
  }
}

async function loadStats(url) {
  try {
    const response = await chrome.runtime.sendMessage({
      action: "getStats",
      url: url
    });

    if (response && response.ok) {
      const stats = response.data;
      $("memoryCount").textContent = stats.total_memories || 0;
      $("domainCount").textContent = stats.domain_count || Object.keys(stats.domains || {}).length || 0;
      $("statsGrid").style.display = "grid";
    }
  } catch (e) {
    // Stats are non-critical
  }
}

function showMessage(text, type) {
  const el = $("message");
  el.textContent = text;
  el.className = "sage-msg " + type;
}

$("testBtn").addEventListener("click", async () => {
  const url = $("serverUrl").value.trim().replace(/\/+$/, "");
  if (!url) {
    showMessage("Please enter a server URL", "error");
    return;
  }

  await chrome.storage.local.set({ sageServerUrl: url });
  $("serverUrl").value = url;
  checkConnection(url);
});

$("serverUrl").addEventListener("keydown", (e) => {
  if (e.key === "Enter") $("testBtn").click();
});

init();
