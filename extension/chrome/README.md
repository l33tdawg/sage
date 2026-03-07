# SAGE Chrome Extension for ChatGPT

Persistent AI memory for ChatGPT, powered by SAGE (Sovereign Agent Governed Experience).

## What it does

This extension bridges ChatGPT's web interface with your local SAGE memory node, giving ChatGPT persistent, consensus-validated memory across conversations.

- **Sidebar panel** on ChatGPT with connection status, memory stats, and quick-action buttons
- **Response monitoring** detects `[SAGE_CALL: ...]` patterns in ChatGPT's responses and auto-executes them against your local SAGE node
- **System prompt injection** teaches ChatGPT the SAGE tool format with one click
- **Ed25519 signed requests** for authenticated communication with your SAGE node

## Install

1. Open `chrome://extensions/` in Chrome
2. Enable "Developer mode" (top right toggle)
3. Click "Load unpacked" and select this `extension/chrome/` directory
4. **Generate icons first**: Open `icons/generate-icons.html` in your browser, download the three PNG files, and save them to the `icons/` directory

## How it works

1. Click the brain button on ChatGPT to open the SAGE sidebar
2. Click "Inject Prompt" to teach ChatGPT about SAGE tools
3. ChatGPT will start using `[SAGE_CALL: tool_name({params})]` syntax in its responses
4. The extension detects these patterns, executes them against `http://localhost:8080`, and pastes results back
5. You can also run tools manually from the sidebar

### The [SAGE_CALL] pattern

Since ChatGPT's web UI does not expose MCP or tool-calling APIs to extensions, this extension uses a text-based interception pattern:

- ChatGPT is told (via system prompt) to emit `[SAGE_CALL: sage_turn({"topic": "..."})]` in its responses
- A MutationObserver watches for these patterns in the DOM
- When found, the extension executes the call via the background service worker
- Results are injected back into the chat input

## Requirements

- SAGE running locally on `http://localhost:8080` (configurable in popup)
- Chrome 109+ (for Ed25519 Web Crypto support)

## Architecture

```
ChatGPT Web UI
    |
    v
content.js (MutationObserver + sidebar UI)
    |
    v (chrome.runtime.sendMessage)
background.js (Ed25519 signing + REST API calls)
    |
    v (fetch with signed headers)
SAGE Node (localhost:8080)
```

### Files

| File | Purpose |
|------|---------|
| `manifest.json` | Chrome extension manifest (Manifest V3) |
| `background.js` | Service worker: API calls, Ed25519 signing, tool execution |
| `content.js` | ChatGPT page injection: sidebar, monitoring, prompt injection |
| `content.css` | Sidebar and UI styles (SAGE neural theme) |
| `sage-tools.js` | Tool definitions, [SAGE_CALL] parser, system prompt |
| `popup.html/js/css` | Extension popup: connection config and status |
| `icons/` | Extension icons (generate from `generate-icons.html`) |

## Available SAGE tools

| Tool | Description |
|------|-------------|
| `sage_inception` | Initialize persistent memory |
| `sage_turn` | Per-turn recall + store (call every turn) |
| `sage_recall` | Semantic memory search |
| `sage_remember` | Store a new memory |
| `sage_forget` | Deprecate a memory |
| `sage_reflect` | Post-task dos and don'ts |
| `sage_status` | Memory statistics |
| `sage_list` | Browse memories with filters |
| `sage_timeline` | Time-range memory view |
