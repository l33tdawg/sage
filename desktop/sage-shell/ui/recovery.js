(() => {
  "use strict";

  const link = document.getElementById("browser-fallback");
  const raw = new URLSearchParams(window.location.search).get("browser");
  if (!link || !raw) return;

  try {
    const url = new URL(raw);
    const loopback = ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname);
    const safe = url.protocol === "http:"
      && loopback
      && url.port !== ""
      && url.username === ""
      && url.password === ""
      && url.pathname === "/ui/"
      && url.search === ""
      && url.hash === "";
    if (!safe) return;
    link.href = url.href;
    link.hidden = false;
  } catch (_) {
    // Malformed fallback input stays hidden. Rust performs the authoritative
    // validation; this renderer-side check is defense in depth only.
  }
})();
