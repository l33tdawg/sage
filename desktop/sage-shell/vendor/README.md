# Wry nullable URL backport

`wry-0.55.1/` is the complete published Wry 0.55.1 crate, including its
Apache-2.0 and MIT licenses. The source archive is:

https://static.crates.io/crates/wry/wry-0.55.1.crate

Its SHA-256, verified against the original Cargo.lock and the official crates.io
index, is:

`186f9871daa55fd9c016578b810d149de58367113db7fb72b462d2323ce19514`

Only these source files differ from that archive:

- `src/wkwebview/mod.rs`: return `Error::Io` when `WKWebView.URL` or its
  `absoluteString` is nil.
- `src/wkwebview/navigation.rs`: cancel a navigation with a nil request URL or
  `absoluteString`, invoking its decision handler once before returning.

These guards backport the two code changes from the official Wry PR
[tauri-apps/wry#1857](https://github.com/tauri-apps/wry/pull/1857), commit
`3fc2e2f75881d332fa5aec7c64544793ddca8ffa`. That PR is unmerged as of
2026-11-01. No other crate code, metadata, examples, or licenses have changed.

Tauri installs a page-load delegate that calls the same URL getter used by
`WebviewWindow::url`. Guarding only SAGE's calls cannot protect that delegate.
Returning an error also matters: returning an empty successful URL would reach
Tauri's URL parser and panic there.

`Cargo.toml` patches crates.io Wry to this local directory. The application lock
file changes only Wry's source identity; every package version stays pinned.
Remove this backport after a compatible upstream release contains both guards
and passes the native runtime, offline startup, and performance gates.

To audit the baseline, verify the archive digest above, extract it, and compare
its complete file tree with `wry-0.55.1/`. Only the two files listed above should
differ.
