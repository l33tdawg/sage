#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod control;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tauri::{Manager, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_deep_link::DeepLinkExt;
use url::Url;

const MAX_PENDING_ROUTES: usize = 32;
const MAX_ROUTE_BYTES: usize = 2_048;

#[derive(Clone, Debug, PartialEq, Eq)]
struct OriginPin {
    scheme: String,
    host: String,
    port: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryView {
    Starting,
    Locked,
    Draining,
    Failed,
    Unavailable,
    Incompatible,
}

impl RecoveryView {
    fn fragment(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Locked => "locked",
            Self::Draining => "draining",
            Self::Failed => "failed",
            Self::Unavailable => "unavailable",
            Self::Incompatible => "incompatible",
        }
    }
}

#[derive(Default)]
struct ShellSession {
    pin: Option<OriginPin>,
    generation: Option<String>,
    attached: bool,
    pending_routes: VecDeque<String>,
    last_route: Option<String>,
    recovery_view: Option<RecoveryView>,
}

type SharedSession = Arc<Mutex<ShellSession>>;

fn main() {
    let session = Arc::new(Mutex::new(ShellSession::default()));
    if let Some(route) = route_from_args(std::env::args()) {
        enqueue_route(&session, route);
    }

    let single_session = Arc::clone(&session);
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(
            move |app, args, _cwd| {
                if let Some(route) = route_from_args(args) {
                    enqueue_route(&single_session, route);
                }
                focus_main_window(app);
            },
        ))
        .plugin(tauri_plugin_deep_link::init())
        .setup({
            let session = Arc::clone(&session);
            move |app| {
                let nav_session = Arc::clone(&session);
                WebviewWindowBuilder::new(app, "main", WebviewUrl::App("index.html".into()))
                    .title("SAGE")
                    .inner_size(1180.0, 800.0)
                    .min_inner_size(720.0, 520.0)
                    // Windows otherwise maps bundled assets to http://tauri.localhost,
                    // while the fail-closed navigation policy pins the secure asset
                    // origin. This setting makes the builder and policy agree.
                    .use_https_scheme(true)
                    .devtools(cfg!(debug_assertions))
                    .on_navigation(move |url| navigation_allowed(url, &nav_session))
                    .on_new_window(|url, _| {
                        if safe_external_url(&url) {
                            open_external(url.as_str());
                        }
                        tauri::webview::NewWindowResponse::Deny
                    })
                    .build()?;

                let deep_session = Arc::clone(&session);
                let deep_handle = app.handle().clone();
                app.deep_link().on_open_url(move |event| {
                    for url in event.urls() {
                        if let Some(route) = parse_deep_link(url.as_str()) {
                            enqueue_route(&deep_session, route);
                        }
                    }
                    focus_main_window(&deep_handle);
                });

                let app_handle = app.handle().clone();
                thread::spawn(move || {
                    supervise(app_handle.clone(), sage_home(&app_handle), session)
                });
                Ok(())
            }
        })
        .run(tauri::generate_context!())
        .expect("SAGE native shell runtime failed");
}

fn supervise<R: tauri::Runtime>(
    app: tauri::AppHandle<R>,
    sage_home: PathBuf,
    session: SharedSession,
) {
    loop {
        match control::status(&sage_home) {
            Ok(status) if status.state.can_render() => {
                attach_ready(&app, &session, &status);
            }
            Ok(status) => {
                let view = match status.state {
                    control::DaemonState::Starting => RecoveryView::Starting,
                    control::DaemonState::Locked => RecoveryView::Locked,
                    control::DaemonState::Draining => RecoveryView::Draining,
                    control::DaemonState::Failed => RecoveryView::Failed,
                    control::DaemonState::Ready | control::DaemonState::Degraded => unreachable!(),
                };
                show_recovery(&app, &session, view, Some(status.instance_generation));
            }
            Err(error) => {
                let view = if error.is_incompatible() {
                    RecoveryView::Incompatible
                } else {
                    RecoveryView::Unavailable
                };
                show_recovery(&app, &session, view, None);
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn attach_ready<R: tauri::Runtime>(
    app: &tauri::AppHandle<R>,
    session: &SharedSession,
    status: &control::Status,
) {
    let Some(window) = app.get_webview_window("main") else {
        return;
    };
    let Ok(origin) = control::validate_origin(&status.ui_origin) else {
        show_recovery(app, session, RecoveryView::Incompatible, None);
        return;
    };
    let pin = OriginPin {
        scheme: origin.scheme().to_owned(),
        host: origin.host_str().expect("validated host").to_owned(),
        port: origin.port().expect("validated explicit port"),
    };

    let current_url = window.url().ok();
    let (should_navigate, route) = {
        let mut state = session.lock().expect("shell session lock");
        remember_current_route(&mut state, current_url.as_ref());
        let changed = !state.attached
            || state.generation.as_deref() != Some(status.instance_generation.as_str())
            || state.pin.as_ref() != Some(&pin);
        state.pin = Some(pin.clone());
        state.generation = Some(status.instance_generation.clone());
        state.attached = true;
        state.recovery_view = None;
        let route = state
            .pending_routes
            .pop_front()
            .or_else(|| changed.then(|| state.last_route.clone()).flatten());
        if let Some(route) = &route {
            state.last_route = Some(route.clone());
        }
        (changed || route.is_some(), route)
    };

    if should_navigate {
        let url = pinned_ui_url(&pin, route.as_deref());
        if window.navigate(url).is_err() {
            show_recovery(app, session, RecoveryView::Unavailable, None);
        }
    }
}

fn show_recovery<R: tauri::Runtime>(
    app: &tauri::AppHandle<R>,
    session: &SharedSession,
    view: RecoveryView,
    generation: Option<String>,
) {
    let Some(window) = app.get_webview_window("main") else {
        return;
    };
    let current_url = window.url().ok();
    let should_navigate = {
        let mut state = session.lock().expect("shell session lock");
        remember_current_route(&mut state, current_url.as_ref());
        let changed = state.recovery_view != Some(view) || state.attached;
        state.pin = None;
        state.attached = false;
        if let Some(generation) = generation {
            state.generation = Some(generation);
        }
        state.recovery_view = Some(view);
        changed
    };
    if should_navigate {
        let _ = window.navigate(recovery_url(view));
    }
}

fn remember_current_route(state: &mut ShellSession, current: Option<&Url>) {
    let (Some(pin), Some(url)) = (state.pin.as_ref(), current) else {
        return;
    };
    if url_matches_pin(url, pin)
        && let Some(fragment) = url.fragment()
        && valid_route(fragment)
    {
        state.last_route = Some(fragment.to_owned());
    }
}

fn enqueue_route(session: &SharedSession, route: String) {
    let mut state = session.lock().expect("shell session lock");
    if state.pending_routes.len() == MAX_PENDING_ROUTES {
        state.pending_routes.pop_front();
    }
    state.pending_routes.push_back(route);
}

fn focus_main_window<R: tauri::Runtime, M: Manager<R>>(manager: &M) {
    if let Some(window) = manager.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
    }
}

fn sage_home<R: tauri::Runtime>(app: &tauri::AppHandle<R>) -> PathBuf {
    std::env::var_os("SAGE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            app.path()
                .home_dir()
                .expect("home directory unavailable")
                .join(".sage")
        })
}

fn navigation_allowed(url: &Url, session: &SharedSession) -> bool {
    if bundled_asset_url(url) {
        return true;
    }
    let state = session.lock().expect("shell session lock");
    state
        .pin
        .as_ref()
        .is_some_and(|pin| url_matches_pin(url, pin))
}

fn bundled_asset_url(url: &Url) -> bool {
    let no_credentials = url.username().is_empty() && url.password().is_none();
    no_credentials
        && ((url.scheme() == "tauri" && url.host_str() == Some("localhost"))
            || (url.scheme() == "https" && url.host_str() == Some("tauri.localhost")))
}

fn url_matches_pin(url: &Url, pin: &OriginPin) -> bool {
    url.scheme() == pin.scheme
        && url
            .host_str()
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(&pin.host))
        && url.port() == Some(pin.port)
        && url.username().is_empty()
        && url.password().is_none()
}

fn pinned_ui_url(pin: &OriginPin, route: Option<&str>) -> Url {
    let mut url = Url::parse(&format!(
        "{}://{}:{}/ui/",
        pin.scheme,
        format_host(&pin.host),
        pin.port
    ))
    .expect("validated pin builds a URL");
    if let Some(route) = route {
        url.set_fragment(Some(route));
    }
    url
}

fn format_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

#[cfg(windows)]
fn recovery_url(view: RecoveryView) -> Url {
    Url::parse(&format!(
        "https://tauri.localhost/index.html#{}",
        view.fragment()
    ))
    .expect("static recovery URL")
}

#[cfg(not(windows))]
fn recovery_url(view: RecoveryView) -> Url {
    Url::parse(&format!("tauri://localhost/index.html#{}", view.fragment()))
        .expect("static recovery URL")
}

fn route_from_args<I, S>(args: I) -> Option<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    args.into_iter()
        .find_map(|arg| parse_deep_link(arg.as_ref()))
}

fn safe_external_url(url: &Url) -> bool {
    url.scheme() == "https" && url.username().is_empty() && url.password().is_none()
}

#[cfg(target_os = "macos")]
fn open_external(url: &str) {
    let _ = std::process::Command::new("/usr/bin/open").arg(url).spawn();
}

#[cfg(target_os = "linux")]
fn open_external(url: &str) {
    let _ = std::process::Command::new("xdg-open").arg(url).spawn();
}

#[cfg(windows)]
fn open_external(url: &str) {
    use std::iter;
    use windows_sys::Win32::UI::Shell::ShellExecuteW;
    let operation: Vec<u16> = "open".encode_utf16().chain(iter::once(0)).collect();
    let target: Vec<u16> = url.encode_utf16().chain(iter::once(0)).collect();
    unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            operation.as_ptr(),
            target.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
        );
    }
}

fn parse_deep_link(raw: &str) -> Option<String> {
    let url = Url::parse(raw).ok()?;
    if url.scheme() != "sage"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
    {
        return None;
    }
    let host = url.host_str()?;
    if !matches!(host, "brain" | "search" | "pipeline" | "tasks" | "settings") {
        return None;
    }
    let path = url.path().trim_matches('/');
    let route = if path.is_empty() {
        format!("/{host}")
    } else {
        format!("/{host}/{path}")
    };
    valid_route(&route).then_some(route)
}

fn valid_route(route: &str) -> bool {
    !route.is_empty()
        && route.len() <= MAX_ROUTE_BYTES
        && route
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "-_/".contains(character))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_links_are_bounded_routes() {
        assert_eq!(
            parse_deep_link("sage://search/agent-123"),
            Some("/search/agent-123".into())
        );
        assert_eq!(parse_deep_link("sage://tasks"), Some("/tasks".into()));
        assert_eq!(parse_deep_link("sage://admin/root"), None);
        assert_eq!(parse_deep_link("sage://search/a?token=secret"), None);
        assert_eq!(parse_deep_link("javascript:alert(1)"), None);
        assert_eq!(
            parse_deep_link(&format!("sage://search/{}", "a".repeat(MAX_ROUTE_BYTES))),
            None
        );
    }

    #[test]
    fn only_bundled_assets_and_pinned_origin_can_navigate() {
        let session = Arc::new(Mutex::new(ShellSession {
            pin: Some(OriginPin {
                scheme: "http".into(),
                host: "127.0.0.1".into(),
                port: 8080,
            }),
            ..ShellSession::default()
        }));
        assert!(navigation_allowed(
            &Url::parse("http://127.0.0.1:8080/ui/#/brain").unwrap(),
            &session
        ));
        assert!(navigation_allowed(
            &Url::parse("https://tauri.localhost/index.html#starting").unwrap(),
            &session
        ));
        assert!(!navigation_allowed(
            &Url::parse("http://tauri.localhost/index.html").unwrap(),
            &session
        ));
        assert!(!navigation_allowed(
            &Url::parse("http://127.0.0.1:8081/ui/").unwrap(),
            &session
        ));
        assert!(!navigation_allowed(
            &Url::parse("file:///tmp/key").unwrap(),
            &session
        ));
        assert!(!navigation_allowed(
            &Url::parse("data:text/html,nope").unwrap(),
            &session
        ));
    }

    #[test]
    fn external_handoff_accepts_only_credential_free_https() {
        assert!(safe_external_url(
            &Url::parse("https://sage.foundation/docs").unwrap()
        ));
        assert!(!safe_external_url(
            &Url::parse("http://sage.foundation/docs").unwrap()
        ));
        assert!(!safe_external_url(
            &Url::parse("https://user@sage.foundation/docs").unwrap()
        ));
    }

    #[test]
    fn pending_routes_are_bounded_and_ordered() {
        let session = Arc::new(Mutex::new(ShellSession::default()));
        for index in 0..(MAX_PENDING_ROUTES + 2) {
            enqueue_route(&session, format!("/tasks/{index}"));
        }
        let state = session.lock().unwrap();
        assert_eq!(state.pending_routes.len(), MAX_PENDING_ROUTES);
        assert_eq!(state.pending_routes.front().unwrap(), "/tasks/2");
    }
}
