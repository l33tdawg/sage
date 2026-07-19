#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod control;

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tauri::{Manager, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_deep_link::DeepLinkExt;
use url::Url;

const MAX_PENDING_ROUTES: usize = 32;
const MAX_ROUTE_BYTES: usize = 2_048;
const SHELL_STARTUP_CHALLENGE_ENV: &str = "SAGE_SHELL_STARTUP_CHALLENGE";
const DAEMON_ALREADY_RUNNING_EXIT_CODE: i32 = 73;

#[derive(Clone, Debug, PartialEq, Eq)]
struct OriginPin {
    scheme: String,
    host: String,
    port: u16,
}

impl OriginPin {
    fn from_url(url: &Url) -> Self {
        Self {
            scheme: url.scheme().to_owned(),
            host: url.host_str().expect("validated host").to_owned(),
            port: url.port().expect("validated explicit port"),
        }
    }
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
    browser_fallback: Option<OriginPin>,
}

type SharedSession = Arc<Mutex<ShellSession>>;

struct LaunchAttempt {
    expected_proof: String,
    child: Option<Child>,
    attach_existing: bool,
}

impl LaunchAttempt {
    fn observe_exit(&mut self) -> Result<(), String> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        match child.try_wait().map_err(|error| error.to_string())? {
            Some(status) => {
                self.attach_existing = allows_existing_attachment_exit_code(status.code());
                self.child = None;
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn allows_existing_attachment(&mut self) -> Result<bool, String> {
        self.observe_exit()?;
        Ok(self.attach_existing)
    }

    fn hand_off(mut self) {
        if let Some(mut child) = self.child.take() {
            thread::spawn(move || {
                let _ = child.wait();
            });
        }
    }
}

fn allows_existing_attachment_exit_code(code: Option<i32>) -> bool {
    code == Some(DAEMON_ALREADY_RUNNING_EXIT_CODE)
}

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
                    .on_new_window({
                        let external_session = Arc::clone(&session);
                        move |url, _| {
                            if external_url_allowed(&url, &external_session) {
                                open_external(url.as_str());
                            }
                            tauri::webview::NewWindowResponse::Deny
                        }
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
    let mut launch_attempt = None;
    let mut launch_attempted = false;
    loop {
        match control::status(&sage_home) {
            Ok(status) if status.state.can_render() => {
                if status_matches_launch_attempt(&status, &mut launch_attempt) {
                    if let Some(attempt) = launch_attempt.take() {
                        attempt.hand_off();
                    }
                    attach_ready(&app, &session, &status);
                } else {
                    show_recovery(&app, &session, RecoveryView::Incompatible, None);
                }
            }
            Ok(status) => {
                if status_matches_launch_attempt(&status, &mut launch_attempt) {
                    let view = match status.state {
                        control::DaemonState::Starting => RecoveryView::Starting,
                        control::DaemonState::Locked => RecoveryView::Locked,
                        control::DaemonState::Draining => RecoveryView::Draining,
                        control::DaemonState::Failed => RecoveryView::Failed,
                        control::DaemonState::Ready | control::DaemonState::Degraded => {
                            unreachable!()
                        }
                    };
                    show_recovery(&app, &session, view, Some(status.instance_generation));
                } else {
                    show_recovery(&app, &session, RecoveryView::Incompatible, None);
                }
            }
            Err(error) => {
                if let Some(attempt) = launch_attempt.as_mut() {
                    let _ = attempt.observe_exit();
                }
                let view = if error.is_incompatible() {
                    RecoveryView::Incompatible
                } else {
                    if !launch_attempted {
                        launch_attempted = true;
                        if let Ok(attempt) = launch_bundled_daemon(&app, &sage_home) {
                            launch_attempt = Some(attempt);
                            show_recovery(&app, &session, RecoveryView::Starting, None);
                            thread::sleep(Duration::from_millis(250));
                            continue;
                        }
                    }
                    RecoveryView::Unavailable
                };
                if view == RecoveryView::Incompatible {
                    let browser_fallback_allowed =
                        launch_attempt_allows_browser_fallback(&error, &mut launch_attempt);
                    let browser_fallback = if browser_fallback_allowed {
                        error.browser_origin().map(OriginPin::from_url)
                    } else {
                        None
                    };
                    show_recovery_with_browser(&app, &session, view, None, browser_fallback);
                } else {
                    show_recovery(&app, &session, view, None);
                }
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn launch_attempt_allows_browser_fallback(
    error: &control::StatusError,
    launch_attempt: &mut Option<LaunchAttempt>,
) -> bool {
    let Some(attempt) = launch_attempt.as_mut() else {
        return true;
    };
    // A browser handoff still requires the startup proof unless the child
    // conclusively lost the daemon instance-lock race.
    error.startup_proof() == Some(attempt.expected_proof.as_str())
        || attempt.allows_existing_attachment().unwrap_or(false)
}

fn status_matches_launch_attempt(
    status: &control::Status,
    launch_attempt: &mut Option<LaunchAttempt>,
) -> bool {
    let Some(attempt) = launch_attempt.as_mut() else {
        return true;
    };
    if status.startup_proof == attempt.expected_proof {
        return true;
    }
    // A daemon that was already acquiring the authoritative instance lock can
    // win a race with our bounded launch. Only the dedicated lock-contention
    // exit result means this child never owned the daemon; any other child exit
    // leaves the startup proof mandatory and fails closed.
    attempt.allows_existing_attachment().unwrap_or(false)
}

fn launch_bundled_daemon<R: tauri::Runtime>(
    app: &tauri::AppHandle<R>,
    sage_home: &std::path::Path,
) -> Result<LaunchAttempt, String> {
    let daemon = bundled_daemon_path(app)?;
    let mut launch_log = open_launch_log(sage_home)?;
    writeln!(launch_log, "\n--- SAGE native shell daemon launch ---")
        .map_err(|error| format!("write native-shell launch log: {error}"))?;
    let launch_stdout = launch_log
        .try_clone()
        .map_err(|error| format!("duplicate native-shell launch log: {error}"))?;
    let mut challenge = [0u8; 32];
    getrandom::fill(&mut challenge).map_err(|error| error.to_string())?;
    let expected_proof = control::startup_proof(&challenge);
    let mut child = Command::new(daemon)
        .arg("serve")
        .env("SAGE_NO_BROWSER", "1")
        .env(SHELL_STARTUP_CHALLENGE_ENV, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::from(launch_stdout))
        .stderr(Stdio::from(launch_log))
        .spawn()
        .map_err(|error| format!("start bundled SAGE daemon: {error}"))?;
    let Some(mut stdin) = child.stdin.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err("bundled SAGE daemon did not expose its startup pipe".into());
    };
    if let Err(error) = stdin.write_all(&challenge) {
        drop(stdin);
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!("send native-shell startup challenge: {error}"));
    }
    // Closing the anonymous pipe is part of the one-shot protocol. The daemon
    // reads exactly 32 bytes before it creates the control endpoint.
    drop(stdin);
    Ok(LaunchAttempt {
        expected_proof,
        child: Some(child),
        attach_existing: false,
    })
}

fn open_launch_log(sage_home: &std::path::Path) -> Result<File, String> {
    let log_dir = sage_home.join("logs");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder
            .create(&log_dir)
            .map_err(|error| format!("create SAGE log directory: {error}"))?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(&log_dir)
        .map_err(|error| format!("create SAGE log directory: {error}"))?;

    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(log_dir.join("sage.log"))
        .map_err(|error| format!("open SAGE launch log: {error}"))
}

fn bundled_daemon_path<R: tauri::Runtime>(app: &tauri::AppHandle<R>) -> Result<PathBuf, String> {
    let filename = if cfg!(windows) {
        "sage-gui.exe"
    } else {
        "sage-gui"
    };
    let path = app
        .path()
        .resource_dir()
        .map_err(|error| format!("locate bundled SAGE daemon: {error}"))?
        .join("binaries")
        .join(filename);
    let metadata = std::fs::metadata(&path)
        .map_err(|error| format!("bundled SAGE daemon is unavailable: {error}"))?;
    if !metadata.is_file() {
        return Err("bundled SAGE daemon path is not a regular file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err("bundled SAGE daemon is not executable".into());
        }
    }
    Ok(path)
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
    let pin = OriginPin::from_url(&origin);

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
        state.browser_fallback = None;
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
    show_recovery_with_browser(app, session, view, generation, None);
}

fn show_recovery_with_browser<R: tauri::Runtime>(
    app: &tauri::AppHandle<R>,
    session: &SharedSession,
    view: RecoveryView,
    generation: Option<String>,
    browser_fallback: Option<OriginPin>,
) {
    let Some(window) = app.get_webview_window("main") else {
        return;
    };
    let current_url = window.url().ok();
    let should_navigate = {
        let mut state = session.lock().expect("shell session lock");
        remember_current_route(&mut state, current_url.as_ref());
        let changed = state.recovery_view != Some(view)
            || state.attached
            || state.browser_fallback != browser_fallback;
        state.pin = None;
        state.attached = false;
        if let Some(generation) = generation {
            state.generation = Some(generation);
        }
        state.recovery_view = Some(view);
        state.browser_fallback = browser_fallback.clone();
        changed
    };
    if should_navigate {
        let _ = window.navigate(recovery_url(view, browser_fallback.as_ref()));
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
fn recovery_base_url() -> Url {
    Url::parse("https://tauri.localhost/index.html").expect("static recovery URL")
}

#[cfg(not(windows))]
fn recovery_base_url() -> Url {
    Url::parse("tauri://localhost/index.html").expect("static recovery URL")
}

fn recovery_url(view: RecoveryView, browser_fallback: Option<&OriginPin>) -> Url {
    let mut url = recovery_base_url();
    if let Some(pin) = browser_fallback {
        let browser_url = pinned_ui_url(pin, None);
        url.query_pairs_mut()
            .append_pair("browser", browser_url.as_str());
    }
    url.set_fragment(Some(view.fragment()));
    url
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

fn external_url_allowed(url: &Url, session: &SharedSession) -> bool {
    if safe_external_url(url) {
        return true;
    }
    let state = session.lock().expect("shell session lock");
    state.browser_fallback.as_ref().is_some_and(|pin| {
        url_matches_pin(url, pin)
            && url.path() == "/ui/"
            && url.query().is_none()
            && url.fragment().is_none()
    })
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
    fn browser_fallback_is_exactly_the_authenticated_ui_root() {
        let session = Arc::new(Mutex::new(ShellSession {
            browser_fallback: Some(OriginPin {
                scheme: "http".into(),
                host: "127.0.0.1".into(),
                port: 8080,
            }),
            ..ShellSession::default()
        }));
        assert!(external_url_allowed(
            &Url::parse("http://127.0.0.1:8080/ui/").unwrap(),
            &session
        ));
        assert!(!external_url_allowed(
            &Url::parse("http://127.0.0.1:8080/v1/memory").unwrap(),
            &session
        ));
        assert!(!external_url_allowed(
            &Url::parse("http://127.0.0.1:8081/ui/").unwrap(),
            &session
        ));
    }

    #[test]
    fn recovery_url_carries_only_the_validated_browser_fallback() {
        let pin = OriginPin {
            scheme: "http".into(),
            host: "127.0.0.1".into(),
            port: 8080,
        };
        let url = recovery_url(RecoveryView::Incompatible, Some(&pin));
        assert_eq!(url.fragment(), Some("incompatible"));
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key == "browser")
                .map(|(_, value)| value.into_owned()),
            Some("http://127.0.0.1:8080/ui/".into())
        );
    }

    #[test]
    fn launch_output_uses_the_existing_sage_log_location() {
        let home =
            std::env::temp_dir().join(format!("sage-shell-launch-log-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        let mut log = open_launch_log(&home).expect("open launch log");
        writeln!(log, "fixture launch output").expect("write launch log");
        drop(log);

        let path = home.join("logs").join("sage.log");
        assert_eq!(
            std::fs::read_to_string(&path).expect("read launch log"),
            "fixture launch output\n"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path)
                .expect("stat launch log")
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0);
        }
        std::fs::remove_dir_all(home).expect("remove launch log fixture");
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

    #[test]
    fn only_explicit_lock_contention_allows_attachment_without_proof() {
        assert!(allows_existing_attachment_exit_code(Some(
            DAEMON_ALREADY_RUNNING_EXIT_CODE
        )));
        assert!(!allows_existing_attachment_exit_code(Some(1)));
        assert!(!allows_existing_attachment_exit_code(None));
    }

    #[test]
    fn browser_fallback_keeps_the_spawn_proof_boundary() {
        let matching = control::StatusError::Incompatible {
            message: "version skew".into(),
            browser_origin: Some(Box::new(
                Url::parse("http://127.0.0.1:8080").unwrap(),
            )),
            startup_proof: Some("expected".into()),
        };
        let missing = control::StatusError::Incompatible {
            message: "version skew".into(),
            browser_origin: Some(Box::new(
                Url::parse("http://127.0.0.1:8080").unwrap(),
            )),
            startup_proof: None,
        };

        let mut ordinary_attach = None;
        assert!(launch_attempt_allows_browser_fallback(
            &missing,
            &mut ordinary_attach
        ));

        let mut spawned = Some(LaunchAttempt {
            expected_proof: "expected".into(),
            child: None,
            attach_existing: false,
        });
        assert!(launch_attempt_allows_browser_fallback(
            &matching,
            &mut spawned
        ));
        assert!(!launch_attempt_allows_browser_fallback(
            &missing,
            &mut spawned
        ));

        spawned.as_mut().unwrap().attach_existing = true;
        assert!(launch_attempt_allows_browser_fallback(
            &missing,
            &mut spawned
        ));
    }
}
