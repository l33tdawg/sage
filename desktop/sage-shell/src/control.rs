use serde::Deserialize;
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::io::{Read, Write};
use std::time::Duration;

const MAX_FRAME: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DaemonState {
    Starting,
    Locked,
    Ready,
    Degraded,
    Draining,
    Failed,
}

impl DaemonState {
    pub fn can_render(self) -> bool {
        matches!(self, Self::Ready | Self::Degraded)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Status {
    pub control_protocol: u32,
    pub daemon_version: String,
    pub api_schema: u32,
    pub min_shell_protocol: u32,
    pub max_shell_protocol: u32,
    pub instance_generation: String,
    pub state: DaemonState,
    #[serde(default)]
    pub ui_origin: String,
    // Present only for a daemon this shell launched with an inherited startup
    // challenge. It is a SHA-256 proof, never the challenge itself.
    #[serde(default)]
    pub startup_proof: String,
}

#[derive(Debug)]
pub enum StatusError {
    Unavailable(String),
    Incompatible {
        message: String,
        browser_origin: Option<Box<url::Url>>,
        startup_proof: Option<String>,
    },
}

impl StatusError {
    pub fn is_incompatible(&self) -> bool {
        matches!(self, Self::Incompatible { .. })
    }

    pub fn browser_origin(&self) -> Option<&url::Url> {
        match self {
            Self::Unavailable(_) => None,
            Self::Incompatible { browser_origin, .. } => browser_origin.as_deref(),
        }
    }

    pub fn startup_proof(&self) -> Option<&str> {
        match self {
            Self::Unavailable(_) => None,
            Self::Incompatible { startup_proof, .. } => startup_proof.as_deref(),
        }
    }
}

impl std::fmt::Display for StatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Unavailable(message) | Self::Incompatible { message, .. } => message,
        };
        formatter.write_str(message)
    }
}

pub fn status(sage_home: &std::path::Path) -> Result<Status, StatusError> {
    let request = br#"{"control_protocol":1,"shell_protocol":1,"operation":"status"}"#;
    let mut stream = connect(sage_home).map_err(StatusError::Unavailable)?;
    stream
        .set_timeouts(Duration::from_secs(1))
        .map_err(|error| StatusError::Unavailable(error.to_string()))?;
    write_frame(&mut *stream, request).map_err(StatusError::Unavailable)?;
    let response = read_frame(&mut *stream).map_err(StatusError::Unavailable)?;
    let status: Status =
        serde_json::from_slice(&response).map_err(|error| StatusError::Incompatible {
            message: error.to_string(),
            browser_origin: None,
            startup_proof: None,
        })?;
    if let Err(message) = validate(&status) {
        let browser_origin = browser_fallback_origin(&status).map(Box::new);
        let startup_proof =
            valid_startup_proof(&status.startup_proof).then(|| status.startup_proof.clone());
        return Err(StatusError::Incompatible {
            message,
            browser_origin,
            startup_proof,
        });
    }
    Ok(status)
}

fn browser_fallback_origin(status: &Status) -> Option<url::Url> {
    if status.state.can_render() {
        validate_origin(&status.ui_origin).ok()
    } else {
        None
    }
}

fn validate(status: &Status) -> Result<(), String> {
    if status.control_protocol != 1
        || status.api_schema != 1
        || status.min_shell_protocol > 1
        || status.max_shell_protocol < 1
        || status.min_shell_protocol > status.max_shell_protocol
        || !valid_generation(&status.instance_generation)
        || !supported_daemon_version(&status.daemon_version)
    {
        return Err("incompatible SAGE daemon control response".into());
    }
    if status.state.can_render() {
        validate_origin(&status.ui_origin)?;
    } else if !status.ui_origin.is_empty() {
        return Err("daemon exposed a UI origin before readiness".into());
    }
    if !status.startup_proof.is_empty() && !valid_startup_proof(&status.startup_proof) {
        return Err("daemon returned an invalid native-shell startup proof".into());
    }
    Ok(())
}

pub fn startup_proof(challenge: &[u8; 32]) -> String {
    Sha256::digest(challenge)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn valid_startup_proof(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn valid_generation(value: &str) -> bool {
    // A canonical unpadded base64url encoding of 32 bytes is 43 characters.
    // For the final four source bits the last alphabet index must be divisible
    // by four; enforcing that prevents accepting non-canonical aliases.
    const CANONICAL_LAST: &str = "AEIMQUYcgkosw048";
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        && value
            .chars()
            .last()
            .is_some_and(|last| CANONICAL_LAST.contains(last))
}

fn supported_daemon_version(value: &str) -> bool {
    if cfg!(debug_assertions) && value == "dev" {
        return true;
    }
    let value = value.strip_prefix('v').unwrap_or(value);
    let (without_build, build) = value
        .split_once('+')
        .map_or((value, None), |(core, build)| (core, Some(build)));
    if build.is_some_and(|identifiers| !valid_semver_identifiers(identifiers)) {
        return false;
    }
    let (core, prerelease) = without_build
        .split_once('-')
        .map_or((without_build, None), |(core, pre)| (core, Some(pre)));
    if prerelease.is_some_and(|identifiers| !valid_semver_identifiers(identifiers)) {
        return false;
    }
    let parts = core.split('.').collect::<Vec<_>>();
    if parts.len() != 3 || parts.iter().any(|part| !valid_semver_number(part)) {
        return false;
    }
    let major = parts[0].parse::<u64>().ok();
    let minor = parts[1].parse::<u64>().ok();
    major == Some(11) && matches!(minor, Some(10..=13))
}

fn valid_semver_number(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'))
}

fn valid_semver_identifiers(value: &str) -> bool {
    value.split('.').all(|identifier| {
        !identifier.is_empty()
            && identifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

pub fn validate_origin(raw: &str) -> Result<url::Url, String> {
    let parsed = url::Url::parse(raw).map_err(|error| error.to_string())?;
    let loopback = matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1"));
    if parsed.scheme() != "http"
        || !loopback
        || parsed.port().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return Err("daemon returned an unsafe CEREBRUM origin".into());
    }
    Ok(parsed)
}

trait ControlStream: Send {
    fn set_timeouts(&mut self, duration: Duration) -> std::io::Result<()>;
    fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()>;
    fn read_exact(&mut self, payload: &mut [u8]) -> std::io::Result<()>;
}

#[cfg(unix)]
impl ControlStream for std::os::unix::net::UnixStream {
    fn set_timeouts(&mut self, duration: Duration) -> std::io::Result<()> {
        self.set_read_timeout(Some(duration))?;
        self.set_write_timeout(Some(duration))
    }

    fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
        Write::write_all(self, payload)
    }

    fn read_exact(&mut self, payload: &mut [u8]) -> std::io::Result<()> {
        Read::read_exact(self, payload)
    }
}

#[cfg(unix)]
fn connect(sage_home: &std::path::Path) -> Result<Box<dyn ControlStream>, String> {
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::fs::PermissionsExt;

    let run_dir = sage_home.join("run");
    let run_meta = std::fs::symlink_metadata(&run_dir).map_err(|error| error.to_string())?;
    if run_meta.file_type().is_symlink()
        || !run_meta.is_dir()
        || run_meta.permissions().mode() & 0o077 != 0
    {
        return Err("unsafe SAGE control runtime directory".into());
    }
    let endpoint = run_dir.join("shell-control.sock");
    let meta = std::fs::symlink_metadata(&endpoint).map_err(|error| error.to_string())?;
    if meta.file_type().is_symlink()
        || !meta.file_type().is_socket()
        || meta.permissions().mode() & 0o077 != 0
    {
        return Err("unsafe SAGE control socket".into());
    }
    std::os::unix::net::UnixStream::connect(endpoint)
        .map(|stream| Box::new(stream) as Box<dyn ControlStream>)
        .map_err(|error| error.to_string())
}

#[cfg(windows)]
struct WindowsPipe {
    handle: std::os::windows::io::OwnedHandle,
    timeout: Duration,
}

#[cfg(windows)]
impl WindowsPipe {
    fn connect(path: &str, timeout: Duration) -> std::io::Result<Self> {
        use std::os::windows::io::FromRawHandle;
        use std::time::Instant;
        use windows_sys::Win32::Foundation::{
            ERROR_PIPE_BUSY, GENERIC_READ, GENERIC_WRITE, INVALID_HANDLE_VALUE,
        };
        use windows_sys::Win32::Storage::FileSystem::{
            CreateFileW, FILE_FLAG_OVERLAPPED, OPEN_EXISTING,
        };

        let wide: Vec<u16> = path.encode_utf16().chain(std::iter::once(0)).collect();
        let deadline = Instant::now() + timeout;
        loop {
            let handle = unsafe {
                CreateFileW(
                    wide.as_ptr(),
                    GENERIC_READ | GENERIC_WRITE,
                    0,
                    std::ptr::null(),
                    OPEN_EXISTING,
                    FILE_FLAG_OVERLAPPED,
                    std::ptr::null_mut(),
                )
            };
            if handle != INVALID_HANDLE_VALUE {
                // SAFETY: a successful CreateFileW call returns a newly owned
                // handle, and OwnedHandle becomes its sole closing owner.
                let handle = unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle) };
                return Ok(Self { handle, timeout });
            }
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_PIPE_BUSY as i32) {
                return Err(error);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out connecting to SAGE control pipe",
                ));
            }
            // CreateFileW is immediate for an available named-pipe instance and
            // returns ERROR_PIPE_BUSY otherwise. Keep the retry cancellable by
            // sleeping only a bounded remaining slice, never with an infinite
            // WaitNamedPipeW call.
            std::thread::sleep(remaining.min(Duration::from_millis(10)));
        }
    }

    fn transfer(
        &self,
        payload: &mut [u8],
        read: bool,
        timeout: Duration,
    ) -> std::io::Result<usize> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, WAIT_OBJECT_0, WAIT_TIMEOUT};
        use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
        use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
        use windows_sys::Win32::System::Threading::{CreateEventW, WaitForSingleObject};

        if payload.is_empty() {
            return Ok(0);
        }
        let handle = self.handle.as_raw_handle();
        let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
        if event.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        struct Event(windows_sys::Win32::Foundation::HANDLE);
        impl Drop for Event {
            fn drop(&mut self) {
                unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
            }
        }
        let event = Event(event);
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.hEvent = event.0;
        let started = unsafe {
            if read {
                ReadFile(
                    handle,
                    payload.as_mut_ptr(),
                    payload.len() as u32,
                    std::ptr::null_mut(),
                    &mut overlapped,
                )
            } else {
                WriteFile(
                    handle,
                    payload.as_ptr(),
                    payload.len() as u32,
                    std::ptr::null_mut(),
                    &mut overlapped,
                )
            }
        };
        let mut transferred = 0u32;
        if started == 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_IO_PENDING as i32) {
                return Err(error);
            }
            let wait = unsafe { WaitForSingleObject(event.0, timeout_millis(timeout)) };
            if wait == WAIT_TIMEOUT {
                // Do not drop the buffer while Windows may still own it. The
                // cancellation plus completion drain makes the deadline safe.
                unsafe {
                    let _ = CancelIoEx(handle, &overlapped);
                    let _ = GetOverlappedResult(handle, &overlapped, &mut transferred, 1);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "SAGE control pipe operation timed out",
                ));
            }
            if wait != WAIT_OBJECT_0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        if unsafe { GetOverlappedResult(handle, &overlapped, &mut transferred, 0) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(transferred as usize)
    }
}

#[cfg(windows)]
impl ControlStream for WindowsPipe {
    fn set_timeouts(&mut self, duration: Duration) -> std::io::Result<()> {
        self.timeout = duration;
        Ok(())
    }

    fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
        let deadline = std::time::Instant::now() + self.timeout;
        let mut offset = 0;
        while offset < payload.len() {
            let mut chunk = payload[offset..].to_vec();
            let transferred = self.transfer(&mut chunk, false, remaining_timeout(deadline)?)?;
            if transferred == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "SAGE control pipe wrote zero bytes",
                ));
            }
            offset += transferred;
        }
        Ok(())
    }

    fn read_exact(&mut self, payload: &mut [u8]) -> std::io::Result<()> {
        let deadline = std::time::Instant::now() + self.timeout;
        let mut offset = 0;
        while offset < payload.len() {
            let transferred =
                self.transfer(&mut payload[offset..], true, remaining_timeout(deadline)?)?;
            if transferred == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "SAGE control pipe closed mid-frame",
                ));
            }
            offset += transferred;
        }
        Ok(())
    }
}

#[cfg(windows)]
fn timeout_millis(duration: Duration) -> u32 {
    duration.as_millis().min(u128::from(u32::MAX)) as u32
}

#[cfg(windows)]
fn remaining_timeout(deadline: std::time::Instant) -> std::io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "SAGE control pipe operation timed out",
        ))
    } else {
        Ok(remaining)
    }
}

#[cfg(windows)]
fn connect(sage_home: &std::path::Path) -> Result<Box<dyn ControlStream>, String> {
    let sid = current_user_sid()?;
    let home = windows_home_identity(sage_home)?;
    let suffix = windows_pipe_suffix(&sid, &home);
    let path = format!(r"\\.\pipe\sage-shell-control-{suffix}");
    WindowsPipe::connect(&path, Duration::from_secs(1))
        .map(|pipe| Box::new(pipe) as Box<dyn ControlStream>)
        .map_err(|error| error.to_string())
}

#[cfg(windows)]
fn windows_home_identity(sage_home: &std::path::Path) -> Result<String, String> {
    use std::path::{Component, PathBuf};

    let absolute = if sage_home.is_absolute() {
        sage_home.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| error.to_string())?
            .join(sage_home)
    };
    // Match Go filepath.Abs followed by filepath.Clean without resolving
    // symlinks: discard '.', collapse '..' without crossing the absolute root,
    // and normalize separators before hashing.
    let mut clean = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = clean.pop();
            }
            component => clean.push(component.as_os_str()),
        }
    }
    let mut identity = clean.to_string_lossy().replace('/', r"\");
    if let Some(unc) = identity.strip_prefix(r"\\?\UNC\") {
        identity = format!(r"\\{unc}");
    } else if let Some(without_prefix) = identity.strip_prefix(r"\\?\") {
        identity = without_prefix.to_owned();
    }
    while identity.len() > 3 && identity.ends_with('\\') {
        identity.pop();
    }
    Ok(identity.to_lowercase())
}

#[cfg(any(windows, test))]
fn windows_pipe_suffix(sid: &str, canonical_home: &str) -> String {
    // Shared daemon/shell algorithm: SHA-256(UTF-8 SID || NUL || UTF-8
    // lower-case absolute Windows SAGE_HOME with '\\' separators and without a
    // leading "\\?\" prefix or trailing separator), first 8 bytes, lower hex.
    let mut hash = Sha256::new();
    hash.update(sid.as_bytes());
    hash.update([0]);
    hash.update(canonical_home.as_bytes());
    let digest = hash.finalize();
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(windows)]
fn current_user_sid() -> Result<String, String> {
    use std::{ptr, slice};
    use windows_sys::Win32::Foundation::{CloseHandle, LocalFree};
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    unsafe {
        let mut token = ptr::null_mut();
        if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
            return Err("cannot open process token".into());
        }
        let mut size = 0;
        GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut size);
        let mut buffer = vec![0u8; size as usize];
        if GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            size,
            &mut size,
        ) == 0
        {
            CloseHandle(token);
            return Err("cannot read process SID".into());
        }
        let user = &*(buffer.as_ptr().cast::<TOKEN_USER>());
        let mut wide = ptr::null_mut();
        if ConvertSidToStringSidW(user.User.Sid, &mut wide) == 0 {
            CloseHandle(token);
            return Err("cannot format process SID".into());
        }
        let mut len = 0usize;
        while *wide.add(len) != 0 {
            len += 1;
        }
        let sid = String::from_utf16(slice::from_raw_parts(wide, len))
            .map_err(|error| error.to_string())?;
        LocalFree(wide.cast());
        CloseHandle(token);
        Ok(sid)
    }
}

fn write_frame(stream: &mut dyn ControlStream, payload: &[u8]) -> Result<(), String> {
    if payload.is_empty() || payload.len() > MAX_FRAME {
        return Err("invalid request frame size".into());
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    stream.write_all(&frame).map_err(|error| error.to_string())
}

fn read_frame(stream: &mut dyn ControlStream) -> Result<Vec<u8>, String> {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .map_err(|error| error.to_string())?;
    let size = u32::from_be_bytes(header) as usize;
    if size == 0 || size > MAX_FRAME {
        return Err("invalid response frame size".into());
    }
    let mut payload = vec![0; size];
    stream
        .read_exact(&mut payload)
        .map_err(|error| error.to_string())?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_is_exact_loopback_http() {
        assert!(validate_origin("http://127.0.0.1:8080").is_ok());
        assert!(validate_origin("http://localhost:8080").is_ok());
        for unsafe_origin in [
            "https://127.0.0.1:8080",
            "http://127.0.0.1:8080/ui/",
            "http://127.0.0.1:8081?x=1",
            "http://192.168.1.2:8080",
        ] {
            assert!(validate_origin(unsafe_origin).is_err(), "{unsafe_origin}");
        }
    }

    #[test]
    fn incompatible_ready_status_can_retain_only_a_safe_browser_origin() {
        let status = Status {
            control_protocol: 99,
            daemon_version: "99.0.0".into(),
            api_schema: 99,
            min_shell_protocol: 99,
            max_shell_protocol: 99,
            instance_generation: "A".repeat(43),
            state: DaemonState::Ready,
            ui_origin: "http://127.0.0.1:8080".into(),
            startup_proof: String::new(),
        };
        assert_eq!(
            browser_fallback_origin(&status)
                .as_ref()
                .map(url::Url::as_str),
            Some("http://127.0.0.1:8080/")
        );

        let unsafe_status = Status {
            ui_origin: "http://example.com:8080".into(),
            ..status
        };
        assert!(browser_fallback_origin(&unsafe_status).is_none());
    }

    #[test]
    fn compatibility_fields_are_strict() {
        assert!(supported_daemon_version("v11.10.0").then_some(()).is_some());
        assert!(supported_daemon_version("11.11.3-rc.1+build.7"));
        assert!(supported_daemon_version("11.12.2"));
        assert!(supported_daemon_version("11.13.0"));
        assert!(!supported_daemon_version("11.14.0"));
        assert!(!supported_daemon_version("eleven"));
        assert!(valid_generation(&"A".repeat(43)));
        assert!(!valid_generation(&"B".repeat(43)));
        assert!(!valid_generation(&"A".repeat(42)));
    }

    #[test]
    fn startup_proof_is_a_canonical_sha256_hex_digest() {
        assert_eq!(
            startup_proof(&[0xA5; 32]),
            "fc8b64001c5fdd0f2f40fb67dae4a865a2c5bd17836676d6d5b58b7917e33717"
        );
        assert!(valid_startup_proof(
            "fc8b64001c5fdd0f2f40fb67dae4a865a2c5bd17836676d6d5b58b7917e33717"
        ));
        assert!(!valid_startup_proof("FC8B6400"));
    }

    #[test]
    fn windows_pipe_suffix_is_profile_scoped_and_stable() {
        assert_eq!(
            windows_pipe_suffix("S-1-5-21-123", r"c:\users\sage\.sage"),
            "e4ec5178983b20c1"
        );
        assert_ne!(
            windows_pipe_suffix("S-1-5-21-123", r"c:\users\sage\.sage"),
            windows_pipe_suffix("S-1-5-21-123", r"d:\fixture\sage-home")
        );
    }

    #[cfg(windows)]
    static TEST_PIPE_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    #[cfg(windows)]
    fn stalled_pipe_server(partial: &[u8]) -> (String, std::thread::JoinHandle<()>) {
        use std::sync::mpsc;
        use std::time::Duration;
        use windows_sys::Win32::Foundation::{
            CloseHandle, ERROR_PIPE_CONNECTED, INVALID_HANDLE_VALUE,
        };
        use windows_sys::Win32::Storage::FileSystem::{PIPE_ACCESS_DUPLEX, WriteFile};
        use windows_sys::Win32::System::Pipes::{
            ConnectNamedPipe, CreateNamedPipeW, DisconnectNamedPipe, PIPE_TYPE_BYTE, PIPE_WAIT,
        };

        let name = format!(
            r"\\.\pipe\sage-shell-control-test-{}-{}",
            std::process::id(),
            TEST_PIPE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let path: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        let partial = partial.to_vec();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let join = std::thread::spawn(move || {
            let pipe = unsafe {
                CreateNamedPipeW(
                    path.as_ptr(),
                    PIPE_ACCESS_DUPLEX,
                    PIPE_TYPE_BYTE | PIPE_WAIT,
                    1,
                    MAX_FRAME as u32,
                    1,
                    0,
                    std::ptr::null(),
                )
            };
            assert_ne!(pipe, INVALID_HANDLE_VALUE, "create stalled test pipe");
            ready_tx.send(()).expect("publish stalled test pipe");
            let connected = unsafe { ConnectNamedPipe(pipe, std::ptr::null_mut()) };
            if connected == 0 {
                let error = std::io::Error::last_os_error();
                assert_eq!(error.raw_os_error(), Some(ERROR_PIPE_CONNECTED as i32));
            }
            if !partial.is_empty() {
                let mut written = 0u32;
                assert_ne!(
                    unsafe {
                        WriteFile(
                            pipe,
                            partial.as_ptr(),
                            partial.len() as u32,
                            &mut written,
                            std::ptr::null_mut(),
                        )
                    },
                    0,
                    "write partial test frame"
                );
                assert_eq!(written as usize, partial.len());
            }
            std::thread::sleep(Duration::from_millis(250));
            unsafe {
                let _ = DisconnectNamedPipe(pipe);
                let _ = CloseHandle(pipe);
            }
        });
        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stalled test pipe must be ready");
        (name, join)
    }

    #[cfg(windows)]
    #[test]
    fn windows_pipe_deadline_cancels_a_stalled_frame() {
        let (name, server) = stalled_pipe_server(&[]);
        let mut pipe =
            WindowsPipe::connect(&name, Duration::from_secs(1)).expect("connect test pipe");
        pipe.set_timeouts(Duration::from_millis(50))
            .expect("set test deadline");
        let started = std::time::Instant::now();
        let mut frame = [0u8; 4];
        let error = pipe
            .read_exact(&mut frame)
            .expect_err("stalled frame must time out");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_millis(200));
        drop(pipe);
        server.join().expect("stalled server exits");
    }

    #[cfg(windows)]
    #[test]
    fn windows_pipe_deadline_cancels_a_stalled_write() {
        let (name, server) = stalled_pipe_server(&[]);
        let mut pipe =
            WindowsPipe::connect(&name, Duration::from_secs(1)).expect("connect test pipe");
        pipe.set_timeouts(Duration::from_millis(50))
            .expect("set test deadline");
        let started = std::time::Instant::now();
        let payload = vec![0xA5; MAX_FRAME];
        let error = pipe
            .write_all(&payload)
            .expect_err("unread pipe must time out a full frame write");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_millis(200));
        drop(pipe);
        server.join().expect("stalled server exits");
    }

    #[cfg(windows)]
    #[test]
    fn windows_pipe_deadline_cancels_a_partial_frame() {
        let (name, server) = stalled_pipe_server(&[0, 0]);
        let mut pipe =
            WindowsPipe::connect(&name, Duration::from_secs(1)).expect("connect test pipe");
        pipe.set_timeouts(Duration::from_millis(50))
            .expect("set test deadline");
        let started = std::time::Instant::now();
        let mut frame = [0u8; 4];
        let error = pipe
            .read_exact(&mut frame)
            .expect_err("partial frame must time out");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_millis(200));
        drop(pipe);
        server.join().expect("partial-frame server exits");
    }
}
