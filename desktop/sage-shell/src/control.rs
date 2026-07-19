use serde::Deserialize;
#[cfg(any(windows, test))]
use sha2::{Digest, Sha256};
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
}

#[derive(Debug)]
pub enum StatusError {
    Unavailable(String),
    Incompatible(String),
}

impl StatusError {
    pub fn is_incompatible(&self) -> bool {
        matches!(self, Self::Incompatible(_))
    }
}

impl std::fmt::Display for StatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Unavailable(message) | Self::Incompatible(message) => message,
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
    let status: Status = serde_json::from_slice(&response)
        .map_err(|error| StatusError::Incompatible(error.to_string()))?;
    validate(&status).map_err(StatusError::Incompatible)?;
    Ok(status)
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
    Ok(())
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
    major == Some(11) && matches!(minor, Some(10 | 11))
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

trait ControlStream: Read + Write + Send {
    fn set_timeouts(&self, duration: Duration) -> std::io::Result<()>;
}

#[cfg(unix)]
impl ControlStream for std::os::unix::net::UnixStream {
    fn set_timeouts(&self, duration: Duration) -> std::io::Result<()> {
        self.set_read_timeout(Some(duration))?;
        self.set_write_timeout(Some(duration))
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
impl ControlStream for std::fs::File {
    fn set_timeouts(&self, _duration: Duration) -> std::io::Result<()> {
        // std::fs::File does not expose a cancellable named-pipe deadline.
        // Windows runtime promotion remains blocked on the native timeout test
        // in native-shell-quality-gates.md; the UI thread itself never blocks.
        Ok(())
    }
}

#[cfg(windows)]
fn connect(_sage_home: &std::path::Path) -> Result<Box<dyn ControlStream>, String> {
    let sid = current_user_sid()?;
    let home = windows_home_identity(_sage_home)?;
    let suffix = windows_pipe_suffix(&sid, &home);
    let path = format!(r"\\.\pipe\sage-shell-control-{suffix}");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map(|file| Box::new(file) as Box<dyn ControlStream>)
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
    use windows_sys::Win32::Security::{
        ConvertSidToStringSidW, GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
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
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(|error| error.to_string())?;
    stream.write_all(payload).map_err(|error| error.to_string())
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
    fn compatibility_fields_are_strict() {
        assert!(supported_daemon_version("v11.10.0").then_some(()).is_some());
        assert!(supported_daemon_version("11.11.3-rc.1+build.7"));
        assert!(!supported_daemon_version("11.12.0"));
        assert!(!supported_daemon_version("eleven"));
        assert!(valid_generation(&"A".repeat(43)));
        assert!(!valid_generation(&"B".repeat(43)));
        assert!(!valid_generation(&"A".repeat(42)));
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
}
