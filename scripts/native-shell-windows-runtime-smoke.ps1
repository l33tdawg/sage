param(
    [Parameter(Mandatory = $true)][string]$BundleRoot,
    [Parameter(Mandatory = $true)][string]$ExpectedVersion
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if (-not $IsWindows) { throw 'native-shell installed runtime smoke requires Windows' }

Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class SagePipeNative {
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool GetNamedPipeServerProcessId(IntPtr pipe, out uint serverProcessId);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    public static extern IntPtr CreateFileW(string name, uint access, uint share, IntPtr security, uint creation, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool CloseHandle(IntPtr handle);
}
'@

function Get-AuthenticodeStatusText([string]$Path) {
    # Diagnostic metadata only -- it must never be able to fail the harness.
    # Get-AuthenticodeSignature returns $null when the file is missing or is not a
    # supported type, and under `Set-StrictMode -Version Latest` the resulting
    # .Status read becomes a TERMINATING error: "The property 'Status' cannot be
    # found on this object." That killed an otherwise-green lifecycle run on
    # 2026-07-20 after every assertion had already passed.
    $sig = $null
    try { $sig = Get-AuthenticodeSignature -FilePath $Path -ErrorAction Stop } catch { return 'unavailable' }
    if ($null -eq $sig) { return 'unavailable' }
    if ($null -eq $sig.Status) { return 'unavailable' }
    return $sig.Status.ToString()
}

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Get-One($Items, [string]$Label) {
    $all = @($Items)
    if ($all.Count -ne 1) { throw "expected exactly one ${Label}, found $($all.Count)" }
    return $all[0]
}

function Get-FreePorts {
    $listeners = @()
    try {
        1..4 | ForEach-Object {
            $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
            $listener.Start()
            $listeners += $listener
        }
        return @($listeners | ForEach-Object { $_.LocalEndpoint.Port })
    } finally {
        $listeners | ForEach-Object { $_.Stop() }
    }
}

function Get-PipeNameForIdentity([string]$Sid, [string]$NodeDataRoot) {
    $canonicalRoot = [IO.Path]::GetFullPath($NodeDataRoot).Replace('/', '\').ToLowerInvariant()
    if ($canonicalRoot.StartsWith('\\?\UNC\', [StringComparison]::OrdinalIgnoreCase)) {
        $canonicalRoot = '\\' + $canonicalRoot.Substring(8)
    } elseif ($canonicalRoot.StartsWith('\\?\', [StringComparison]::OrdinalIgnoreCase)) {
        $canonicalRoot = $canonicalRoot.Substring(4)
    }
    $root = [IO.Path]::GetPathRoot($canonicalRoot)
    while ($canonicalRoot.Length -gt $root.Length -and $canonicalRoot.EndsWith('\')) {
        $canonicalRoot = $canonicalRoot.Substring(0, $canonicalRoot.Length - 1)
    }
    $bytes = [Text.Encoding]::UTF8.GetBytes($Sid + [char]0 + $canonicalRoot)
    $hash = [Security.Cryptography.SHA256]::HashData($bytes)
    $suffix = -join ($hash[0..7] | ForEach-Object { $_.ToString('x2') })
    return "sage-shell-control-$suffix"
}

function Get-PipeName([string]$NodeDataRoot) {
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    return Get-PipeNameForIdentity $sid $NodeDataRoot
}

function Read-Exact($Stream, [int]$Count, [Threading.CancellationToken]$Token) {
    $buffer = [byte[]]::new($Count)
    $offset = 0
    while ($offset -lt $Count) {
        $read = $Stream.ReadAsync($buffer, $offset, $Count - $offset, $Token).GetAwaiter().GetResult()
        if ($read -eq 0) { throw 'SSCP connection closed before the frame completed' }
        $offset += $read
    }
    return $buffer
}

function Read-ReadyStatus([string]$PipeName, [string]$ExpectedOrigin, [string]$ExpectedDaemon, [bool]$RequireStartupProof = $true, [int]$TimeoutSeconds = 60) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = $null
    while ([DateTime]::UtcNow -lt $deadline) {
        $pipe = $null
        $cancel = $null
        try {
            $pipe = [IO.Pipes.NamedPipeClientStream]::new('.', $PipeName, [IO.Pipes.PipeDirection]::InOut, [IO.Pipes.PipeOptions]::Asynchronous)
            $pipe.Connect(1000)
            $cancel = [Threading.CancellationTokenSource]::new(2000)
            $request = [Text.Encoding]::UTF8.GetBytes('{"control_protocol":1,"shell_protocol":1,"operation":"status"}')
            $header = [BitConverter]::GetBytes([uint32]$request.Length)
            if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($header) }
            $frame = [byte[]]::new($header.Length + $request.Length)
            [Array]::Copy($header, 0, $frame, 0, $header.Length)
            [Array]::Copy($request, 0, $frame, $header.Length, $request.Length)
            $pipe.WriteAsync($frame, 0, $frame.Length, $cancel.Token).GetAwaiter().GetResult()
            $pipe.Flush()

            $sizeBytes = Read-Exact $pipe 4 $cancel.Token
            if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($sizeBytes) }
            $size = [BitConverter]::ToUInt32($sizeBytes, 0)
            Assert-True ($size -gt 0 -and $size -le 16384) "invalid SSCP response size: $size"
            $payload = Read-Exact $pipe ([int]$size) $cancel.Token
            $json = [Text.UTF8Encoding]::new($false, $true).GetString($payload)
            $status = $json | ConvertFrom-Json

            $expectedFields = @('api_schema','control_protocol','daemon_version','instance_generation','max_shell_protocol','min_shell_protocol','state','ui_origin')
            if ($RequireStartupProof) { $expectedFields += 'startup_proof' }
            $expectedFields = @($expectedFields | Sort-Object)
            $actualFields = @($status.PSObject.Properties.Name | Sort-Object)
            Assert-True (($actualFields -join ',') -ceq ($expectedFields -join ',')) 'SSCP status schema is not exact'
            Assert-True ($status.control_protocol -is [long] -or $status.control_protocol -is [int]) 'SSCP control protocol has the wrong type'
            Assert-True ($status.api_schema -is [long] -or $status.api_schema -is [int]) 'SSCP API schema has the wrong type'
            Assert-True ($status.control_protocol -eq 1 -and $status.api_schema -eq 1) 'SSCP control/API protocol mismatch'
            Assert-True (($status.min_shell_protocol -is [long] -or $status.min_shell_protocol -is [int]) -and ($status.max_shell_protocol -is [long] -or $status.max_shell_protocol -is [int])) 'SSCP shell protocol range has the wrong type'
            Assert-True ($status.min_shell_protocol -le 1 -and $status.max_shell_protocol -ge 1 -and $status.min_shell_protocol -le $status.max_shell_protocol) 'SSCP shell protocol is incompatible'
            Assert-True ($status.daemon_version -is [string]) 'SSCP daemon version has the wrong type'
            Assert-True ($status.daemon_version -ceq $ExpectedDaemon) "unexpected SSCP daemon version: $($status.daemon_version)"
            Assert-True ($status.instance_generation -is [string]) 'SSCP generation has the wrong type'
            Assert-True ($status.instance_generation -cmatch '^[A-Za-z0-9_-]{43}$') 'SSCP generation is malformed'
            Assert-True ('AEIMQUYcgkosw048'.Contains($status.instance_generation[42])) 'SSCP generation is not canonical base64url'
            Assert-True ($status.state -is [string]) 'SSCP daemon state has the wrong type'
            Assert-True (@('ready','degraded') -ccontains $status.state) "SSCP daemon is not renderable: $($status.state)"
            Assert-True ($status.ui_origin -is [string]) 'SSCP UI origin has the wrong type'
            Assert-True ($status.ui_origin -ceq $ExpectedOrigin) "unexpected SSCP UI origin: $($status.ui_origin)"
            if ($RequireStartupProof) {
                Assert-True ($status.startup_proof -is [string]) 'SSCP startup proof has the wrong type'
                Assert-True ($status.startup_proof -cmatch '^[0-9a-f]{64}$') 'SSCP startup proof is missing or malformed'
            }

            [uint32]$serverPid = 0
            $gotPid = [SagePipeNative]::GetNamedPipeServerProcessId($pipe.SafePipeHandle.DangerousGetHandle(), [ref]$serverPid)
            Assert-True $gotPid "GetNamedPipeServerProcessId failed: $([Runtime.InteropServices.Marshal]::GetLastWin32Error())"
            return [pscustomobject]@{ Status = $status; ServerPid = [int]$serverPid }
        } catch {
            $lastError = $_
            Start-Sleep -Milliseconds 150
        } finally {
            if ($cancel) { $cancel.Dispose() }
            if ($pipe) { $pipe.Dispose() }
        }
    }
    throw "timed out waiting for renderable verified SSCP status: $lastError"
}

function Get-ExecutablePath([int]$ProcessId) {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId"
    if (-not $process -or -not $process.ExecutablePath) { throw "cannot resolve executable path for PID $ProcessId" }
    return [IO.Path]::GetFullPath($process.ExecutablePath)
}

function Assert-ServerPath($Result, [string]$ExpectedPath) {
    $actual = Get-ExecutablePath $Result.ServerPid
    Assert-True ($actual.Equals([IO.Path]::GetFullPath($ExpectedPath), [StringComparison]::OrdinalIgnoreCase)) "SSCP server path mismatch: $actual"
}

function Get-ExactProcessHandle([int]$ProcessId, [string]$ExpectedPath, [string]$Label) {
    $process = [Diagnostics.Process]::GetProcessById($ProcessId)
    try {
        [void]$process.Handle
        $actual = [IO.Path]::GetFullPath($process.MainModule.FileName)
        Assert-True ($actual.Equals([IO.Path]::GetFullPath($ExpectedPath), [StringComparison]::OrdinalIgnoreCase)) "${Label} path mismatch: $actual"
        return $process
    } catch {
        $process.Dispose()
        throw
    }
}

function Start-Shell([string]$ShellPath, [string]$NodeDataRoot, [int[]]$Ports) {
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $ShellPath
    $start.UseShellExecute = $false
    $start.Environment['SAGE_HOME'] = $NodeDataRoot
    $start.Environment['SAGE_NO_BROWSER'] = '1'
    $start.Environment['REST_ADDR'] = "127.0.0.1:$($Ports[0])"
    $start.Environment['SAGE_CMT_RPC_ADDR'] = "tcp://127.0.0.1:$($Ports[1])"
    $start.Environment['SAGE_CMT_P2P_ADDR'] = "tcp://127.0.0.1:$($Ports[2])"
    return [Diagnostics.Process]::Start($start)
}

function Start-Daemon([string]$DaemonPath, [string]$NodeDataRoot, [int[]]$Ports) {
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $DaemonPath
    $start.ArgumentList.Add('serve')
    $start.UseShellExecute = $false
    $start.Environment['SAGE_HOME'] = $NodeDataRoot
    $start.Environment['SAGE_NO_BROWSER'] = '1'
    $start.Environment['REST_ADDR'] = "127.0.0.1:$($Ports[0])"
    $start.Environment['SAGE_CMT_RPC_ADDR'] = "tcp://127.0.0.1:$($Ports[1])"
    $start.Environment['SAGE_CMT_P2P_ADDR'] = "tcp://127.0.0.1:$($Ports[2])"
    return [Diagnostics.Process]::Start($start)
}

function Stop-ExactTree([int]$ProcessId, [string]$ExpectedPath) {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
    if (-not $process) { return }
    Assert-True ($process.ExecutablePath -and [IO.Path]::GetFullPath($process.ExecutablePath).Equals([IO.Path]::GetFullPath($ExpectedPath), [StringComparison]::OrdinalIgnoreCase)) "refusing to stop unexpected PID $ProcessId"
    & taskkill.exe /PID $ProcessId /T /F | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "taskkill failed for verified PID $ProcessId" }
}

function Stop-LaunchedTree([Diagnostics.Process]$Process, [string]$ExpectedPath, [string]$Label) {
    Assert-True (-not $Process.HasExited) "${Label} exited before its explicit stop"
    $actual = [IO.Path]::GetFullPath($Process.MainModule.FileName)
    Assert-True ($actual.Equals([IO.Path]::GetFullPath($ExpectedPath), [StringComparison]::OrdinalIgnoreCase)) "refusing to stop ${Label} from unexpected path: $actual"
    $Process.Kill($true)
    Assert-True ($Process.WaitForExit(10000)) "${Label} tree did not exit after its explicit stop"
}

function Stop-AllExactPath([string]$ExpectedPath) {
    if (-not $ExpectedPath) { return }
    $fullExpected = [IO.Path]::GetFullPath($ExpectedPath)
    Get-CimInstance Win32_Process | Where-Object {
        $_.ExecutablePath -and [IO.Path]::GetFullPath($_.ExecutablePath).Equals($fullExpected, [StringComparison]::OrdinalIgnoreCase)
    } | ForEach-Object { Stop-ExactTree ([int]$_.ProcessId) $fullExpected }
}

function Wait-PipeGone([string]$PipeName) {
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    $path = "\\.\pipe\$PipeName"
    $genericReadWrite = [uint32]3221225472
    $openExisting = [uint32]3
    $overlapped = [uint32]1073741824
    while ([DateTime]::UtcNow -lt $deadline) {
        $handle = [SagePipeNative]::CreateFileW(
            $path,
            $genericReadWrite,
            [uint32]0,
            [IntPtr]::Zero,
            $openExisting,
            $overlapped,
            [IntPtr]::Zero
        )
        if ($handle -ne [IntPtr](-1)) {
            [void][SagePipeNative]::CloseHandle($handle)
        } else {
            $errorCode = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
            if ($errorCode -eq 2) { return }
            if ($errorCode -notin @(121, 231)) { throw "unexpected error probing SSCP pipe disappearance: $errorCode" }
        }
        Start-Sleep -Milliseconds 100
    }
    throw "SSCP pipe remained reachable after verified process cleanup: $PipeName"
}

function Invoke-Installer([string]$Path, [string[]]$Arguments) {
    $process = Start-Process -FilePath $Path -ArgumentList $Arguments -Wait -PassThru
    if ($process.ExitCode -ne 0) { throw "installer exited with status $($process.ExitCode): $Path" }
}

function Get-WebViewVersion {
    $roots = @(
        'HKLM:\SOFTWARE\Microsoft\EdgeUpdate\Clients',
        'HKLM:\SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients',
        'HKCU:\SOFTWARE\Microsoft\EdgeUpdate\Clients',
        'HKCU:\SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients'
    )
    $versions = @($roots | ForEach-Object {
        Get-ChildItem $_ -ErrorAction SilentlyContinue |
            ForEach-Object { Get-ItemProperty $_.PSPath -ErrorAction SilentlyContinue } |
            Where-Object { $_.name -like '*WebView*' -and $_.pv } |
            ForEach-Object { [string]$_.pv }
    } | Sort-Object -Unique)
    return ($versions -join ',')
}

$setup = Get-One (Get-ChildItem -LiteralPath (Join-Path $BundleRoot 'nsis') -File | Where-Object Name -Match '(?i)setup.*\.exe$') 'NSIS installer'
$smokeRoot = Join-Path $env:RUNNER_TEMP ("sage-native-shell-windows-runtime.{0}" -f [Guid]::NewGuid().ToString('N'))
$installRoot = Join-Path $smokeRoot 'install'
$nodeDataRoot = Join-Path $smokeRoot 'home'
$profileBHome = Join-Path $smokeRoot 'profile-b'
$diagnostics = Join-Path $smokeRoot 'diagnostics'
New-Item -ItemType Directory -Path $nodeDataRoot, $profileBHome, $diagnostics | Out-Null
Set-Content -LiteralPath (Join-Path $nodeDataRoot 'preserve.sentinel') -Value 'native-shell-uninstall-preservation' -NoNewline
Assert-True (-not (Test-Path -LiteralPath $installRoot)) "install root already exists: $installRoot"

$productKey = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\SAGE Native Preview'
$machineKeys = @(
    'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\SAGE Native Preview',
    'HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\SAGE Native Preview'
)
Assert-True (-not (Test-Path $productKey)) 'refusing to replace a pre-existing SAGE Native Preview installation'
foreach ($key in $machineKeys) { Assert-True (-not (Test-Path $key)) "unexpected machine-wide native-shell installation: $key" }
Assert-True ((Get-PipeNameForIdentity 'S-1-5-21-123' 'C:\Users\SAGE\.sage\') -ceq 'sage-shell-control-e4ec5178983b20c1') 'Windows pipe derivation broke the published Go/Rust vector'

$os = Get-CimInstance Win32_OperatingSystem
$webviewVersion = Get-WebViewVersion
[ordered]@{
    expected_version = $ExpectedVersion
    installer_path = $setup.FullName
    installer_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $setup.FullName).Hash.ToLowerInvariant()
    authenticode = Get-AuthenticodeStatusText $setup.FullName
    image_os = $env:ImageOS
    image_version = $env:ImageVersion
    os_caption = $os.Caption
    os_version = $os.Version
    os_architecture = $os.OSArchitecture
    webview2_version_preinstall = $webviewVersion
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $diagnostics 'preflight.json')

$installed = $false
$installAttempted = $false
$shellProcesses = [Collections.Generic.List[Diagnostics.Process]]::new()
$daemonProcesses = [Collections.Generic.List[Diagnostics.Process]]::new()
$serverPids = [Collections.Generic.List[int]]::new()
$daemonPath = $null
$uninstaller = $null
try {
    $installAttempted = $true
    Invoke-Installer $setup.FullName @('/S', "/D=$installRoot")
    $installed = $true
    Assert-True (Test-Path -LiteralPath $installRoot -PathType Container) 'NSIS installer did not create the requested install root'
    Assert-True (Test-Path $productKey) 'NSIS installer did not create its current-user uninstall record'
    foreach ($key in $machineKeys) { Assert-True (-not (Test-Path $key)) "NSIS unexpectedly created a machine-wide uninstall record: $key" }
    $product = Get-ItemProperty $productKey
    $recordedRoot = ([string]$product.InstallLocation).Trim('"').TrimEnd('\')
    Assert-True ([IO.Path]::GetFullPath($recordedRoot).Equals([IO.Path]::GetFullPath($installRoot).TrimEnd('\'), [StringComparison]::OrdinalIgnoreCase)) "NSIS InstallLocation mismatch: $recordedRoot"
    Assert-True ([string]$product.MainBinaryName -match '\.exe$') 'NSIS uninstall record is missing MainBinaryName'
    $uninstaller = Get-One (Get-ChildItem -LiteralPath $recordedRoot -File -Filter '*.exe' | Where-Object Name -Match '(?i)^uninstall.*\.exe$') 'NSIS uninstaller'
    $shellExe = Get-One (Get-ChildItem -LiteralPath $recordedRoot -File -Filter '*.exe' | Where-Object Name -NotMatch '(?i)^uninstall.*\.exe$') 'installed native shell executable'
    Assert-True ($shellExe.Name -ceq [string]$product.MainBinaryName) 'installed shell does not match NSIS MainBinaryName'
    $webviewVersion = Get-WebViewVersion
    Assert-True (-not [string]::IsNullOrWhiteSpace($webviewVersion)) 'installed Windows image has no identifiable WebView2 runtime version'
    @{ webview2_version = $webviewVersion } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $diagnostics 'webview2.json')
    $daemonPath = (Get-One (Get-ChildItem -LiteralPath $installRoot -Recurse -File -Filter 'sage-gui.exe' | Where-Object FullName -Match '(?i)[\\/]binaries[\\/]sage-gui\.exe$') 'installed bundled daemon').FullName
    $ports = @(Get-FreePorts)
    $origin = "http://127.0.0.1:$($ports[0])"
    $pipeName = Get-PipeName $nodeDataRoot

    $first = Start-Shell $shellExe.FullName $nodeDataRoot $ports
    $shellProcesses.Add($first)
    $firstResult = Read-ReadyStatus $pipeName $origin $ExpectedVersion
    Assert-True (-not $first.HasExited) 'installed native shell exited before attachment evidence'
    Assert-ServerPath $firstResult $daemonPath
    $firstDaemon = Get-ExactProcessHandle $firstResult.ServerPid $daemonPath 'installed bundled daemon'
    $daemonProcesses.Add($firstDaemon)
    $serverPids.Add($firstResult.ServerPid)
    $firstGeneration = $firstResult.Status.instance_generation

    $profileBPipe = Get-PipeName $profileBHome
    Assert-True ($profileBPipe -cne $pipeName) 'distinct SAGE_HOME profiles derived the same Windows control pipe'
    Wait-PipeGone $profileBPipe
    $profileBPorts = @(Get-FreePorts)
    $profileBOrigin = "http://127.0.0.1:$($profileBPorts[0])"
    Set-Content -LiteralPath (Join-Path $profileBHome 'config.yaml') -Value @(
        'quorum:'
        "  tls_addr: `"127.0.0.1:$($profileBPorts[3])`""
    )
    $profileBDaemon = Start-Daemon $daemonPath $profileBHome $profileBPorts
    $daemonProcesses.Add($profileBDaemon)
    $profileBResult = Read-ReadyStatus $profileBPipe $profileBOrigin $ExpectedVersion $false
    Assert-True ($profileBResult.ServerPid -eq $profileBDaemon.Id) 'profile-B SSCP server PID did not match the directly launched daemon'
    Assert-ServerPath $profileBResult $daemonPath
    Assert-True ($profileBResult.Status.instance_generation -cne $firstGeneration) 'distinct profiles reused one daemon generation'
    $serverPids.Add($profileBResult.ServerPid)

    $second = Start-Shell $shellExe.FullName $nodeDataRoot $ports
    $shellProcesses.Add($second)
    Assert-True ($second.WaitForExit(10000)) 'second native-shell launch did not hand off to the existing instance'
    Assert-True ($second.ExitCode -eq 0) "second native-shell launch exited with status $($second.ExitCode)"
    $secondResult = Read-ReadyStatus $pipeName $origin $ExpectedVersion
    Assert-True ($secondResult.Status.instance_generation -ceq $firstGeneration) 'second launch changed the daemon generation'
    Assert-True ($secondResult.ServerPid -eq $firstResult.ServerPid) 'second launch changed the daemon PID'
    Assert-True (-not $first.HasExited) 'primary native shell exited during second-instance handoff'

    Assert-True ($first.CloseMainWindow()) 'installed shell did not expose a closeable main window'
    Assert-True ($first.WaitForExit(10000)) 'installed shell did not exit after normal window close'
    $daemonOnly = Read-ReadyStatus $pipeName $origin $ExpectedVersion
    Assert-True ($daemonOnly.Status.instance_generation -ceq $firstGeneration) 'window close changed the daemon generation'
    Assert-True ($daemonOnly.ServerPid -eq $firstResult.ServerPid) 'window close changed the daemon PID'
    Assert-ServerPath $daemonOnly $daemonPath

    $safeStatus = [ordered]@{
        installer_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $setup.FullName).Hash.ToLowerInvariant()
        authenticode = Get-AuthenticodeStatusText $setup.FullName
        daemon_version = $daemonOnly.Status.daemon_version
        state = $daemonOnly.Status.state
        ui_origin = $daemonOnly.Status.ui_origin
        instance_generation = $firstGeneration
        runner = $env:ImageOS
    }
    $safeStatus | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $diagnostics 'first-install.json')

    Stop-LaunchedTree $profileBDaemon $daemonPath 'profile-B daemon'
    Wait-PipeGone $profileBPipe
    Stop-LaunchedTree $firstDaemon $daemonPath 'installed bundled daemon'
    Wait-PipeGone $pipeName
    Invoke-Installer $uninstaller.FullName @('/S')
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    while ((Test-Path -LiteralPath $installRoot) -and [DateTime]::UtcNow -lt $deadline) { Start-Sleep -Milliseconds 200 }
    Assert-True (-not (Test-Path -LiteralPath $installRoot)) 'NSIS uninstall left the install root behind'
    Assert-True (-not (Test-Path $productKey)) 'NSIS uninstall left its current-user product record behind'
    $installed = $false
    Assert-True ((Get-Content -Raw -LiteralPath (Join-Path $nodeDataRoot 'preserve.sentinel')) -ceq 'native-shell-uninstall-preservation') 'NSIS uninstall modified SAGE_HOME'

    $installAttempted = $true
    Invoke-Installer $setup.FullName @('/S', "/D=$installRoot")
    $installed = $true
    Assert-True (Test-Path $productKey) 'NSIS reinstall did not restore its current-user uninstall record'
    $uninstaller = Get-One (Get-ChildItem -LiteralPath $installRoot -File -Filter '*.exe' | Where-Object Name -Match '(?i)^uninstall.*\.exe$') 'reinstalled NSIS uninstaller'
    $shellExe = Get-One (Get-ChildItem -LiteralPath $installRoot -File -Filter '*.exe' | Where-Object Name -NotMatch '(?i)^uninstall.*\.exe$') 'reinstalled native shell executable'
    $daemonPath = (Get-One (Get-ChildItem -LiteralPath $installRoot -Recurse -File -Filter 'sage-gui.exe' | Where-Object FullName -Match '(?i)[\\/]binaries[\\/]sage-gui\.exe$') 'reinstalled bundled daemon').FullName
    $reinstalled = Start-Shell $shellExe.FullName $nodeDataRoot $ports
    $shellProcesses.Add($reinstalled)
    $reinstallResult = Read-ReadyStatus $pipeName $origin $ExpectedVersion
    Assert-True (-not $reinstalled.HasExited) 'reinstalled native shell exited before attachment evidence'
    Assert-ServerPath $reinstallResult $daemonPath
    $reinstallDaemon = Get-ExactProcessHandle $reinstallResult.ServerPid $daemonPath 'reinstalled bundled daemon'
    $daemonProcesses.Add($reinstallDaemon)
    $serverPids.Add($reinstallResult.ServerPid)
    Assert-True ($reinstallResult.Status.instance_generation -cne $firstGeneration) 'reinstall reused a stale daemon generation'
    Assert-True ((Get-Content -Raw -LiteralPath (Join-Path $nodeDataRoot 'preserve.sentinel')) -ceq 'native-shell-uninstall-preservation') 'reinstall modified SAGE_HOME'

    Assert-True ($reinstalled.CloseMainWindow()) 'reinstalled native shell did not expose a closeable main window'
    Assert-True ($reinstalled.WaitForExit(10000)) 'reinstalled native shell did not exit after normal window close'
    Assert-True (-not $reinstallDaemon.HasExited) 'reinstalled bundled daemon did not survive normal shell close'
    Stop-LaunchedTree $reinstallDaemon $daemonPath 'reinstalled bundled daemon'
    Wait-PipeGone $pipeName
    Invoke-Installer $uninstaller.FullName @('/S')
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    while ((Test-Path -LiteralPath $installRoot) -and [DateTime]::UtcNow -lt $deadline) { Start-Sleep -Milliseconds 200 }
    Assert-True (-not (Test-Path -LiteralPath $installRoot)) 'final NSIS uninstall left the install root behind'
    Assert-True (-not (Test-Path $productKey)) 'final NSIS uninstall left its current-user product record behind'
    $installed = $false
    Assert-True ((Get-Content -Raw -LiteralPath (Join-Path $nodeDataRoot 'preserve.sentinel')) -ceq 'native-shell-uninstall-preservation') 'final NSIS uninstall modified SAGE_HOME'

    Write-Output "native-shell Windows installed runtime smoke passed first=$firstGeneration reinstall=$($reinstallResult.Status.instance_generation)"
} finally {
    $daemonLog = Join-Path $nodeDataRoot 'logs\sage.log'
    if (Test-Path -LiteralPath $daemonLog) {
        Get-Content -LiteralPath $daemonLog -Tail 400 | Set-Content -LiteralPath (Join-Path $diagnostics 'daemon-tail.log')
    }
    foreach ($process in $shellProcesses) {
        try {
            if (-not $process.HasExited) { Stop-LaunchedTree $process $process.StartInfo.FileName 'native shell' }
        } catch { Write-Warning $_ }
    }
    try { Stop-AllExactPath $daemonPath } catch { Write-Warning $_ }
    if ($daemonPath) {
        foreach ($serverPid in $serverPids) {
            try { Stop-ExactTree $serverPid $daemonPath } catch { Write-Warning $_ }
        }
    }
    foreach ($process in $daemonProcesses) { $process.Dispose() }
    if (($installed -or $installAttempted) -and (Test-Path -LiteralPath $installRoot)) {
        try {
            $cleanupUninstaller = Get-One (Get-ChildItem -LiteralPath $installRoot -File -Filter '*.exe' | Where-Object Name -Match '(?i)^uninstall.*\.exe$') 'cleanup NSIS uninstaller'
            Invoke-Installer $cleanupUninstaller.FullName @('/S')
        } catch { Write-Warning "partial native-shell installation requires runner cleanup: $_" }
    }
    Write-Output "native-shell Windows runtime smoke evidence: $diagnostics"
}
