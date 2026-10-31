import Cocoa
import Darwin

// SAGE Dock App — launches the SAGE server and manages it from the dock.
// Double-click: starts SAGE, opens CEREBRUM dashboard
// Right-click dock icon: Open CEREBRUM, Show Log, Quit

class SAGEApp: NSObject, NSApplicationDelegate {
    var serverProcess: Process?
    var vaultPassphrase: String?
    private let dashboardOpenQueue = DispatchQueue(label: "com.sage.dashboard-open", qos: .userInitiated)
    private let serverLogQueue = DispatchQueue(label: "com.sage.server-log", qos: .utility)
    private var serverLogHandle: FileHandle?
    private var serverStartedAt: Date?
    private var healthTimer: Timer?
    private var healthProbeInFlight = false
    private var unhealthySince: Date?
    private var pendingServerRestart: DispatchWorkItem?
    private var stableServerReset: DispatchWorkItem?
    private var consecutiveServerFailures = 0
    private var isTerminating = false

    private let healthCheckInterval: TimeInterval = 5
    private let serverStartupGrace: TimeInterval = 45
    private let unhealthyRestartGrace: TimeInterval = 90
    private let terminationGrace: TimeInterval = 45
    private let maximumAutomaticRestarts = 5
    private let restartDelays: [TimeInterval] = [1, 2, 4, 8, 16, 30]

    let sageHome: String = {
        ProcessInfo.processInfo.environment["SAGE_HOME"] ?? NSHomeDirectory() + "/.sage"
    }()
    var logPath: String { sageHome + "/sage.log" }
    var sageBinary: String {
        let myDir = (Bundle.main.executablePath! as NSString).deletingLastPathComponent
        let sibling = myDir + "/sage-gui"
        if FileManager.default.fileExists(atPath: sibling) { return sibling }
        return "/usr/local/bin/sage-gui"
    }
    var vaultKeyPath: String { sageHome + "/vault.key" }

    func applicationDidFinishLaunching(_ notification: Notification) {
        // If vault.key exists, prompt for passphrase before starting the server.
        // This ensures encryption is unlocked at startup, not deferred to the web UI.
        if FileManager.default.fileExists(atPath: vaultKeyPath) {
            vaultPassphrase = promptPassphrase()
            // If user cancels, server starts locked — web UI shows lock screen
        }

        startServer()
        startHealthMonitor()

        // Open dashboard after server has a moment to start
        DispatchQueue.main.asyncAfter(deadline: .now() + 3) {
            // Browser tabs outlive app restarts and upgrades. Run the same
            // reuse/presence checks on initial launch as on later dock clicks.
            self.openDashboard()
        }
    }

    // Clicking dock icon when app is already running → open dashboard
    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        openDashboard()
        return false
    }

    // Right-click dock menu
    @objc func applicationDockMenu(_ sender: NSApplication) -> NSMenu? {
        let menu = NSMenu()

        let dashItem = NSMenuItem(title: "Open CEREBRUM", action: #selector(openDashboard), keyEquivalent: "")
        dashItem.target = self
        menu.addItem(dashItem)

        menu.addItem(NSMenuItem.separator())

        let logItem = NSMenuItem(title: "Show Log", action: #selector(showLog), keyEquivalent: "")
        logItem.target = self
        menu.addItem(logItem)

        return menu
    }

    func applicationWillTerminate(_ notification: Notification) {
        isTerminating = true
        pendingServerRestart?.cancel()
        pendingServerRestart = nil
        stableServerReset?.cancel()
        stableServerReset = nil
        healthTimer?.invalidate()
        healthTimer = nil
        vaultPassphrase = nil
        serverProcess?.terminate()
    }

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        isTerminating = true
        pendingServerRestart?.cancel()
        pendingServerRestart = nil
        guard let process = serverProcess, process.isRunning else {
            return .terminateNow
        }

        appendServerLog("application quitting; asking daemon pid \(process.processIdentifier) to stop")
        process.terminate()
        let pid = process.processIdentifier
        let grace = terminationGrace
        DispatchQueue.global(qos: .utility).async { [weak self] in
            let deadline = Date().addingTimeInterval(grace)
            while process.isRunning && Date() < deadline {
                Thread.sleep(forTimeInterval: 0.1)
            }
            if process.isRunning {
                self?.appendServerLog(
                    "daemon pid \(pid) did not stop within \(Int(grace))s; sending SIGKILL"
                )
                kill(pid, SIGKILL)
            }
            DispatchQueue.main.async {
                sender.reply(toApplicationShouldTerminate: true)
            }
        }
        return .terminateLater
    }

    // MARK: - Server Management

    func startServer() {
        pendingServerRestart?.cancel()
        pendingServerRestart = nil
        stableServerReset?.cancel()
        stableServerReset = nil

        let process = Process()
        process.executableURL = URL(fileURLWithPath: sageBinary)
        process.arguments = ["serve"]

        var env = ProcessInfo.processInfo.environment
        env["SAGE_NO_BROWSER"] = "1"
        if let passphrase = vaultPassphrase {
            env["SAGE_PASSPHRASE"] = passphrase
        }
        process.environment = env
        process.terminationHandler = { [weak self] terminated in
            DispatchQueue.main.async {
                self?.serverDidExit(terminated)
            }
        }

        if let logHandle = openAppendLogHandle() {
            process.standardOutput = logHandle
            process.standardError = logHandle
            serverLogHandle = logHandle
        }

        do {
            appendServerLog("starting daemon \(sageBinary)")
            try process.run()
            serverProcess = process
            serverStartedAt = Date()
            unhealthySince = nil
            scheduleStableServerReset(for: process)
        } catch {
            NSLog("Failed to start SAGE server: \(error)")
            appendServerLog("failed to start daemon: \(error)")
            serverProcess = nil
            serverStartedAt = nil
            scheduleServerRestart(afterFailure: true)
        }
    }

    private func serverDidExit(_ process: Process) {
        guard serverProcess === process else { return }
        let reason = process.terminationReason == .uncaughtSignal ? "signal" : "exit"
        appendServerLog(
            "daemon pid \(process.processIdentifier) ended (\(reason) \(process.terminationStatus))"
        )
        serverLogHandle?.closeFile()
        serverLogHandle = nil
        serverProcess = nil
        serverStartedAt = nil
        unhealthySince = nil
        stableServerReset?.cancel()
        stableServerReset = nil

        guard !isTerminating else { return }
        if process.terminationStatus == 73 {
            appendServerLog("another SAGE daemon already owns the instance lock; supervisor will not respawn")
            return
        }
        scheduleServerRestart(afterFailure: true)
    }

    private func scheduleServerRestart(afterFailure: Bool) {
        guard !isTerminating else { return }
        pendingServerRestart?.cancel()
        if afterFailure {
            consecutiveServerFailures += 1
        }
        guard consecutiveServerFailures <= maximumAutomaticRestarts else {
            appendServerLog(
                "daemon restart budget exhausted after \(maximumAutomaticRestarts) attempts; leaving SAGE stopped"
            )
            return
        }
        let index = min(max(consecutiveServerFailures - 1, 0), restartDelays.count - 1)
        let delay = restartDelays[index]
        appendServerLog("restarting daemon in \(delay)s (attempt \(consecutiveServerFailures)/\(maximumAutomaticRestarts))")
        let work = DispatchWorkItem { [weak self] in
            guard let self, !self.isTerminating else { return }
            self.startServer()
        }
        pendingServerRestart = work
        DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: work)
    }

    private func scheduleStableServerReset(for process: Process) {
        stableServerReset?.cancel()
        let work = DispatchWorkItem { [weak self, weak process] in
            guard let self, let process, self.serverProcess === process else { return }
            self.consecutiveServerFailures = 0
            self.appendServerLog("daemon remained running for 60s; automatic restart budget reset")
        }
        stableServerReset = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 60, execute: work)
    }

    private func startHealthMonitor() {
        healthTimer?.invalidate()
        let timer = Timer(timeInterval: healthCheckInterval, repeats: true) { [weak self] _ in
            self?.checkServerHealth()
        }
        RunLoop.main.add(timer, forMode: .common)
        healthTimer = timer
    }

    private func checkServerHealth() {
        guard !isTerminating,
              !healthProbeInFlight,
              let process = serverProcess,
              process.isRunning,
              let startedAt = serverStartedAt,
              Date().timeIntervalSince(startedAt) >= serverStartupGrace,
              let url = URL(string: "http://127.0.0.1:8080/health") else {
            return
        }

        healthProbeInFlight = true
        var request = URLRequest(url: url)
        request.timeoutInterval = 2
        URLSession.shared.dataTask(with: request) { [weak self] _, response, _ in
            DispatchQueue.main.async {
                guard let self else { return }
                self.healthProbeInFlight = false
                guard self.serverProcess === process, process.isRunning, !self.isTerminating else {
                    return
                }
                if let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) {
                    self.unhealthySince = nil
                    return
                }

                if self.unhealthySince == nil {
                    self.unhealthySince = Date()
                    self.appendServerLog("daemon pid \(process.processIdentifier) stopped answering health checks")
                    return
                }
                if let since = self.unhealthySince,
                   Date().timeIntervalSince(since) >= self.unhealthyRestartGrace {
                    self.recoverUnresponsiveServer(process)
                }
            }
        }.resume()
    }

    private func recoverUnresponsiveServer(_ process: Process) {
        guard serverProcess === process, process.isRunning, !isTerminating else { return }
        unhealthySince = nil
        let pid = process.processIdentifier
        appendServerLog(
            "daemon pid \(pid) did not recover after \(Int(unhealthyRestartGrace))s; terminating it so the supervisor can restart"
        )
        process.terminate()
        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + 15) { [weak self] in
            guard process.isRunning else { return }
            self?.appendServerLog("unresponsive daemon pid \(pid) ignored SIGTERM; sending SIGKILL")
            kill(pid, SIGKILL)
        }
    }

    private func appendServerLog(_ message: String) {
        let line = "[\(ISO8601DateFormatter().string(from: Date()))] [sage-tray] \(message)\n"
        serverLogQueue.async { [weak self] in
            guard let self, let handle = self.openAppendLogHandle() else { return }
            defer { try? handle.close() }
            if let data = line.data(using: .utf8) {
                try? handle.write(contentsOf: data)
            }
        }
    }

    private func openAppendLogHandle() -> FileHandle? {
        let descriptor = open(logPath, O_WRONLY | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR)
        guard descriptor >= 0 else { return nil }
        return FileHandle(fileDescriptor: descriptor, closeOnDealloc: true)
    }

    /// Show a native macOS dialog to collect the vault passphrase.
    /// Returns the passphrase string, or nil if the user cancels.
    func promptPassphrase() -> String? {
        let alert = NSAlert()
        alert.messageText = "SAGE — Unlock Encrypted Memory"
        alert.informativeText = "Your memory vault is encrypted. Enter your passphrase to unlock."
        alert.alertStyle = .informational
        alert.addButton(withTitle: "Unlock")
        alert.addButton(withTitle: "Skip")

        let input = NSSecureTextField(frame: NSRect(x: 0, y: 0, width: 300, height: 24))
        input.placeholderString = "Vault passphrase"
        alert.accessoryView = input
        alert.window.initialFirstResponder = input

        let response = alert.runModal()
        if response == .alertFirstButtonReturn {
            let value = input.stringValue
            if !value.isEmpty { return value }
        }
        return nil
    }

    @objc func openDashboard() {
        dashboardOpenQueue.async { [weak self] in
            self?.openDashboardOnce()
        }
    }

    private func openDashboardOnce() {
        // NSWorkspace.open always asks the browser to open a URL, which creates a
        // fresh tab on every dock-icon click. Focus a matching tab first; only
        // open /ui/launch when no supported running browser already has CEREBRUM.
        if focusExistingDashboardTab() { return }

        // Firefox does not expose tab URLs through AppleScript. CEREBRUM keeps
        // an SSE stream alive for the lifetime of an unlocked dashboard tab, so
        // use that browser-independent signal and activate the default browser
        // instead of blindly creating a duplicate tab. If the user closed every
        // dashboard tab, the count falls to zero and the normal open path runs.
        if hasActiveDashboard(), activateDefaultBrowser() { return }

        _ = DispatchQueue.main.sync {
            NSWorkspace.shared.open(URL(string: "http://localhost:8080/ui/launch")!)
        }
    }

    private func hasActiveDashboard() -> Bool {
        guard let url = URL(string: "http://localhost:8080/ui/presence") else { return false }
        var request = URLRequest(url: url)
        request.timeoutInterval = 2

        let finished = DispatchSemaphore(value: 0)
        var active = false
        let task = URLSession.shared.dataTask(with: request) { data, _, _ in
            defer { finished.signal() }
            guard let data,
                  let payload = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let value = payload["active"] as? Bool else { return }
            active = value
        }
        task.resume()
        if finished.wait(timeout: .now() + 2.5) == .timedOut {
            task.cancel()
            return false
        }
        return active
    }

    private func activateDefaultBrowser() -> Bool {
        guard let dashboardURL = URL(string: "http://localhost:8080/ui/"),
              let appURL = NSWorkspace.shared.urlForApplication(toOpen: dashboardURL),
              let bundleID = Bundle(url: appURL)?.bundleIdentifier,
              let browser = NSRunningApplication.runningApplications(withBundleIdentifier: bundleID).first
        else { return false }
        return browser.activate(options: [.activateAllWindows, .activateIgnoringOtherApps])
    }

    /// Focus an existing CEREBRUM tab without launching a browser that is not
    /// already running. The browser list is keyed by bundle id so localized app
    /// names do not break detection. AppleScript failures (unsupported browser,
    /// denied Automation permission, browser update) safely fall back to opening
    /// the dashboard normally.
    func focusExistingDashboardTab() -> Bool {
        let runningIDs = Set(NSWorkspace.shared.runningApplications.compactMap { $0.bundleIdentifier })
        let chromiumIDs = [
            "com.google.Chrome",
            "com.google.Chrome.canary",
            "com.brave.Browser",
            "com.microsoft.edgemac",
            "company.thebrowser.Browser",
            "com.vivaldi.Vivaldi",
            "org.chromium.Chromium",
        ]

        for bundleID in chromiumIDs where runningIDs.contains(bundleID) {
            let script = """
            tell application id "\(bundleID)"
                repeat with w in windows
                    set tabIndex to 0
                    repeat with t in tabs of w
                        set tabIndex to tabIndex + 1
                        if (URL of t) contains "localhost:8080" or (URL of t) contains "127.0.0.1:8080" then
                            set active tab index of w to tabIndex
                            set index of w to 1
                            activate
                            return "focused"
                        end if
                    end repeat
                end repeat
            end tell
            return "none"
            """
            if runAppleScript(script) == "focused" { return true }
        }

        if runningIDs.contains("com.apple.Safari") {
            let script = """
            tell application id "com.apple.Safari"
                repeat with w in windows
                    repeat with t in tabs of w
                        if (URL of t) contains "localhost:8080" or (URL of t) contains "127.0.0.1:8080" then
                            set current tab of w to t
                            set index of w to 1
                            activate
                            return "focused"
                        end if
                    end repeat
                end repeat
            end tell
            return "none"
            """
            if runAppleScript(script) == "focused" { return true }
        }
        return false
    }

    private func runAppleScript(_ source: String) -> String? {
        let process = Process()
        let output = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/osascript")
        process.arguments = ["-e", source]
        process.standardOutput = output
        process.standardError = FileHandle.nullDevice
        let finished = DispatchSemaphore(value: 0)
        process.terminationHandler = { _ in finished.signal() }
        do {
            try process.run()
        } catch {
            return nil
        }
        if finished.wait(timeout: .now() + 5) == .timedOut {
            process.terminate()
            return nil
        }
        guard process.terminationStatus == 0 else { return nil }
        let data = output.fileHandleForReading.readDataToEndOfFile()
        return String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    @objc func showLog() {
        let script = """
            tell application "Terminal"
                activate
                do script "tail -f '\(logPath)'"
            end tell
        """
        if let appleScript = NSAppleScript(source: script) {
            appleScript.executeAndReturnError(nil)
        }
    }
}

let app = NSApplication.shared
let delegate = SAGEApp()
app.delegate = delegate
app.run()
