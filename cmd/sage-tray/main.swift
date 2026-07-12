import Cocoa

// SAGE Dock App — launches the SAGE server and manages it from the dock.
// Double-click: starts SAGE, opens CEREBRUM dashboard
// Right-click dock icon: Open CEREBRUM, Show Log, Quit

class SAGEApp: NSObject, NSApplicationDelegate {
    var serverProcess: Process?
    var vaultPassphrase: String?
    private let dashboardOpenQueue = DispatchQueue(label: "com.sage.dashboard-open", qos: .userInitiated)

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

        // Open dashboard after server has a moment to start
        DispatchQueue.main.asyncAfter(deadline: .now() + 3) {
            // First launch cannot have a SAGE tab from this app instance; open
            // directly so users are not asked for browser Automation permission
            // until tab reuse is actually needed on a later dock click.
            NSWorkspace.shared.open(URL(string: "http://localhost:8080/ui/launch")!)
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
        serverProcess?.terminate()
        serverProcess?.waitUntilExit()
    }

    // MARK: - Server Management

    func startServer() {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: sageBinary)
        process.arguments = ["serve"]

        var env = ProcessInfo.processInfo.environment
        env["SAGE_NO_BROWSER"] = "1"
        if let passphrase = vaultPassphrase {
            env["SAGE_PASSPHRASE"] = passphrase
            vaultPassphrase = nil // clear from memory after passing to server
        }
        process.environment = env

        FileManager.default.createFile(atPath: logPath, contents: nil)
        if let logHandle = FileHandle(forWritingAtPath: logPath) {
            logHandle.seekToEndOfFile()
            process.standardOutput = logHandle
            process.standardError = logHandle
        }

        do {
            try process.run()
            serverProcess = process
        } catch {
            NSLog("Failed to start SAGE server: \(error)")
        }
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
        _ = DispatchQueue.main.sync {
            NSWorkspace.shared.open(URL(string: "http://localhost:8080/ui/launch")!)
        }
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
