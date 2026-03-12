import Cocoa

// SAGE Dock App — launches the SAGE server and manages it from the dock.
// Double-click: starts SAGE, opens CEREBRUM dashboard
// Right-click dock icon: Open CEREBRUM, Show Log, Quit

class SAGEApp: NSObject, NSApplicationDelegate {
    var serverProcess: Process?

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

    func applicationDidFinishLaunching(_ notification: Notification) {
        startServer()

        // Open dashboard after server has a moment to start
        DispatchQueue.main.asyncAfter(deadline: .now() + 3) {
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

    @objc func openDashboard() {
        // /ui/launch uses window.open with a named target ('cerebrum'),
        // so the browser reuses the same tab across ALL browsers — no AppleScript needed.
        NSWorkspace.shared.open(URL(string: "http://localhost:8080/ui/launch")!)
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
