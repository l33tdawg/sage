import AppKit
import ApplicationServices
import Darwin
import Foundation

private enum ProbeFailure: Error, CustomStringConvertible {
    case usage(String)
    case timeout(String)
    case assertion(String)
    case ax(String, AXError)

    var description: String {
        switch self {
        case let .usage(message), let .timeout(message), let .assertion(message): message
        case let .ax(operation, error): "\(operation) failed with AX error \(error.rawValue)"
        }
    }
}

private struct Arguments {
    var preflight = false
    var prompt = false
    var pid: pid_t?
    var scenario: String?
    var timeoutSeconds = 12.0

    init(_ raw: [String]) throws {
        var index = 1
        while index < raw.count {
            switch raw[index] {
            case "--preflight": preflight = true
            case "--prompt": prompt = true
            case "--pid":
                index += 1
                guard index < raw.count, let value = Int32(raw[index]), value > 1 else {
                    throw ProbeFailure.usage("--pid requires a positive process ID")
                }
                pid = value
            case "--scenario":
                index += 1
                guard index < raw.count else { throw ProbeFailure.usage("--scenario requires a value") }
                scenario = raw[index]
            case "--timeout":
                index += 1
                guard index < raw.count, let value = Double(raw[index]), (1...60).contains(value) else {
                    throw ProbeFailure.usage("--timeout must be between 1 and 60 seconds")
                }
                timeoutSeconds = value
            case "--help", "-h":
                throw ProbeFailure.usage(usage)
            default:
                throw ProbeFailure.usage("unknown argument: \(raw[index])\n\(usage)")
            }
            index += 1
        }
    }
}

private let usage = """
usage:
  v12-native-system-ax --preflight [--prompt]
  v12-native-system-ax --pid <pid> --scenario <retry-fail|retry-restore|brain-menu-focus> [--timeout <seconds>]
"""

private let clock = ContinuousClock()
private let traversalLimits: [String: Int] = [
    "maximum_nodes": 8_192,
    "maximum_depth": 64,
    "maximum_children_per_node": 512,
    "child_page_size": 64,
]

private func attribute(_ element: AXUIElement, _ name: CFString) -> CFTypeRef? {
    var value: CFTypeRef?
    guard AXUIElementCopyAttributeValue(element, name, &value) == .success else { return nil }
    return value
}

private func stringAttribute(_ element: AXUIElement, _ name: CFString) -> String? {
    attribute(element, name) as? String
}

private func boolAttribute(_ element: AXUIElement, _ name: CFString) -> Bool? {
    attribute(element, name) as? Bool
}

private func elementAttribute(_ element: AXUIElement, _ name: CFString) -> AXUIElement? {
    guard let value = attribute(element, name), CFGetTypeID(value) == AXUIElementGetTypeID() else {
        return nil
    }
    return unsafeDowncast(value, to: AXUIElement.self)
}

private func pagedChildren(_ element: AXUIElement) -> [AXUIElement] {
    var rawCount: CFIndex = 0
    let countError = AXUIElementGetAttributeValueCount(
        element, kAXChildrenAttribute as CFString, &rawCount
    )
    guard countError == .success, rawCount > 0 else { return [] }
    let boundedCount = min(rawCount, CFIndex(traversalLimits["maximum_children_per_node"]!))
    let pageSize = CFIndex(traversalLimits["child_page_size"]!)
    var result: [AXUIElement] = []
    var start: CFIndex = 0
    while start < boundedCount {
        var values: CFArray?
        let error = AXUIElementCopyAttributeValues(
            element,
            kAXChildrenAttribute as CFString,
            start,
            min(pageSize, boundedCount - start),
            &values
        )
        guard error == .success, let values else { break }
        for value in values as [AnyObject] where CFGetTypeID(value) == AXUIElementGetTypeID() {
            result.append(unsafeDowncast(value, to: AXUIElement.self))
        }
        start += pageSize
    }
    return result
}

private func wasVisited(
    _ element: AXUIElement,
    buckets: inout [CFHashCode: [AXUIElement]]
) -> Bool {
    let hash = CFHash(element)
    if buckets[hash]?.contains(where: { CFEqual($0, element) }) == true { return true }
    buckets[hash, default: []].append(element)
    return false
}

private func findElement(
    in application: AXUIElement,
    predicate: (AXUIElement) -> Bool
) -> AXUIElement? {
    var queue: [(element: AXUIElement, depth: Int)] = [(application, 0)]
    var cursor = 0
    var visited: [CFHashCode: [AXUIElement]] = [:]
    while cursor < queue.count, cursor < traversalLimits["maximum_nodes"]! {
        let entry = queue[cursor]
        cursor += 1
        guard !wasVisited(entry.element, buckets: &visited) else { continue }
        if predicate(entry.element) { return entry.element }
        guard entry.depth < traversalLimits["maximum_depth"]! else { continue }
        queue.append(contentsOf: pagedChildren(entry.element).map { ($0, entry.depth + 1) })
    }
    return nil
}

private func findElement(identifier: String, in application: AXUIElement) -> AXUIElement? {
    findElement(in: application) {
        stringAttribute($0, kAXIdentifierAttribute as CFString) == identifier
    }
}

private func waitForElement(
    identifier: String,
    in application: AXUIElement,
    deadline: ContinuousClock.Instant,
    predicate: (AXUIElement) -> Bool = { _ in true }
) throws -> AXUIElement {
    while clock.now < deadline {
        if let element = findElement(identifier: identifier, in: application), predicate(element) {
            return element
        }
        usleep(20_000)
    }
    throw ProbeFailure.timeout("timed out waiting for system AX element \(identifier)")
}

private func focusedElement(_ owner: AXUIElement) -> AXUIElement? {
    elementAttribute(owner, kAXFocusedUIElementAttribute as CFString)
}

private func focusedIdentifier() -> String? {
    let system = AXUIElementCreateSystemWide()
    guard let focused = focusedElement(system) else { return nil }
    return stringAttribute(focused, kAXIdentifierAttribute as CFString)
}

private func ownsExactFocus(_ expected: AXUIElement, application: AXUIElement, pid: pid_t) -> Bool {
    guard let applicationFocused = focusedElement(application),
          let systemFocused = focusedElement(AXUIElementCreateSystemWide()) else { return false }
    var focusedPID: pid_t = 0
    return AXUIElementGetPid(systemFocused, &focusedPID) == .success && focusedPID == pid &&
        CFEqual(applicationFocused, expected) && CFEqual(systemFocused, expected) &&
        boolAttribute(expected, kAXFocusedAttribute as CFString) == true
}

private func waitForFocus(
    element expected: AXUIElement,
    application: AXUIElement,
    pid: pid_t,
    deadline: ContinuousClock.Instant
) throws {
    let system = AXUIElementCreateSystemWide()
    while clock.now < deadline {
        if let applicationFocused = focusedElement(application),
           let systemFocused = focusedElement(system) {
            var focusedPID: pid_t = 0
            let pidResult = AXUIElementGetPid(systemFocused, &focusedPID)
            if pidResult == .success,
               focusedPID == pid,
               CFEqual(applicationFocused, expected),
               CFEqual(systemFocused, expected),
               boolAttribute(expected, kAXFocusedAttribute as CFString) == true {
                return
            }
        }
        usleep(20_000)
    }
    var details: [String: Any] = ["expected": snapshot(expected),
        "expected_focused": boolAttribute(expected, kAXFocusedAttribute as CFString) as Any? ?? NSNull()]
    if let appFocus = focusedElement(application) {
        details["application_focused"] = snapshot(appFocus)
        details["application_equal"] = CFEqual(appFocus, expected)
    }
    if let systemFocus = focusedElement(system) {
        details["system_focused"] = snapshot(systemFocus)
        details["system_equal"] = CFEqual(systemFocus, expected)
    }
    let data = try JSONSerialization.data(withJSONObject: ["focus_diagnostic": details], options: [.sortedKeys])
    FileHandle.standardError.write(data + Data("\n".utf8))
    throw ProbeFailure.timeout("system and application AX focus did not reach the exact expected element")
}

private func label(_ element: AXUIElement) -> String? {
    stringAttribute(element, kAXDescriptionAttribute as CFString) ??
        stringAttribute(element, kAXTitleAttribute as CFString)
}

private func snapshot(_ element: AXUIElement) -> [String: Any] {
    var result: [String: Any] = [
        "identifier": stringAttribute(element, kAXIdentifierAttribute as CFString) ?? "",
        "role": stringAttribute(element, kAXRoleAttribute as CFString) ?? "",
        "label": label(element) ?? "",
        "enabled": boolAttribute(element, kAXEnabledAttribute as CFString) ?? false,
    ]
    if let help = stringAttribute(element, kAXHelpAttribute as CFString) { result["help"] = help }
    if let value = stringAttribute(element, kAXValueAttribute as CFString) { result["value"] = value }
    return result
}

private func assertRetryReady(_ element: AXUIElement) throws {
    guard stringAttribute(element, kAXRoleAttribute as CFString) == (kAXButtonRole as String) else {
        throw ProbeFailure.assertion("brain-metal-retry is not exposed as an AX button")
    }
    guard label(element) == "Try MRI Again" else {
        throw ProbeFailure.assertion("brain-metal-retry has an unexpected ready label")
    }
    guard boolAttribute(element, kAXEnabledAttribute as CFString) == true else {
        throw ProbeFailure.assertion("brain-metal-retry is not enabled before AXPress")
    }
    guard !(stringAttribute(element, kAXHelpAttribute as CFString) ?? "").isEmpty else {
        throw ProbeFailure.assertion("brain-metal-retry has no AX help")
    }
}

private func attributeCount(_ element: AXUIElement, _ name: CFString) -> Int {
    var count: CFIndex = 0
    guard AXUIElementGetAttributeValueCount(element, name, &count) == .success else { return 0 }
    return count
}

private func elementArrayAttribute(_ element: AXUIElement, _ name: CFString) -> [AXUIElement] {
    guard let values = attribute(element, name) as? [AnyObject] else { return [] }
    return values.compactMap {
        guard CFGetTypeID($0) == AXUIElementGetTypeID() else { return nil }
        return unsafeDowncast($0, to: AXUIElement.self)
    }
}

private func press(_ element: AXUIElement, operation: String) throws -> AXError {
    let result = AXUIElementPerformAction(element, kAXPressAction as CFString)
    // cannotComplete can mean the app entered menu tracking before replying.
    // Never repeat the action: the following observed state must prove its effect.
    guard result == .success || result == .cannotComplete else {
        throw ProbeFailure.ax(operation, result)
    }
    return result
}

private func waitForMatch(
    in root: AXUIElement,
    deadline: ContinuousClock.Instant,
    description: String,
    predicate: (AXUIElement) -> Bool
) throws -> AXUIElement {
    while clock.now < deadline {
        if let match = findElement(in: root, predicate: predicate) { return match }
        usleep(20_000)
    }
    throw ProbeFailure.timeout("timed out waiting for \(description)")
}

private func pressMenuPath(
    _ path: [String], application: AXUIElement, deadline: ContinuousClock.Instant
) throws -> [String: Any] {
    guard let first = path.first, path.count >= 2,
          let menuBar = elementAttribute(application, kAXMenuBarAttribute as CFString)
    else { throw ProbeFailure.assertion("application has no AX menu bar") }
    FileHandle.standardError.write(Data("AX menu: \(path.joined(separator: " > "))\n".utf8))
    var parent = try waitForMatch(in: menuBar, deadline: deadline, description: "menu \(first)") {
        stringAttribute($0, kAXRoleAttribute as CFString) == (kAXMenuBarItemRole as String) &&
            stringAttribute($0, kAXTitleAttribute as CFString) == first
    }
    var results: [Int32] = []
    results.append(try press(parent, operation: "AXPress menu \(first)").rawValue)
    for title in path.dropFirst() {
        let item = try waitForMatch(in: parent, deadline: deadline, description: "menu item \(title)") {
            stringAttribute($0, kAXRoleAttribute as CFString) == (kAXMenuItemRole as String) &&
                stringAttribute($0, kAXTitleAttribute as CFString) == title &&
                boolAttribute($0, kAXEnabledAttribute as CFString) == true
        }
        results.append(try press(item, operation: "AXPress \(title)").rawValue)
        parent = item
    }
    return ["path": path, "ax_press_results": results]
}

private func waitForAbsence(identifier: String, application: AXUIElement, deadline: ContinuousClock.Instant) throws {
    while clock.now < deadline {
        if findElement(identifier: identifier, in: application) == nil { return }
        usleep(20_000)
    }
    throw ProbeFailure.timeout("system AX element remained mounted: \(identifier)")
}

private func waitForTable(
    _ identifier: String, application: AXUIElement, pid: pid_t,
    deadline: ContinuousClock.Instant
) throws -> AXUIElement {
    let table: AXUIElement
    do {
        table = try waitForMatch(in: application, deadline: deadline, description: "row-bearing AX table \(identifier)") {
            stringAttribute($0, kAXIdentifierAttribute as CFString) == identifier &&
                [kAXTableRole as String, kAXOutlineRole as String].contains(stringAttribute($0, kAXRoleAttribute as CFString) ?? "") &&
                attributeCount($0, kAXRowsAttribute as CFString) > 0 &&
                ownsExactFocus($0, application: application, pid: pid)
        }
    } catch {
        var matches: [[String: Any]] = []
        _ = findElement(in: application) { element in
            if stringAttribute(element, kAXIdentifierAttribute as CFString) == identifier, matches.count < 8 {
                var details = snapshot(element)
                details["row_count"] = attributeCount(element, kAXRowsAttribute as CFString)
                matches.append(details)
            }
            return false
        }
        let data = try JSONSerialization.data(withJSONObject: ["table_diagnostic": matches, "focused_identifier": focusedIdentifier() ?? ""], options: [.sortedKeys])
        FileHandle.standardError.write(data + Data("\n".utf8))
        throw error
    }
    try waitForFocus(element: table, application: application, pid: pid, deadline: deadline)
    return table
}

private func selectedRowContains(_ table: AXUIElement, text: String) -> Bool {
    let selected = elementArrayAttribute(table, kAXSelectedRowsAttribute as CFString)
    guard selected.count == 1 else { return false }
    return findElement(in: selected[0]) {
        stringAttribute($0, kAXValueAttribute as CFString) == text ||
            stringAttribute($0, kAXTitleAttribute as CFString) == text ||
            stringAttribute($0, kAXDescriptionAttribute as CFString) == text
    } != nil
}

private func waitForSelectedRow(
    _ text: String, tableIdentifier: String, application: AXUIElement,
    deadline: ContinuousClock.Instant
) throws -> AXUIElement {
    while clock.now < deadline {
        if let table = findElement(in: application, predicate: {
            stringAttribute($0, kAXIdentifierAttribute as CFString) == tableIdentifier &&
                [kAXTableRole as String, kAXOutlineRole as String].contains(stringAttribute($0, kAXRoleAttribute as CFString) ?? "")
        }), selectedRowContains(table, text: text) { return table }
        usleep(20_000)
    }
    throw ProbeFailure.timeout("the expected synthetic fixture row was not selected")
}

private func postSyntheticKey(_ keyCode: CGKeyCode, flags: CGEventFlags, pid: pid_t) throws {
    guard NSWorkspace.shared.frontmostApplication?.processIdentifier == pid,
          let focus = focusedElement(AXUIElementCreateSystemWide()) else {
        throw ProbeFailure.assertion("refusing keyboard injection outside the target foreground app")
    }
    var focusPID: pid_t = 0
    guard AXUIElementGetPid(focus, &focusPID) == .success, focusPID == pid,
          CGPreflightPostEventAccess(),
          let source = CGEventSource(stateID: .combinedSessionState),
          let down = CGEvent(keyboardEventSource: source, virtualKey: keyCode, keyDown: true),
          let up = CGEvent(keyboardEventSource: source, virtualKey: keyCode, keyDown: false)
    else { throw ProbeFailure.assertion("synthetic keyboard event posting is unavailable or focus left the target") }
    down.flags = flags
    up.flags = flags
    down.post(tap: .cgSessionEventTap)
    up.post(tap: .cgSessionEventTap)
}

private func runBrainMenuFocus(
    application: AXUIElement, pid: pid_t, deadline: ContinuousClock.Instant
) throws -> [String: Any] {
    var actions: [[String: Any]] = []
    actions.append(try pressMenuPath(["Navigate", "Brain"], application: application, deadline: deadline))
    actions.append(try pressMenuPath(["View", "Brain Presentation", "List View"], application: application, deadline: deadline))
    let initialTable = try waitForTable("brain-memory-table", application: application, pid: pid, deadline: deadline)
    var initialTableSnapshot = snapshot(initialTable)
    initialTableSnapshot["row_count"] = attributeCount(initialTable, kAXRowsAttribute as CFString)
    guard attributeCount(initialTable, kAXRowsAttribute as CFString) > 0 else {
        throw ProbeFailure.assertion("memory table exposes no AX rows")
    }
    try postSyntheticKey(125, flags: [], pid: pid) // Down arrow; external WindowServer injection, never physical HID.
    _ = try waitForSelectedRow("Native CEREBRUM architecture", tableIdentifier: "brain-memory-table", application: application, deadline: deadline)
    _ = try waitForElement(identifier: "brain-inspector-close", in: application, deadline: deadline)
    actions.append(try pressMenuPath(["View", "Hide Inspector"], application: application, deadline: deadline))
    try waitForAbsence(identifier: "brain-inspector-close", application: application, deadline: deadline)
    let hiddenTable = try waitForTable("brain-memory-table", application: application, pid: pid, deadline: deadline)
    guard selectedRowContains(hiddenTable, text: "Native CEREBRUM architecture"),
          findElement(identifier: "brain-inspector-close", in: application) == nil else {
        throw ProbeFailure.assertion("hiding the inspector lost selection or left its close control mounted")
    }
    actions.append(try pressMenuPath(["View", "Show Inspector"], application: application, deadline: deadline))
    let close = try waitForElement(identifier: "brain-inspector-close", in: application, deadline: deadline) {
        stringAttribute($0, kAXRoleAttribute as CFString) == (kAXButtonRole as String)
    }
    try waitForFocus(element: close, application: application, pid: pid, deadline: deadline)
    let closeSnapshot = snapshot(close)
    _ = try press(close, operation: "AXPress Brain inspector close")
    try waitForAbsence(identifier: "brain-inspector-close", application: application, deadline: deadline)
    let restoredTable = try waitForTable("brain-memory-table", application: application, pid: pid, deadline: deadline)
    guard selectedRowContains(restoredTable, text: "Native CEREBRUM architecture") else {
        throw ProbeFailure.assertion("closing the inspector lost the selected memory")
    }
    try postSyntheticKey(34, flags: [.maskControl, .maskCommand], pid: pid) // Control-Command-I.
    let keyboardClose = try waitForElement(identifier: "brain-inspector-close", in: application, deadline: deadline)
    try waitForFocus(element: keyboardClose, application: application, pid: pid, deadline: deadline)
    actions.append(try pressMenuPath(["View", "Hide Inspector"], application: application, deadline: deadline))
    try waitForAbsence(identifier: "brain-inspector-close", application: application, deadline: deadline)
    _ = try waitForTable("brain-memory-table", application: application, pid: pid, deadline: deadline)
    actions.append(try pressMenuPath(["View", "Brain Mode", "Agent Network"], application: application, deadline: deadline))
    let agentTable = try waitForTable("brain-connectome-table", application: application, pid: pid, deadline: deadline)
    guard attributeCount(agentTable, kAXRowsAttribute as CFString) == 3 else {
        throw ProbeFailure.assertion("agent table is not the three-row synthetic fixture")
    }
    try postSyntheticKey(125, flags: [], pid: pid)
    _ = try waitForSelectedRow("Codex", tableIdentifier: "brain-connectome-table", application: application, deadline: deadline)
    actions.append(try pressMenuPath(["View", "Show Inspector"], application: application, deadline: deadline))
    let agentClose = try waitForElement(identifier: "brain-inspector-close", in: application, deadline: deadline)
    try waitForFocus(element: agentClose, application: application, pid: pid, deadline: deadline)
    _ = try press(agentClose, operation: "AXPress agent inspector close")
    try waitForAbsence(identifier: "brain-inspector-close", application: application, deadline: deadline)
    let finalTable = try waitForTable("brain-connectome-table", application: application, pid: pid, deadline: deadline)
    guard selectedRowContains(finalTable, text: "Codex") else {
        throw ProbeFailure.assertion("closing the agent inspector lost selection")
    }
    var finalSnapshot = snapshot(finalTable)
    finalSnapshot["row_count"] = attributeCount(finalTable, kAXRowsAttribute as CFString)
    return [
        "menu_actions": actions,
        "initial_table": initialTableSnapshot,
        "inspector_close": closeSnapshot,
        "final": finalSnapshot,
        "memory_selection_preserved": true,
        "agent_selection_preserved": true,
        "exact_application_and_system_focus": true,
        "synthetic_windowserver_keyboard_events": true,
        "physical_keyboard_event_routing": false,
        "keyboard_sequence": ["Down", "Control-Command-I", "Down"],
        "fixture_focus_injection": false,
        "focused_identifier": focusedIdentifier() ?? "",
    ]
}

private func runScenario(arguments: Arguments) throws -> [String: Any] {
    let startedAt = Date()
    let startedInstant = clock.now
    guard let pid = arguments.pid, let scenario = arguments.scenario,
          ["retry-fail", "retry-restore", "brain-menu-focus"].contains(scenario)
    else { throw ProbeFailure.usage(usage) }

    let application = AXUIElementCreateApplication(pid)
    AXUIElementSetMessagingTimeout(application, 1.0)
    var reportedPID: pid_t = 0
    guard AXUIElementGetPid(application, &reportedPID) == .success, reportedPID == pid else {
        throw ProbeFailure.assertion("AX application PID does not match the requested process")
    }
    let deadline = clock.now + .milliseconds(Int(arguments.timeoutSeconds * 1_000))
    var candidate = NSRunningApplication(processIdentifier: pid)
    while clock.now < deadline, candidate?.bundleIdentifier == nil {
        guard kill(pid, 0) == 0 else { throw ProbeFailure.assertion("target process exited before application registration") }
        usleep(20_000)
        candidate = NSRunningApplication(processIdentifier: pid)
    }
    guard let runningApplication = candidate,
          runningApplication.bundleIdentifier == "com.sage.cerebrum.beta"
    else { throw ProbeFailure.assertion("target process is not com.sage.cerebrum.beta") }
    let targetBundle = runningApplication.bundleURL.flatMap(Bundle.init(url:))
    _ = runningApplication.activate(options: [.activateAllWindows])
    while clock.now < deadline,
          stringAttribute(application, kAXRoleAttribute as CFString) != (kAXApplicationRole as String) {
        usleep(20_000)
    }
    guard stringAttribute(application, kAXRoleAttribute as CFString) == (kAXApplicationRole as String) else {
        throw ProbeFailure.timeout("target was not exposed as AXApplication before the deadline")
    }
    let expectedWindowTitles: Set<String> = scenario == "brain-menu-focus"
        ? ["SAGE CEREBRUM", "Overview"] : ["SAGE CEREBRUM", "Brain"]
    let window = try waitForMatch(in: application, deadline: deadline, description: "main native AX window") {
        stringAttribute($0, kAXRoleAttribute as CFString) == (kAXWindowRole as String) &&
            expectedWindowTitles.contains(stringAttribute($0, kAXTitleAttribute as CFString) ?? "")
    }
    let initialWindowTitle = stringAttribute(window, kAXTitleAttribute as CFString) ?? ""
    _ = runningApplication.activate(options: [.activateAllWindows])
    if scenario == "brain-menu-focus" {
        var result = try runBrainMenuFocus(application: application, pid: pid, deadline: deadline)
        result.merge([
            "schema": "sage.v12.native-system-ax.brain.v1",
            "scenario": scenario,
            "pid": Int(pid),
            "bundle_id": runningApplication.bundleIdentifier ?? "",
            "bundle_version": targetBundle?.object(forInfoDictionaryKey: "SAGEBetaVersion") as? String ?? "",
            "window_title": initialWindowTitle,
            "started_at": ISO8601DateFormatter().string(from: startedAt),
            "completed_at": ISO8601DateFormatter().string(from: Date()),
            "os_version": ProcessInfo.processInfo.operatingSystemVersionString,
            "trusted": true,
            "system_ax_server": true,
            "voiceover_spoken_evidence": false,
            "traversal_limits": traversalLimits,
            "passed": true,
        ]) { _, new in new }
        return result
    }
    let retry = try waitForElement(identifier: "brain-metal-retry", in: application, deadline: deadline)
    _ = try waitForElement(identifier: "brain-metal-fallback-notice", in: application, deadline: deadline)
    try assertRetryReady(retry)
    let ready = snapshot(retry)

    let pressError = AXUIElementPerformAction(retry, kAXPressAction as CFString)
    guard pressError == .success || pressError == .cannotComplete else {
        throw ProbeFailure.ax("AXPress brain-metal-retry", pressError)
    }

    let inFlight = try waitForElement(identifier: "brain-metal-retry", in: application, deadline: deadline) {
        label($0) == "Trying MRI" &&
            stringAttribute($0, kAXValueAttribute as CFString) == "In progress" &&
            boolAttribute($0, kAXEnabledAttribute as CFString) == false
    }

    let finalIdentifier: String
    let finalElement: AXUIElement
    if scenario == "retry-fail" {
        finalIdentifier = "brain-metal-retry"
        finalElement = try waitForElement(identifier: finalIdentifier, in: application, deadline: deadline) {
            label($0) == "Try MRI Again" && boolAttribute($0, kAXEnabledAttribute as CFString) == true
        }
    } else {
        finalIdentifier = "brain-memory-metal-surface"
        finalElement = try waitForElement(identifier: finalIdentifier, in: application, deadline: deadline)
    }
    try waitForFocus(element: finalElement, application: application, pid: pid, deadline: deadline)

    let elapsed = startedInstant.duration(to: clock.now).components
    let durationMilliseconds = elapsed.seconds * 1_000 + elapsed.attoseconds / 1_000_000_000_000_000
    return [
        "schema": "sage.v12.native-system-ax.v1",
        "scenario": scenario,
        "pid": Int(pid),
        "bundle_id": runningApplication.bundleIdentifier ?? "",
        "bundle_version": targetBundle?.object(forInfoDictionaryKey: "SAGEBetaVersion") as? String ?? "",
        "window_title": initialWindowTitle,
        "started_at": ISO8601DateFormatter().string(from: startedAt),
        "completed_at": ISO8601DateFormatter().string(from: Date()),
        "duration_ms": durationMilliseconds,
        "os_version": ProcessInfo.processInfo.operatingSystemVersionString,
        "trusted": true,
        "system_ax_server": true,
        "voiceover_spoken_evidence": false,
        "ax_press_result": pressError.rawValue,
        "traversal_limits": traversalLimits,
        "ready": ready,
        "in_flight": snapshot(inFlight),
        "final": snapshot(finalElement),
        "focused_identifier": focusedIdentifier() ?? "",
        "passed": true,
    ]
}

private func emit(_ object: [String: Any]) throws {
    let data = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
    FileHandle.standardOutput.write(data)
    FileHandle.standardOutput.write(Data("\n".utf8))
}

do {
    let arguments = try Arguments(CommandLine.arguments)
    let options = [kAXTrustedCheckOptionPrompt.takeUnretainedValue() as String: arguments.prompt] as CFDictionary
    let trusted = AXIsProcessTrustedWithOptions(options)
    if arguments.preflight {
        try emit([
            "schema": "sage.v12.native-system-ax.preflight.v1",
            "trusted": trusted,
            "prompt_requested": arguments.prompt,
        ])
        exit(trusted ? 0 : 77)
    }
    guard trusted else {
        try emit([
            "schema": "sage.v12.native-system-ax.preflight.v1",
            "trusted": false,
            "prompt_requested": false,
        ])
        exit(77)
    }
    try emit(runScenario(arguments: arguments))
} catch let error as ProbeFailure {
    FileHandle.standardError.write(Data("v12 native system AX probe: \(error)\n".utf8))
    if case .usage = error { exit(64) }
    exit(1)
} catch {
    FileHandle.standardError.write(Data("v12 native system AX probe: \(error)\n".utf8))
    exit(1)
}
