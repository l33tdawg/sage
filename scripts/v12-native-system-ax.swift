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
  v12-native-system-ax --pid <pid> --scenario <retry-fail|retry-restore> [--timeout <seconds>]
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

private func runScenario(arguments: Arguments) throws -> [String: Any] {
    let startedAt = Date()
    let startedInstant = clock.now
    guard let pid = arguments.pid, let scenario = arguments.scenario,
          ["retry-fail", "retry-restore"].contains(scenario)
    else { throw ProbeFailure.usage(usage) }

    let application = AXUIElementCreateApplication(pid)
    AXUIElementSetMessagingTimeout(application, 1.0)
    var reportedPID: pid_t = 0
    guard AXUIElementGetPid(application, &reportedPID) == .success, reportedPID == pid else {
        throw ProbeFailure.assertion("AX application PID does not match the requested process")
    }
    guard let runningApplication = NSRunningApplication(processIdentifier: pid),
          runningApplication.bundleIdentifier == "com.sage.cerebrum.beta"
    else { throw ProbeFailure.assertion("target process is not com.sage.cerebrum.beta") }
    let targetBundle = runningApplication.bundleURL.flatMap(Bundle.init(url:))
    _ = runningApplication.activate(options: [.activateAllWindows])
    guard stringAttribute(application, kAXRoleAttribute as CFString) == (kAXApplicationRole as String) else {
        throw ProbeFailure.assertion("target is not exposed as AXApplication")
    }
    let deadline = clock.now + .milliseconds(Int(arguments.timeoutSeconds * 1_000))
    while clock.now < deadline {
        if findElement(in: application, predicate: {
            stringAttribute($0, kAXRoleAttribute as CFString) == (kAXWindowRole as String) &&
                stringAttribute($0, kAXTitleAttribute as CFString) == "SAGE CEREBRUM"
        }) != nil { break }
        usleep(20_000)
    }
    guard findElement(in: application, predicate: {
        stringAttribute($0, kAXRoleAttribute as CFString) == (kAXWindowRole as String) &&
            stringAttribute($0, kAXTitleAttribute as CFString) == "SAGE CEREBRUM"
    }) != nil else { throw ProbeFailure.timeout("SAGE CEREBRUM AX window did not appear") }
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
        "window_title": "SAGE CEREBRUM",
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
