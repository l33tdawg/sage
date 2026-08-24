#if DEBUG
import AppKit
import Darwin
import Foundation
import SwiftUI

struct NativeAppSceneAcceptanceFixture: Equatable {
    static let scenario = "rendered-menu-mounted-search-focus"
    let commit: String
    let sourceState: String

    init?(environment: [String: String] = ProcessInfo.processInfo.environment) {
        guard environment["SAGE_NATIVE_DESIGN_PREVIEW"] == "1",
              environment["SAGE_NATIVE_APP_SCENE_ACCEPTANCE"] == Self.scenario,
              let commit = environment["SAGE_NATIVE_APP_SCENE_COMMIT"],
              commit.range(of: #"^[0-9a-f]{40}$"#, options: .regularExpression) != nil,
              let sourceState = environment["SAGE_NATIVE_APP_SCENE_SOURCE_STATE"],
              sourceState.range(of: #"^(clean|dirty):[0-9a-f]{64}$"#, options: .regularExpression) != nil
        else { return nil }
        self.commit = commit
        self.sourceState = sourceState
    }

    @MainActor
    func run(window: NSWindow, focusSink: NSView, session: AppSession) async -> Never {
        let runner = NativeAppSceneAcceptanceRunner(
            window: window,
            focusSink: focusSink,
            session: session,
            commit: commit,
            sourceState: sourceState
        )
        let result: [String: Any]
        do {
            result = try await runner.run()
        } catch {
            result = runner.failureResult(error.localizedDescription)
        }
        do {
            let data = try JSONSerialization.data(withJSONObject: result, options: [.sortedKeys])
            FileHandle.standardOutput.write(data)
            FileHandle.standardOutput.write(Data("\n".utf8))
            try FileHandle.standardOutput.synchronize()
        } catch {
            FileHandle.standardError.write(Data("native app-scene fixture could not emit JSON: \(error)\n".utf8))
            Darwin.exit(1)
        }
        Darwin.exit(result["passed"] as? Bool == true ? 0 : 1)
    }
}

struct NativeAppSceneAcceptanceProbe: NSViewRepresentable {
    let fixture: NativeAppSceneAcceptanceFixture
    let session: AppSession

    func makeNSView(context: Context) -> NativeAppSceneProbeView {
        let view = NativeAppSceneProbeView()
        view.onWindow = { window in
            guard !context.coordinator.didStart else { return }
            context.coordinator.didStart = true
            Task { @MainActor in await fixture.run(window: window, focusSink: view, session: session) }
        }
        return view
    }

    func updateNSView(_ nsView: NativeAppSceneProbeView, context: Context) {}
    func makeCoordinator() -> Coordinator { Coordinator() }

    final class Coordinator { var didStart = false }
}

final class NativeAppSceneProbeView: NSView {
    var onWindow: ((NSWindow) -> Void)?
    override var acceptsFirstResponder: Bool { true }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        guard let window else { return }
        DispatchQueue.main.async { [weak self, weak window] in
            if let window { self?.onWindow?(window) }
        }
    }
}

@MainActor
private final class NativeAppSceneAcceptanceRunner {
    private enum FixtureError: LocalizedError {
        case assertion(String)
        case timeout(String)

        var errorDescription: String? {
            switch self {
            case let .assertion(message), let .timeout(message): message
            }
        }
    }

    private let window: NSWindow
    private let focusSink: NSView
    private let session: AppSession
    private let commit: String
    private let sourceState: String
    private let startedAt = Date()
    private let startedInstant = ContinuousClock.now
    private let deadline: ContinuousClock.Instant
    private var assertions: [[String: Any]] = []
    private var menuSnapshot: [[String: Any]] = []
    private var responderSnapshots: [[String: Any]] = []

    init(window: NSWindow, focusSink: NSView, session: AppSession, commit: String, sourceState: String) {
        self.window = window
        self.focusSink = focusSink
        self.session = session
        self.commit = commit
        self.sourceState = sourceState
        deadline = startedInstant + .seconds(15)
    }

    func run() async throws -> [String: Any] {
        NSApp.activate(ignoringOtherApps: true)
        window.makeKeyAndOrderFront(nil)
        try await wait("real app window and main menu") {
            self.window.isVisible && self.window.contentView != nil && NSApp.mainMenu != nil
        }
        guard NSApp.windows.contains(where: { $0 === window }),
              window.windowController?.window === window || window.windowController == nil else {
            throw FixtureError.assertion("captured scene window is not owned by this NSApplication")
        }
        record(
            "captured-real-scene-window",
            expected: "captured probe window is visible and owned by NSApplication",
            actual: "title=\(window.title), visible=\(window.isVisible)"
        )

        guard let mainMenu = NSApp.mainMenu else {
            throw FixtureError.assertion("NSApplication.mainMenu is unavailable")
        }
        update(menu: mainMenu)
        let renderedMenuEntries = menuEntries(in: mainMenu)
        guard renderedMenuEntries.count <= 256 else {
            throw FixtureError.assertion("rendered menu inventory exceeded 256 items")
        }
        menuSnapshot = snapshot(entries: renderedMenuEntries)

        let focusItem = try uniqueMenuItem(
            in: mainMenu,
            parent: "View",
            title: "Focus Search",
            key: "f",
            modifiers: [.command]
        )
        record("rendered-focus-search-menu", expected: "View > Focus Search | command-f | enabled", actual: menuDescription(focusItem))
        guard focusItem.isEnabled else { throw FixtureError.assertion("rendered Focus Search item is disabled") }

        let firstRequest = session.searchFocusRequestID &+ 1
        try dispatch(focusItem)
        try await wait("first Focus Search request and mounted field-editor focus") {
            self.session.route == .search &&
                self.session.searchFocusRequestID == firstRequest &&
                self.session.consumedSearchFocusRequestID == firstRequest &&
                self.focusedSearchField() != nil
        }
        guard let firstField = focusedSearchField() else {
            throw FixtureError.assertion("mounted search field did not own its window field editor")
        }
        responderSnapshots.append(responderSnapshot(stage: "first-focus", field: firstField))
        record("first-mounted-search-focus", expected: "search field currentEditor is exact window firstResponder", actual: "matched")

        guard focusSink.window === window,
              window.makeFirstResponder(focusSink),
              window.firstResponder === focusSink else {
            throw FixtureError.assertion("could not move focus away before repeated command")
        }
        let secondRequest = firstRequest &+ 1
        update(menu: mainMenu)
        let repeatedItem = try uniqueMenuItem(
            in: mainMenu,
            parent: "View",
            title: "Focus Search",
            key: "f",
            modifiers: [.command]
        )
        try dispatch(repeatedItem)
        try await wait("repeated Focus Search request and mounted field-editor focus") {
            self.session.route == .search &&
                self.session.searchFocusRequestID == secondRequest &&
                self.session.consumedSearchFocusRequestID == secondRequest &&
                self.focusedSearchField() != nil
        }
        guard let secondField = focusedSearchField(), secondField === firstField else {
            throw FixtureError.assertion("repeated Focus Search did not restore the exact mounted field")
        }
        responderSnapshots.append(responderSnapshot(stage: "repeated-focus", field: secondField))
        record("repeated-mounted-search-focus", expected: "one increment, one consumption, exact mounted field-editor restored", actual: "matched")

        return result(passed: true, failure: nil)
    }

    func failureResult(_ failure: String) -> [String: Any] {
        result(passed: false, failure: String(failure.prefix(1_024)))
    }

    private func result(passed: Bool, failure: String?) -> [String: Any] {
        let elapsed = startedInstant.duration(to: .now).components
        var value: [String: Any] = [
            "schema": "sage.v12.native-app-scene.v1",
            "scenario": NativeAppSceneAcceptanceFixture.scenario,
            "commit": commit,
            "source_state": sourceState,
            "pid": Int(ProcessInfo.processInfo.processIdentifier),
            "bundle_id": Bundle.main.bundleIdentifier ?? "",
            "bundle_version": Bundle.main.object(forInfoDictionaryKey: "SAGEBetaVersion") as? String ?? "",
            "architecture": architecture,
            "os_version": ProcessInfo.processInfo.operatingSystemVersionString,
            "started_at": ISO8601DateFormatter().string(from: startedAt),
            "completed_at": ISO8601DateFormatter().string(from: Date()),
            "duration_ms": elapsed.seconds * 1_000 + elapsed.attoseconds / 1_000_000_000_000_000,
            "assertions": assertions,
            "menu_snapshot": menuSnapshot,
            "responder_snapshot": responderSnapshots,
            "system_ax_server": false,
            "voiceover_spoken_evidence": false,
            "keyboard_event_routing": false,
            "passed": passed,
        ]
        if let failure { value["failure"] = failure }
        return value
    }

    private var architecture: String {
        var info = utsname()
        uname(&info)
        return withUnsafeBytes(of: &info.machine) { bytes in
            String(decoding: bytes.prefix { $0 != 0 }, as: UTF8.self)
        }
    }

    private func wait(_ description: String, condition: @escaping @MainActor () -> Bool) async throws {
        while ContinuousClock.now < deadline {
            if condition() { return }
            try await Task.sleep(for: .milliseconds(20))
        }
        throw FixtureError.timeout("timed out waiting for \(description)")
    }

    private func record(_ id: String, expected: String, actual: String) {
        assertions.append(["id": id, "expected": expected, "actual": actual, "passed": true])
    }

    private func dispatch(_ item: NSMenuItem) throws {
        guard item.isEnabled, let action = item.action else {
            throw FixtureError.assertion("rendered menu item \(item.title) cannot dispatch")
        }
        guard NSApp.sendAction(action, to: item.target, from: item) else {
            throw FixtureError.assertion("rendered menu target/action rejected \(item.title)")
        }
    }

    private func focusedSearchField() -> NSSearchField? {
        guard window.isKeyWindow, let toolbar = window.toolbar else { return nil }
        let candidates = toolbar.items.compactMap { $0 as? NSSearchToolbarItem }
        guard candidates.count == 1 else { return nil }
        let field = candidates[0].searchField
        guard field.placeholderString == "Search sovereign memory",
              let editor = field.currentEditor(),
              window.firstResponder === editor,
              field.window === window else { return nil }
        return field
    }

    private func responderSnapshot(stage: String, field: NSSearchField) -> [String: Any] {
        [
            "stage": stage,
            "window_title": window.title,
            "window_is_key": window.isKeyWindow,
            "field_is_editable": field.isEditable,
            "field_window_matches": field.window === window,
            "field_editor_matches_first_responder": field.currentEditor() === window.firstResponder,
        ]
    }

    private func update(menu: NSMenu) {
        menu.update()
        for item in menu.items {
            if let submenu = item.submenu { update(menu: submenu) }
        }
    }

    private func uniqueMenuItem(
        in menu: NSMenu,
        parent: String,
        title: String,
        key: String,
        modifiers: NSEvent.ModifierFlags
    ) throws -> NSMenuItem {
        let matches = menuEntries(in: menu).filter {
            $0.path == [parent, title] && $0.item.title == title &&
                $0.item.keyEquivalent == key &&
                $0.item.keyEquivalentModifierMask.intersection(.deviceIndependentFlagsMask) == modifiers
        }
        guard matches.count == 1, let match = matches.first else {
            throw FixtureError.assertion("expected one rendered \(parent) > \(title) item; found \(matches.count)")
        }
        return match.item
    }

    private func menuDescription(_ item: NSMenuItem) -> String {
        "View > \(item.title) | \(modifierNames(item.keyEquivalentModifierMask))-\(item.keyEquivalent) | \(item.isEnabled ? "enabled" : "disabled")"
    }

    private func snapshot(entries: [(path: [String], item: NSMenuItem)]) -> [[String: Any]] {
        entries.map { entry in
            [
                "path": entry.path.joined(separator: " > "),
                "key": entry.item.keyEquivalent,
                "modifiers": modifierNames(entry.item.keyEquivalentModifierMask),
                "enabled": entry.item.isEnabled,
            ]
        }
    }

    private func menuEntries(in menu: NSMenu, path: [String] = []) -> [(path: [String], item: NSMenuItem)] {
        var result: [(path: [String], item: NSMenuItem)] = []
        for item in menu.items where !item.isSeparatorItem {
            let nextPath = path + [item.title]
            result.append((nextPath, item))
            if let submenu = item.submenu { result.append(contentsOf: menuEntries(in: submenu, path: nextPath)) }
        }
        return result
    }

    private func modifierNames(_ flags: NSEvent.ModifierFlags) -> String {
        let flags = flags.intersection(.deviceIndependentFlagsMask)
        return [
            flags.contains(.control) ? "control" : nil,
            flags.contains(.option) ? "option" : nil,
            flags.contains(.shift) ? "shift" : nil,
            flags.contains(.command) ? "command" : nil,
        ].compactMap(\.self).joined(separator: "+")
    }
}
#endif
