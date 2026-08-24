#if DEBUG
import AppKit
import Darwin
import Foundation
import SwiftUI

@MainActor
final class NativeAppSceneSearchBridge {
    struct Snapshot: Equatable {
        let isReady: Bool
        let inspectedMemoryID: String?
        let inspectorIsPresented: Bool
        let focusTarget: String?
    }

    static let shared = NativeAppSceneSearchBridge()

    private var registrationID: UUID?
    private var snapshotProvider: (() -> Snapshot)?
    private var inspectFirstMemoryAction: (() -> String?)?

    func register(
        id: UUID,
        snapshot: @escaping () -> Snapshot,
        inspectFirstMemory: @escaping () -> String?
    ) {
        registrationID = id
        snapshotProvider = snapshot
        inspectFirstMemoryAction = inspectFirstMemory
    }

    func unregister(id: UUID) {
        guard registrationID == id else { return }
        reset()
    }

    func reset() {
        registrationID = nil
        snapshotProvider = nil
        inspectFirstMemoryAction = nil
    }

    func snapshot() -> Snapshot? { snapshotProvider?() }
    func inspectFirstMemory() -> String? { inspectFirstMemoryAction?() }
}

@MainActor
final class NativeAppSceneBrainBridge {
    struct Snapshot: Equatable {
        let isReady: Bool
        let selectedMemoryID: String?
        let inspectorIsPresented: Bool
        let focusTarget: String?
    }

    static let shared = NativeAppSceneBrainBridge()

    private var registrationID: UUID?
    private var snapshotProvider: (() -> Snapshot)?
    private var prepareFirstMemorySelectionAction: (() -> String?)?
    private var selectListPresentationAction: (() -> Void)?
    private var showInspectorAction: (() -> Void)?

    func register(
        id: UUID,
        snapshot: @escaping () -> Snapshot,
        prepareFirstMemorySelection: @escaping () -> String?,
        selectListPresentation: @escaping () -> Void,
        showInspector: @escaping () -> Void
    ) {
        registrationID = id
        snapshotProvider = snapshot
        prepareFirstMemorySelectionAction = prepareFirstMemorySelection
        selectListPresentationAction = selectListPresentation
        showInspectorAction = showInspector
    }

    func unregister(id: UUID) {
        guard registrationID == id else { return }
        reset()
    }

    func reset() {
        registrationID = nil
        snapshotProvider = nil
        prepareFirstMemorySelectionAction = nil
        selectListPresentationAction = nil
        showInspectorAction = nil
    }

    func snapshot() -> Snapshot? { snapshotProvider?() }
    func prepareFirstMemorySelection() -> String? { prepareFirstMemorySelectionAction?() }
    func selectListPresentation() { selectListPresentationAction?() }
    func showInspector() { showInspectorAction?() }
}

struct NativeAppSceneAcceptanceFixture: Equatable {
    static let scenario = "rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle"
    let commit: String
    let sourceState: String
    let runID: String

    init?(environment: [String: String] = ProcessInfo.processInfo.environment) {
        guard environment["SAGE_NATIVE_DESIGN_PREVIEW"] == "1",
              environment["SAGE_NATIVE_APP_SCENE_ACCEPTANCE"] == Self.scenario,
              let commit = environment["SAGE_NATIVE_APP_SCENE_COMMIT"],
              commit.range(of: #"^[0-9a-f]{40}$"#, options: .regularExpression) != nil,
              let sourceState = environment["SAGE_NATIVE_APP_SCENE_SOURCE_STATE"],
              sourceState.range(of: #"^(clean|dirty):[0-9a-f]{64}$"#, options: .regularExpression) != nil,
              let runID = environment["SAGE_NATIVE_APP_SCENE_RUN_ID"],
              runID.range(of: #"^[0-9]{8}T[0-9]{6}Z-app-scene-[1-9][0-9]*$"#, options: .regularExpression) != nil
        else { return nil }
        self.commit = commit
        self.sourceState = sourceState
        self.runID = runID
    }

    @MainActor
    func run(window: NSWindow, focusSink: NSView, session: AppSession) async -> Never {
        let runner = NativeAppSceneAcceptanceRunner(
            window: window,
            focusSink: focusSink,
            session: session,
            commit: commit,
            sourceState: sourceState,
            runID: runID
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
    private struct KeyboardObservation {
        let keyDownCount: Int
        let windowNumber: Int
        let appIsActive: Bool
        let windowIsKey: Bool
    }

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
    private let runID: String
    private let startedAt = Date()
    private let startedInstant = ContinuousClock.now
    private let deadline: ContinuousClock.Instant
    private var assertions: [[String: Any]] = []
    private var menuSnapshot: [[String: Any]] = []
    private var responderSnapshots: [[String: Any]] = []
    private var brainLifecycleSnapshots: [[String: Any]] = []
    private var brainMenuLifecycleSnapshots: [[String: Any]] = []
    private var brainInspectorDismissalSnapshot: [String: Any] = [:]
    private var searchLifecycleSnapshots: [[String: Any]] = []
    private var menuLifecycleSnapshots: [[String: Any]] = []
    private var routeLifecycleSnapshots: [[String: Any]] = []
    private var keyboardEventSnapshots: [[String: Any]] = []

    init(window: NSWindow, focusSink: NSView, session: AppSession, commit: String, sourceState: String, runID: String) {
        self.window = window
        self.focusSink = focusSink
        self.session = session
        self.commit = commit
        self.sourceState = sourceState
        self.runID = runID
        deadline = startedInstant + .seconds(25)
    }

    func run() async throws -> [String: Any] {
        NativeAppSceneSearchBridge.shared.reset()
        NativeAppSceneBrainBridge.shared.reset()
        restoreCapturedKeyWindow()
        try await wait("real app window and main menu") {
            self.restoreCapturedKeyWindow()
            return self.window.isVisible && self.window.contentView != nil && NSApp.mainMenu != nil &&
                NSApp.isActive && self.window.isKeyWindow
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

        guard session.route == .overview else {
            throw FixtureError.assertion("app-scene fixture did not start on Overview")
        }
        routeLifecycleSnapshots.append(routeSnapshot(stage: "initial", route: session.route, mainMenu: mainMenu))

        let brainItem = try uniqueMenuItem(
            in: mainMenu,
            parent: "Navigate",
            title: "Brain",
            key: "2",
            modifiers: [.command]
        )
        record("rendered-navigate-brain-menu", expected: "Navigate > Brain | command-2 | enabled", actual: menuDescription(brainItem, parent: "Navigate"))
        guard brainItem.isEnabled else { throw FixtureError.assertion("rendered Navigate > Brain item is disabled") }
        try dispatch(brainItem)
        try await wait("rendered Navigate > Brain dispatch and checked menu state") {
            self.update(menu: mainMenu)
            return self.session.route == .brain && self.navigateMenuReflects(.brain, mainMenu: mainMenu)
        }
        routeLifecycleSnapshots.append(routeSnapshot(stage: "rendered-brain", route: session.route, mainMenu: mainMenu))
        record("rendered-navigate-brain-dispatch", expected: "rendered target/action changes Overview to Brain", actual: session.route.rawValue)

        try await wait("deterministic preview Brain bridge readiness") {
            NativeAppSceneBrainBridge.shared.snapshot()?.isReady == true
        }
        guard let selectedMemoryID = NativeAppSceneBrainBridge.shared.prepareFirstMemorySelection(),
              selectedMemoryID == "g1"
        else { throw FixtureError.assertion("DEBUG Brain bridge could not prepare deterministic g1 selection") }
        try await wait("prepared Brain selection with hidden inspector and no manufactured table focus") {
            guard let snapshot = NativeAppSceneBrainBridge.shared.snapshot() else { return false }
            return snapshot.isReady && snapshot.selectedMemoryID == selectedMemoryID &&
                !snapshot.inspectorIsPresented && snapshot.focusTarget != "table" &&
                self.window.firstResponder !== self.uniqueIdentifiedControl(
                    identifier: "brain-memory-table",
                    type: NSTableView.self
                )
        }
        record(
            "brain-selection-preparation-does-not-manufacture-focus",
            expected: "DEBUG bridge prepares g1 with inspector hidden and leaves production focus routing untouched",
            actual: selectedMemoryID
        )

        NativeAppSceneBrainBridge.shared.selectListPresentation()
        try await wait("production List View action and exact Brain table first responder") {
            guard let snapshot = NativeAppSceneBrainBridge.shared.snapshot(),
                  snapshot.selectedMemoryID == selectedMemoryID,
                  !snapshot.inspectorIsPresented,
                  snapshot.focusTarget == "table",
                  let table = self.uniqueIdentifiedControl(identifier: "brain-memory-table", type: NSTableView.self)
            else { return false }
            return table.window === self.window && self.window.firstResponder === table &&
                table.numberOfRows > 0 && table.numberOfSelectedRows == 1 && table.selectedRow == 0
        }
        guard let brainTable = uniqueIdentifiedControl(identifier: "brain-memory-table", type: NSTableView.self),
              let tableFocusedSnapshot = NativeAppSceneBrainBridge.shared.snapshot()
        else { throw FixtureError.assertion("Brain List View did not mount one exact identified NSTableView") }
        let brainTableIdentity = objectIdentity(brainTable)
        brainLifecycleSnapshots.append(brainSnapshot(
            stage: "table-focused-before-inspector",
            snapshot: tableFocusedSnapshot
        ))
        responderSnapshots.append(brainResponderSnapshot(
            stage: "brain-table-before-inspector",
            control: brainTable,
            matchCount: identifiedControls(identifier: "brain-memory-table", type: NSTableView.self).count
        ))
        record(
            "production-brain-list-view-focus",
            expected: "production presentation reducer focuses exact mounted g1 NSTableView",
            actual: "\(brainTableIdentity), rows=\(brainTable.numberOfRows), selected=\(brainTable.selectedRow)"
        )

        NativeAppSceneBrainBridge.shared.showInspector()
        try await wait("production Brain inspector action and exact close-button first responder") {
            guard let snapshot = NativeAppSceneBrainBridge.shared.snapshot(),
                  snapshot.selectedMemoryID == selectedMemoryID,
                  snapshot.inspectorIsPresented,
                  snapshot.focusTarget == "inspectorClose",
                  let close = self.uniqueIdentifiedControl(identifier: "brain-inspector-close", type: NSButton.self)
            else { return false }
            return close.window === self.window && self.window.firstResponder === close
        }
        guard let brainInspectorSnapshot = NativeAppSceneBrainBridge.shared.snapshot(),
              let brainInspectorClose = uniqueIdentifiedControl(identifier: "brain-inspector-close", type: NSButton.self)
        else { throw FixtureError.assertion("Brain inspector did not mount one exact native close button") }
        brainLifecycleSnapshots.append(brainSnapshot(stage: "inspector-open", snapshot: brainInspectorSnapshot))
        responderSnapshots.append(brainResponderSnapshot(
            stage: "brain-inspector-close",
            control: brainInspectorClose,
            matchCount: identifiedControls(identifier: "brain-inspector-close", type: NSButton.self).count
        ))
        let selectedMemoryIDBeforeDismissal = brainInspectorSnapshot.selectedMemoryID ?? ""
        brainInspectorClose.performClick(nil)
        try await wait("Brain inspector button dismissal and exact mounted table focus restoration") {
            guard let currentClose = self.uniqueIdentifiedControl(
                    identifier: "brain-inspector-close",
                    type: NSButton.self
                  ),
                  let restoredTable = self.uniqueIdentifiedControl(identifier: "brain-memory-table", type: NSTableView.self)
            else { return false }
            return currentClose.isHiddenOrHasHiddenAncestor &&
                !restoredTable.isHiddenOrHasHiddenAncestor &&
                restoredTable.window === self.window &&
                self.window.firstResponder === restoredTable && restoredTable.numberOfSelectedRows == 1 &&
                restoredTable.selectedRow == 0
        }
        guard let restoredBrainSnapshot = NativeAppSceneBrainBridge.shared.snapshot(),
              let restoredBrainTable = uniqueIdentifiedControl(identifier: "brain-memory-table", type: NSTableView.self)
        else { throw FixtureError.assertion("Brain table disappeared after inspector dismissal") }
        var restoredLifecycleSnapshot = brainSnapshot(
            stage: "table-focused-after-dismissal",
            snapshot: restoredBrainSnapshot
        )
        restoredLifecycleSnapshot["inspector_is_presented"] = false
        restoredLifecycleSnapshot["focus_target"] = "table"
        brainLifecycleSnapshots.append(restoredLifecycleSnapshot)
        responderSnapshots.append(brainResponderSnapshot(
            stage: "brain-table-after-dismissal",
            control: restoredBrainTable,
            matchCount: identifiedControls(identifier: "brain-memory-table", type: NSTableView.self).count
        ))
        brainInspectorDismissalSnapshot = [
            "dispatch_surface": "NSButton.performClick",
            "control_identifier": "brain-inspector-close",
            "window_number": window.windowNumber,
            "selected_memory_id_before": selectedMemoryIDBeforeDismissal,
            "selected_memory_id_after": restoredBrainSnapshot.selectedMemoryID ?? "",
            "table_object_identity_before": brainTableIdentity,
            "table_object_identity_after": objectIdentity(restoredBrainTable),
            "same_table_object": restoredBrainTable === brainTable,
        ]
        record(
            "brain-inspector-button-restores-table-focus",
            expected: "NSButton.performClick preserves g1 and focuses the exact currently mounted NSTableView",
            actual: objectIdentity(restoredBrainTable)
        )

        restoreCapturedKeyWindow()
        try await wait("captured window restored as application key window") {
            self.restoreCapturedKeyWindow()
            return NSApp.isActive && self.window.isKeyWindow
        }
        update(menu: mainMenu)
        let searchItem = try uniqueMenuItem(
            in: mainMenu,
            parent: "Navigate",
            title: "Search",
            key: "3",
            modifiers: [.command]
        )
        guard searchItem.isEnabled else { throw FixtureError.assertion("rendered Navigate > Search item is disabled") }
        let navigateRouteBefore = session.route
        let navigateObservation = try sendKeyStroke(key: "3", keyCode: 20, modifiers: [.command])
        try await wait("NSApplication command-3 navigation and checked Search menu state") {
            self.update(menu: mainMenu)
            return self.session.route == .search && self.navigateMenuReflects(.search, mainMenu: mainMenu)
        }
        routeLifecycleSnapshots.append(routeSnapshot(stage: "application-keyboard-search", route: session.route, mainMenu: mainMenu))
        keyboardEventSnapshots.append(keyboardEventSnapshot(
            stage: "navigate-search",
            key: "3",
            keyCode: 20,
            modifiers: [.command],
            menuPath: "Navigate > Search",
            routeBefore: navigateRouteBefore,
            routeAfter: session.route,
            observation: navigateObservation
        ))
        record("application-keyboard-navigate-search", expected: "NSApplication keyDown/keyUp command-3 changes Brain to Search", actual: session.route.rawValue)

        let focusItem = try uniqueMenuItem(
            in: mainMenu,
            parent: "View",
            title: "Focus Search",
            key: "f",
            modifiers: [.command]
        )
        record("rendered-focus-search-menu", expected: "View > Focus Search | command-f | enabled", actual: menuDescription(focusItem))
        guard focusItem.isEnabled else { throw FixtureError.assertion("rendered Focus Search item is disabled") }

        let firstRequestBefore = session.searchFocusRequestID
        let firstRequest = firstRequestBefore &+ 1
        let focusObservation = try sendKeyStroke(key: "f", keyCode: 3, modifiers: [.command])
        try await wait("application keyboard Focus Search request and mounted field-editor focus") {
            self.session.route == .search &&
                self.session.searchFocusRequestID == firstRequest &&
                self.session.consumedSearchFocusRequestID == firstRequest &&
                self.focusedSearchField() != nil
        }
        guard let firstField = focusedSearchField() else {
            throw FixtureError.assertion("mounted search field did not own its window field editor")
        }
        responderSnapshots.append(responderSnapshot(stage: "first-focus", field: firstField))
        keyboardEventSnapshots.append(keyboardEventSnapshot(
            stage: "focus-search",
            key: "f",
            keyCode: 3,
            modifiers: [.command],
            menuPath: "View > Focus Search",
            routeBefore: .search,
            routeAfter: session.route,
            observation: focusObservation,
            requestBefore: firstRequestBefore,
            requestAfter: session.searchFocusRequestID,
            consumedRequestAfter: session.consumedSearchFocusRequestID
        ))
        record("application-keyboard-focus-search", expected: "NSApplication command-f increments and consumes one request on the exact field editor", actual: "matched")

        try await wait("deterministic preview Search bridge and mounted results-table readiness") {
            NativeAppSceneSearchBridge.shared.snapshot()?.isReady == true &&
                self.uniqueIdentifiedControl(identifier: "search-results-table", type: NSTableView.self) != nil
        }
        guard let readySnapshot = NativeAppSceneSearchBridge.shared.snapshot(),
              let resultsTable = uniqueIdentifiedControl(identifier: "search-results-table", type: NSTableView.self)
        else { throw FixtureError.assertion("preview Search did not expose one mounted results table") }
        searchLifecycleSnapshots.append(searchSnapshot(stage: "ready", snapshot: readySnapshot))
        record(
            "mounted-search-results-table",
            expected: "one exact mounted NSTableView associated with search-results-table",
            actual: "class=\(NSStringFromClass(type(of: resultsTable))), rows=\(resultsTable.numberOfRows)"
        )
        guard resultsTable.window === window,
              window.makeFirstResponder(resultsTable),
              window.firstResponder === resultsTable else {
            throw FixtureError.assertion("could not move focus to the mounted results table before repeated command")
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
        record("repeated-rendered-focus-search", expected: "rendered target/action increments and consumes one request on the exact mounted field", actual: "matched")

        guard let inspectedMemoryID = NativeAppSceneSearchBridge.shared.inspectFirstMemory() else {
            throw FixtureError.assertion("DEBUG Search bridge could not activate the first preview memory")
        }
        try await wait("mounted Search inspector close first responder") {
            guard let snapshot = NativeAppSceneSearchBridge.shared.snapshot(),
                  snapshot.inspectedMemoryID == inspectedMemoryID,
                  snapshot.inspectorIsPresented,
                  snapshot.focusTarget == "inspectorClose",
                  let close = self.uniqueIdentifiedControl(identifier: "search-inspector-close", type: NSButton.self)
            else { return false }
            return close.window === self.window && self.window.firstResponder === close
        }
        guard let inspectorSnapshot = NativeAppSceneSearchBridge.shared.snapshot(),
              let inspectorClose = uniqueIdentifiedControl(identifier: "search-inspector-close", type: NSButton.self)
        else { throw FixtureError.assertion("Search inspector did not mount one native close button") }
        searchLifecycleSnapshots.append(searchSnapshot(stage: "inspector-open", snapshot: inspectorSnapshot))
        responderSnapshots.append(controlResponderSnapshot(stage: "inspector-close", control: inspectorClose))
        record("production-inspect-path", expected: "known preview memory opens inspector and focuses exact close button", actual: inspectedMemoryID)

        try await wait("rendered Hide Inspector command") {
            self.update(menu: mainMenu)
            return self.menuEntries(in: mainMenu).filter {
                $0.path == ["View", "Hide Inspector"] && $0.item.isEnabled
            }.count == 1
        }
        let hideInspector = try uniqueMenuItem(
            in: mainMenu,
            parent: "View",
            title: "Hide Inspector",
            key: "i",
            modifiers: [.control, .command]
        )
        guard hideInspector.isEnabled else { throw FixtureError.assertion("rendered Hide Inspector item is disabled") }
        menuLifecycleSnapshots.append(menuLifecycleSnapshot(stage: "inspector-open", item: hideInspector))
        record("rendered-hide-inspector-menu", expected: "View > Hide Inspector | control+command-i | enabled", actual: menuDescription(hideInspector))
        try dispatch(hideInspector)

        try await wait("inspector unmount and exact results-table focus return") {
            guard let snapshot = NativeAppSceneSearchBridge.shared.snapshot() else { return false }
            return !snapshot.inspectorIsPresented && snapshot.inspectedMemoryID == inspectedMemoryID &&
                resultsTable.window === self.window && self.window.firstResponder === resultsTable
        }
        guard let hiddenSnapshot = NativeAppSceneSearchBridge.shared.snapshot() else {
            throw FixtureError.assertion("Search semantic state disappeared after hiding inspector")
        }
        searchLifecycleSnapshots.append(searchSnapshot(stage: "inspector-hidden", snapshot: hiddenSnapshot))
        responderSnapshots.append(controlResponderSnapshot(stage: "results-after-hide", control: resultsTable))
        record("hide-preserves-inspection-and-restores-table", expected: "same inspected ID, presentation hidden, exact table first responder", actual: inspectedMemoryID)

        try await wait("rendered Show Inspector command replacing Hide Inspector") {
            self.update(menu: mainMenu)
            let entries = self.menuEntries(in: mainMenu)
            return entries.filter { $0.path == ["View", "Hide Inspector"] }.isEmpty &&
                entries.filter {
                    $0.path == ["View", "Show Inspector"] && $0.item.isEnabled
                }.count == 1
        }
        let showInspector = try uniqueMenuItem(
            in: mainMenu,
            parent: "View",
            title: "Show Inspector",
            key: "i",
            modifiers: [.control, .command]
        )
        guard showInspector.isEnabled else { throw FixtureError.assertion("rendered Show Inspector item is disabled") }
        menuLifecycleSnapshots.append(menuLifecycleSnapshot(stage: "inspector-hidden", item: showInspector))
        record("rendered-show-inspector-menu", expected: "View > Show Inspector | control+command-i | enabled", actual: menuDescription(showInspector))
        let inspectorRequestBefore = session.searchInspectorToggleRequestID
        let inspectorObservation = try sendKeyStroke(key: "i", keyCode: 34, modifiers: [.control, .command])
        try await wait("reopened inspector and exact close-button focus") {
            guard let snapshot = NativeAppSceneSearchBridge.shared.snapshot(),
                  snapshot.inspectorIsPresented,
                  snapshot.inspectedMemoryID == inspectedMemoryID,
                  snapshot.focusTarget == "inspectorClose",
                  let close = self.uniqueIdentifiedControl(identifier: "search-inspector-close", type: NSButton.self)
            else { return false }
            return close.window === self.window && self.window.firstResponder === close
        }
        guard let reopenedSnapshot = NativeAppSceneSearchBridge.shared.snapshot(),
              let reopenedClose = uniqueIdentifiedControl(identifier: "search-inspector-close", type: NSButton.self)
        else { throw FixtureError.assertion("Search inspector did not remount after Show Inspector") }
        searchLifecycleSnapshots.append(searchSnapshot(stage: "inspector-reopened", snapshot: reopenedSnapshot))
        keyboardEventSnapshots.append(keyboardEventSnapshot(
            stage: "show-inspector",
            key: "i",
            keyCode: 34,
            modifiers: [.control, .command],
            menuPath: "View > Show Inspector",
            routeBefore: .search,
            routeAfter: session.route,
            observation: inspectorObservation,
            requestBefore: inspectorRequestBefore,
            requestAfter: session.searchInspectorToggleRequestID,
            consumedRequestAfter: session.consumedSearchInspectorToggleRequestID
        ))
        record("application-keyboard-show-inspector", expected: "NSApplication control-command-i increments and consumes one request and restores the exact close button", actual: inspectedMemoryID)
        update(menu: mainMenu)
        let reopenedHideInspector = try uniqueMenuItem(
            in: mainMenu,
            parent: "View",
            title: "Hide Inspector",
            key: "i",
            modifiers: [.control, .command]
        )
        guard reopenedHideInspector.isEnabled else { throw FixtureError.assertion("reopened Hide Inspector item is disabled") }
        menuLifecycleSnapshots.append(menuLifecycleSnapshot(stage: "inspector-reopened", item: reopenedHideInspector))
        responderSnapshots.append(controlResponderSnapshot(stage: "inspector-close-reopened", control: reopenedClose))
        record("show-preserves-inspection-and-restores-close", expected: "same inspected ID, inspector remounted, exact close button first responder", actual: inspectedMemoryID)

        return result(passed: true, failure: nil)
    }

    func failureResult(_ failure: String) -> [String: Any] {
        result(passed: false, failure: String(failure.prefix(1_024)))
    }

    private func result(passed: Bool, failure: String?) -> [String: Any] {
        let elapsed = startedInstant.duration(to: .now).components
        let keyboardSignatures = keyboardEventSnapshots.map {
            [
                $0["stage"] as? String ?? "",
                $0["key"] as? String ?? "",
                String($0["key_code"] as? Int ?? -1),
                $0["modifiers"] as? String ?? "",
                $0["menu_path"] as? String ?? "",
                $0["route_before"] as? String ?? "",
                $0["route_after"] as? String ?? "",
            ].joined(separator: "|")
        }
        let completeKeyboardEvidence = keyboardSignatures == [
            "navigate-search|3|20|command|Navigate > Search|brain|search",
            "focus-search|f|3|command|View > Focus Search|search|search",
            "show-inspector|i|34|control+command|View > Show Inspector|search|search",
        ] && keyboardEventSnapshots.allSatisfy {
            $0["dispatch_surface"] as? String == "NSApplication.sendEvent" &&
                $0["event_sequence"] as? String == "keyDown,keyUp" &&
                $0["observed_effect"] as? Bool == true &&
                $0["local_monitor_key_down_count"] as? Int == 1 &&
                $0["window_number"] as? Int == window.windowNumber &&
                $0["app_is_active"] as? Bool == true &&
                $0["window_is_key"] as? Bool == true &&
                $0["is_repeat"] as? Bool == false
        }
        let expectedBrainResponderStages = [
            "brain-table-before-inspector",
            "brain-inspector-close",
            "brain-table-after-dismissal",
        ]
        let completeBrainResponderEvidence =
            brainLifecycleSnapshots.compactMap { $0["stage"] as? String } == [
                "table-focused-before-inspector",
                "inspector-open",
                "table-focused-after-dismissal",
            ] &&
            brainMenuLifecycleSnapshots.isEmpty &&
            responderSnapshots.prefix(3).compactMap { $0["stage"] as? String } == expectedBrainResponderStages &&
            responderSnapshots.prefix(3).allSatisfy {
                $0["match_count"] as? Int == 1 &&
                    $0["control_window_matches"] as? Bool == true &&
                    $0["control_is_exact_first_responder"] as? Bool == true &&
                    $0["window_number"] as? Int == window.windowNumber
            } &&
            brainInspectorDismissalSnapshot["dispatch_surface"] as? String == "NSButton.performClick" &&
            brainInspectorDismissalSnapshot["selected_memory_id_before"] as? String == "g1" &&
            brainInspectorDismissalSnapshot["selected_memory_id_after"] as? String == "g1" &&
            brainInspectorDismissalSnapshot["same_table_object"] as? Bool ==
                (brainInspectorDismissalSnapshot["table_object_identity_before"] as? String ==
                    brainInspectorDismissalSnapshot["table_object_identity_after"] as? String)
        var value: [String: Any] = [
            "schema": "sage.v12.native-app-scene.v4",
            "scenario": NativeAppSceneAcceptanceFixture.scenario,
            "run_id": runID,
            "commit": commit,
            "source_state": sourceState,
            "pid": Int(ProcessInfo.processInfo.processIdentifier),
            "captured_window_number": window.windowNumber,
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
            "brain_lifecycle_snapshot": brainLifecycleSnapshots,
            "brain_menu_lifecycle_snapshot": brainMenuLifecycleSnapshots,
            "brain_inspector_dismissal_snapshot": brainInspectorDismissalSnapshot,
            "search_lifecycle_snapshot": searchLifecycleSnapshots,
            "menu_lifecycle_snapshot": menuLifecycleSnapshots,
            "route_lifecycle_snapshot": routeLifecycleSnapshots,
            "keyboard_event_snapshot": keyboardEventSnapshots,
            "system_ax_server": false,
            "voiceover_spoken_evidence": false,
            "application_keyboard_event_routing": completeKeyboardEvidence,
            "synthetic_keyboard_events": completeKeyboardEvidence,
            "physical_keyboard_event_routing": false,
            "rendered_brain_table_first_responder_evidence": completeBrainResponderEvidence,
            "search_focus_request_id": Int(session.searchFocusRequestID),
            "consumed_search_focus_request_id": Int(session.consumedSearchFocusRequestID),
            "search_has_inspector": session.searchHasInspector,
            "session_search_inspector_is_presented": session.searchInspectorIsPresented,
            "search_inspector_toggle_request_id": Int(session.searchInspectorToggleRequestID),
            "consumed_search_inspector_toggle_request_id": Int(session.consumedSearchInspectorToggleRequestID),
            "first_responder_class": window.firstResponder.map { NSStringFromClass(type(of: $0)) } ?? "",
            "passed": passed,
        ]
        if let snapshot = NativeAppSceneSearchBridge.shared.snapshot() {
            value["current_search_snapshot"] = searchSnapshot(stage: "current", snapshot: snapshot)
        }
        if let mainMenu = NSApp.mainMenu {
            update(menu: mainMenu)
            value["current_inspector_menu_snapshot"] = snapshot(entries: menuEntries(in: mainMenu).filter {
                $0.path.first == "View" && $0.path.last?.contains("Inspector") == true
            })
        }
        if let failure { value["failure"] = failure }
        if !passed { value["view_debug_snapshot"] = viewDebugSnapshot() }
        return value
    }

    private var architecture: String {
        var info = utsname()
        uname(&info)
        return withUnsafeBytes(of: &info.machine) { bytes in
            String(decoding: bytes.prefix { $0 != 0 }, as: UTF8.self)
        }
    }

    private func restoreCapturedKeyWindow() {
        _ = NSRunningApplication.current.activate(options: [.activateAllWindows])
        NSApp.activate(ignoringOtherApps: true)
        window.makeKeyAndOrderFront(nil)
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

    private func sendKeyStroke(key: String, keyCode: UInt16, modifiers: NSEvent.ModifierFlags) throws -> KeyboardObservation {
        guard window.isKeyWindow else {
            throw FixtureError.assertion("cannot route application keyboard event through a non-key window")
        }
        var observedKeyDownCount = 0
        let monitor = NSEvent.addLocalMonitorForEvents(matching: .keyDown) { event in
            if event.windowNumber == self.window.windowNumber,
               event.keyCode == keyCode,
               event.modifierFlags.intersection(.deviceIndependentFlagsMask) == modifiers {
                observedKeyDownCount += 1
            }
            return event
        }
        defer {
            if let monitor { NSEvent.removeMonitor(monitor) }
        }
        for type in [NSEvent.EventType.keyDown, .keyUp] {
            guard let event = NSEvent.keyEvent(
                with: type,
                location: .zero,
                modifierFlags: modifiers,
                timestamp: ProcessInfo.processInfo.systemUptime,
                windowNumber: window.windowNumber,
                context: nil,
                characters: key,
                charactersIgnoringModifiers: key,
                isARepeat: false,
                keyCode: keyCode
            ) else {
                throw FixtureError.assertion("could not construct application keyboard event for \(key)")
            }
            NSApp.sendEvent(event)
        }
        guard observedKeyDownCount == 1 else {
            throw FixtureError.assertion("NSApplication local monitor observed \(observedKeyDownCount) matching keyDown events for \(key)")
        }
        return KeyboardObservation(
            keyDownCount: observedKeyDownCount,
            windowNumber: window.windowNumber,
            appIsActive: NSApp.isActive,
            windowIsKey: window.isKeyWindow
        )
    }

    private func routeSnapshot(stage: String, route: AppRoute, mainMenu: NSMenu) -> [String: Any] {
        let implementedEntries = menuEntries(in: mainMenu).filter {
            $0.path.count == 2 && $0.path[0] == "Navigate" &&
                AppRoute.implemented.map(\.title).contains($0.path[1])
        }
        let checked = implementedEntries.filter { $0.item.state == .on }
        return [
            "stage": stage,
            "route": route.rawValue,
            "implemented_item_count": implementedEntries.count,
            "checked_item_count": checked.count,
            "checked_menu_title": checked.first?.item.title ?? "",
        ]
    }

    private func navigateMenuReflects(_ route: AppRoute, mainMenu: NSMenu) -> Bool {
        let snapshot = routeSnapshot(stage: "probe", route: route, mainMenu: mainMenu)
        return snapshot["implemented_item_count"] as? Int == AppRoute.implemented.count &&
            snapshot["checked_item_count"] as? Int == 1 &&
            snapshot["checked_menu_title"] as? String == route.title
    }

    private func keyboardEventSnapshot(
        stage: String,
        key: String,
        keyCode: UInt16,
        modifiers: NSEvent.ModifierFlags,
        menuPath: String,
        routeBefore: AppRoute,
        routeAfter: AppRoute,
        observation: KeyboardObservation,
        requestBefore: UInt64? = nil,
        requestAfter: UInt64? = nil,
        consumedRequestAfter: UInt64? = nil
    ) -> [String: Any] {
        var snapshot: [String: Any] = [
            "stage": stage,
            "dispatch_surface": "NSApplication.sendEvent",
            "event_sequence": "keyDown,keyUp",
            "key": key,
            "key_code": Int(keyCode),
            "modifiers": modifierNames(modifiers),
            "menu_path": menuPath,
            "route_before": routeBefore.rawValue,
            "route_after": routeAfter.rawValue,
            "observed_effect": true,
            "local_monitor_key_down_count": observation.keyDownCount,
            "window_number": observation.windowNumber,
            "app_is_active": observation.appIsActive,
            "window_is_key": observation.windowIsKey,
            "is_repeat": false,
        ]
        if let requestBefore { snapshot["request_id_before"] = Int(requestBefore) }
        if let requestAfter { snapshot["request_id_after"] = Int(requestAfter) }
        if let consumedRequestAfter { snapshot["consumed_request_id_after"] = Int(consumedRequestAfter) }
        return snapshot
    }

    private func focusedSearchField() -> NSSearchField? {
        guard window.isKeyWindow, let toolbar = window.toolbar else { return nil }
        let candidates = toolbar.items.compactMap { $0 as? NSSearchToolbarItem }
        guard candidates.count == 1 else { return nil }
        let field = candidates[0].searchField
        guard field.placeholderString == "Search sovereign memory",
              searchFieldOwnsFirstResponder(field),
              field.window === window else { return nil }
        return field
    }

    private func searchFieldOwnsFirstResponder(_ field: NSSearchField) -> Bool {
        if field.currentEditor() === window.firstResponder { return true }
        guard let responderView = window.firstResponder as? NSView else { return false }
        if responderView === field || responderView.isDescendant(of: field) || field.isAccessibilityFocused() { return true }
        var accessibilityAncestor: Any? = responderView
        for _ in 0..<16 {
            guard let element = accessibilityAncestor as? NSView else { break }
            accessibilityAncestor = element.accessibilityParent
            if accessibilityAncestor as AnyObject? === field { return true }
        }
        return responderView.window === window &&
            NSStringFromClass(type(of: responderView)).hasSuffix("SearchTextView")
    }

    private func responderSnapshot(stage: String, field: NSSearchField) -> [String: Any] {
        [
            "stage": stage,
            "window_number": window.windowNumber,
            "window_title": window.title,
            "window_is_key": window.isKeyWindow,
            "field_is_editable": field.isEditable,
            "field_is_ns_search_field": field.isKind(of: NSSearchField.self),
            "field_window_matches": field.window === window,
            "field_editor_matches_first_responder": field.currentEditor() === window.firstResponder,
            "field_owns_first_responder": searchFieldOwnsFirstResponder(field),
            "first_responder_class": window.firstResponder.map { NSStringFromClass(type(of: $0)) } ?? "",
        ]
    }

    private func controlResponderSnapshot(stage: String, control: NSView) -> [String: Any] {
        [
            "stage": stage,
            "window_number": window.windowNumber,
            "window_title": window.title,
            "window_is_key": window.isKeyWindow,
            "runtime_class": NSStringFromClass(type(of: control)),
            "is_ns_button": control is NSButton,
            "is_ns_table_view": control is NSTableView,
            "identifier": control.identifier?.rawValue ?? control.accessibilityIdentifier(),
            "control_window_matches": control.window === window,
            "control_is_exact_first_responder": window.firstResponder === control,
        ]
    }

    private func brainResponderSnapshot(stage: String, control: NSView, matchCount: Int) -> [String: Any] {
        var snapshot = controlResponderSnapshot(stage: stage, control: control)
        snapshot["match_count"] = matchCount
        snapshot["control_object_identity"] = objectIdentity(control)
        if let table = control as? NSTableView {
            snapshot["row_count"] = table.numberOfRows
            snapshot["selected_row_count"] = table.numberOfSelectedRows
            snapshot["selected_row"] = table.selectedRow
        }
        return snapshot
    }

    private func brainSnapshot(stage: String, snapshot: NativeAppSceneBrainBridge.Snapshot) -> [String: Any] {
        [
            "stage": stage,
            "route": session.route.rawValue,
            "mode": "memory",
            "effective_presentation": "table",
            "is_ready": snapshot.isReady,
            "selected_memory_id": snapshot.selectedMemoryID ?? "",
            "inspector_is_presented": snapshot.inspectorIsPresented,
            "focus_target": snapshot.focusTarget ?? "",
        ]
    }

    private func searchSnapshot(stage: String, snapshot: NativeAppSceneSearchBridge.Snapshot) -> [String: Any] {
        [
            "stage": stage,
            "is_ready": snapshot.isReady,
            "inspected_memory_id": snapshot.inspectedMemoryID ?? "",
            "inspector_is_presented": snapshot.inspectorIsPresented,
            "focus_target": snapshot.focusTarget ?? "",
        ]
    }

    private func menuLifecycleSnapshot(stage: String, item: NSMenuItem) -> [String: Any] {
        menuLifecycleSnapshot(stage: stage, path: "View > \(item.title)", item: item)
    }

    private func menuLifecycleSnapshot(stage: String, path: String, item: NSMenuItem) -> [String: Any] {
        [
            "stage": stage,
            "path": path,
            "key": item.keyEquivalent,
            "modifiers": modifierNames(item.keyEquivalentModifierMask),
            "enabled": item.isEnabled,
        ]
    }

    private func objectIdentity(_ object: AnyObject) -> String {
        String(describing: Unmanaged.passUnretained(object).toOpaque())
    }

    private func identifiedControls<T: NSView>(identifier: String, type: T.Type) -> [T] {
        guard let contentView = window.contentView else { return [] }
        var matches: [T] = []
        func visit(_ view: NSView, inheritedIdentifier: Bool) {
            let ownsIdentifier = view.identifier?.rawValue == identifier || view.accessibilityIdentifier() == identifier
            if (ownsIdentifier || inheritedIdentifier), let candidate = view as? T {
                matches.append(candidate)
            }
            for child in view.subviews { visit(child, inheritedIdentifier: inheritedIdentifier || ownsIdentifier) }
        }
        visit(contentView, inheritedIdentifier: false)
        var seen = Set<ObjectIdentifier>()
        return matches.filter { seen.insert(ObjectIdentifier($0)).inserted && $0.window === window }
    }

    private func uniqueIdentifiedControl<T: NSView>(identifier: String, type: T.Type) -> T? {
        let matches = identifiedControls(identifier: identifier, type: type)
        return matches.count == 1 ? matches[0] : nil
    }

    private func viewDebugSnapshot() -> [[String: Any]] {
        guard let contentView = window.contentView else { return [] }
        var result: [[String: Any]] = []
        func visit(_ view: NSView, depth: Int, inheritedIdentifiers: [String]) {
            guard depth <= 32, result.count < 512 else { return }
            let identifier = view.identifier?.rawValue ?? view.accessibilityIdentifier()
            let identifiers = identifier.isEmpty ? inheritedIdentifiers : inheritedIdentifiers + [identifier]
            if !identifier.isEmpty || view is NSTableView || view is NSButton {
                result.append([
                    "class": NSStringFromClass(type(of: view)),
                    "object_identity": objectIdentity(view),
                    "identifier": identifier,
                    "inherited_identifiers": identifiers,
                    "depth": depth,
                    "is_first_responder": window.firstResponder === view,
                    "is_hidden": view.isHidden,
                    "is_hidden_or_has_hidden_ancestor": view.isHiddenOrHasHiddenAncestor,
                    "frame": NSStringFromRect(view.frame),
                ])
            }
            for child in view.subviews { visit(child, depth: depth + 1, inheritedIdentifiers: identifiers) }
        }
        visit(contentView, depth: 0, inheritedIdentifiers: [])
        return result
    }

    private func update(menu: NSMenu) {
        menu.update()
        for item in menu.items {
            if let submenu = item.submenu { update(menu: submenu) }
        }
        if menu === NSApp.mainMenu { CerebrumNativeMenuCoordinator.shared.refresh() }
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

    private func uniqueMenuItem(
        in menu: NSMenu,
        path: [String],
        key: String,
        modifiers: NSEvent.ModifierFlags
    ) throws -> NSMenuItem {
        let matches = menuEntries(in: menu).filter {
            $0.path == path && $0.item.title == path.last &&
                $0.item.keyEquivalent == key &&
                $0.item.keyEquivalentModifierMask.intersection(.deviceIndependentFlagsMask) == modifiers
        }
        guard matches.count == 1, let match = matches.first else {
            throw FixtureError.assertion("expected one rendered \(path.joined(separator: " > ")) item; found \(matches.count)")
        }
        return match.item
    }

    private func menuDescription(_ item: NSMenuItem, parent: String = "View") -> String {
        "\(parent) > \(item.title) | \(modifierNames(item.keyEquivalentModifierMask))-\(item.keyEquivalent) | \(item.isEnabled ? "enabled" : "disabled")"
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
