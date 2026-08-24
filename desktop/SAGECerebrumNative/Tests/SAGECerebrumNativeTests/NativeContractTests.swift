import AppKit
import Foundation
import MetalKit
import SwiftUI
import Testing
@testable import SAGECerebrumNative

@Test func routeInventoryAndAvailabilityStayExplicit() {
    #expect(AppRoute.allCases.map(\.rawValue) == [
        "overview", "brain", "search", "tasks", "import",
        "network", "access", "federation", "settings",
    ])
    #expect(Set(AppRoute.allCases.map(\.cerebrumHash)).count == 9)
    #expect(AppRoute.implemented == [.overview, .brain, .search])
    #expect(AppRoute.implemented.compactMap(\.navigationShortcut) == ["1", "2", "3"])
    #expect(Set(AppRoute.implemented.compactMap(\.navigationShortcut)).count == 3)
    #expect(AppRoute.allCases.filter { !$0.isImplemented }.allSatisfy { $0.navigationShortcut == nil })
    #expect(AppRoute.settings.isImplemented == false)
}

@MainActor
@Test func nativeCommandsAreAvailableOnlyForAReadySession() {
    let session = AppSession()
    for phase in [
        AppSession.Phase.connecting,
        .locked,
        .failed("Unavailable"),
    ] {
        session.phase = phase
        #expect(session.acceptsReadyCommands == false)
    }
    session.phase = .ready
    #expect(session.acceptsReadyCommands)
}

@MainActor
@Test func focusSearchIsReadyGatedAndConsumedExactlyOnce() {
    let session = AppSession(previewAPI: MutationTestAPI(forgetResults: []))
    session.phase = .locked
    session.route = .brain
    session.focusSearch()
    #expect(session.route == .brain)
    #expect(session.searchFocusRequestID == 0)

    session.phase = .ready
    session.focusSearch()
    #expect(session.route == .search)
    #expect(session.searchFocusRequestID == 1)
    #expect(session.consumedSearchFocusRequestID == 0)
    session.consumeSearchFocusRequest(1)
    #expect(session.consumedSearchFocusRequestID == 1)
    session.focusSearch()
    #expect(session.searchFocusRequestID == 2)
    session.consumeSearchFocusRequest(1)
    #expect(session.consumedSearchFocusRequestID == 1)
    session.consumeSearchFocusRequest(2)
    #expect(session.consumedSearchFocusRequestID == 2)
}

@MainActor
@Test func searchInspectorToggleIsRouteAndStateGatedAndConsumedExactlyOnce() {
    let session = AppSession(previewAPI: MutationTestAPI(forgetResults: []))
    session.updateSearchInspectorCommandState(hasInspector: true, isPresented: true, commandsBlocked: false)

    session.requestSearchInspectorToggle()
    #expect(session.searchInspectorToggleRequestID == 0)

    session.route = .search
    session.requestSearchInspectorToggle()
    #expect(session.searchInspectorToggleRequestID == 1)
    session.requestSearchInspectorToggle()
    #expect(session.searchInspectorToggleRequestID == 1)
    session.consumeSearchInspectorToggleRequest(0)
    #expect(session.consumedSearchInspectorToggleRequestID == 0)
    session.consumeSearchInspectorToggleRequest(1)
    #expect(session.consumedSearchInspectorToggleRequestID == 1)

    session.updateSearchInspectorCommandState(hasInspector: true, isPresented: false, commandsBlocked: true)
    session.requestSearchInspectorToggle()
    #expect(session.searchInspectorToggleRequestID == 1)

    session.updateSearchInspectorCommandState(hasInspector: false, isPresented: true, commandsBlocked: false)
    #expect(session.searchInspectorIsPresented == false)
    session.requestSearchInspectorToggle()
    #expect(session.searchInspectorToggleRequestID == 1)

    session.updateSearchInspectorCommandState(hasInspector: true, isPresented: true, commandsBlocked: false)
    session.route = .brain
    #expect(session.searchHasInspector == false)
    #expect(session.searchInspectorIsPresented == false)
}

@MainActor
@Test func routeCommandsRejectMissingAPINonReadyAndStaleFocusedRoutes() {
    let session = AppSession()
    session.phase = .ready
    session.route = .brain
    #expect(!session.acceptsRouteCommands(for: .brain))

    session.api = MutationTestAPI(forgetResults: [])
    #expect(session.acceptsRouteCommands(for: .brain))
    #expect(!session.acceptsRouteCommands(for: .search))
    #expect(!session.acceptsRouteCommands(for: .settings))

    session.phase = .failed("Unavailable")
    #expect(!session.acceptsRouteCommands(for: .brain))
}

@Test func nativeCommandIDsAreExactAndUnique() {
    #expect(CerebrumCommandID.allCases.map(\.rawValue) == [
        "global.command.focus-search",
        "global.command.keyboard-shortcuts",
        "overview.command.refresh",
        "search.command.refresh",
        "search.command.toggle-inspector",
        "search.command.clear-selection",
        "brain.command.refresh",
        "brain.command.toggle-inspector",
        "brain.command.mode-memory-map",
        "brain.command.mode-agent-network",
        "brain.command.presentation-interactive-map",
        "brain.command.presentation-list-view",
        "brain.command.clear-selection",
        "brain.command.view-options",
    ])
    #expect(Set(CerebrumCommandID.allCases.map(\.rawValue)).count == CerebrumCommandID.allCases.count)

    func modifierName(_ modifiers: EventModifiers) -> String {
        [
            (EventModifiers.command, "command"),
            (.control, "control"),
            (.option, "option"),
            (.shift, "shift"),
        ]
        .filter { modifiers.contains($0.0) }
        .map(\.1)
        .joined(separator: "+")
    }
    let catalog = CerebrumCommandID.allCases.map { command in
        let spec = command.specification
        return [
            command.rawValue, spec.label, spec.key.map(String.init) ?? "-",
            modifierName(spec.modifiers), spec.display, spec.section,
        ].joined(separator: "|")
    }
    #expect(catalog == [
        "global.command.focus-search|Focus Search|f|command|⌘F|Search",
        "global.command.keyboard-shortcuts|Keyboard Shortcuts…|/|command|⌘/|Global",
        "overview.command.refresh|Refresh Overview|r|command|⌘R|Global",
        "search.command.refresh|Refresh Search|r|command|⌘R|Global",
        "search.command.toggle-inspector|Show or Hide Inspector|i|command+control|⌃⌘I|Search",
        "search.command.clear-selection|Clear Search Selection|-|||Search",
        "brain.command.refresh|Refresh Brain|r|command|⌘R|Global",
        "brain.command.toggle-inspector|Show or Hide Inspector|i|command+control|⌃⌘I|Brain",
        "brain.command.mode-memory-map|Memory Map|1|command+control|⌃⌘1|Brain",
        "brain.command.mode-agent-network|Agent Network|2|command+control|⌃⌘2|Brain",
        "brain.command.presentation-interactive-map|Interactive Map|m|command+control|⌃⌘M|Brain",
        "brain.command.presentation-list-view|List View|l|command+control|⌃⌘L|Brain",
        "brain.command.clear-selection|Clear Brain Selection|-|||Brain",
        "brain.command.view-options|Show or Hide View Options|v|command+control|⌃⌘V|Brain",
    ])
    #expect(CerebrumCommandID.allCases.allSatisfy {
        !($0.specification.modifiers.contains(.control) && $0.specification.modifiers.contains(.option))
    })
}

@Test func routedRefreshOwnsTheOnlyCommandRRegistration() throws {
    let packageRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
    let sourceRoot = packageRoot.appendingPathComponent("Sources/SAGECerebrumNative")
    let routeFiles = ["OverviewView.swift", "SearchView.swift", "BrainView.swift"]
    for file in routeFiles {
        let source = try String(contentsOf: sourceRoot.appendingPathComponent(file), encoding: .utf8)
        #expect(!source.contains(".keyboardShortcut(\"r\""))
        #expect(source.contains(".focusedSceneValue(\\.cerebrumRouteCommandActions"))
    }
    let commands = try String(
        contentsOf: sourceRoot.appendingPathComponent("CerebrumCommands.swift"),
        encoding: .utf8
    )
    #expect(commands.components(separatedBy: ".cerebrumShortcut(refreshCommandID").count - 1 == 1)
    #expect(commands.components(separatedBy: "key: \"r\", modifiers: .command").count - 1 == 3)
}

@Test func nativeCommandsUseStandardMenuPlacementAndExactShortcutCatalog() throws {
    let packageRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
    let sourceRoot = packageRoot.appendingPathComponent("Sources/SAGECerebrumNative")
    let commands = try String(
        contentsOf: sourceRoot.appendingPathComponent("CerebrumCommands.swift"),
        encoding: .utf8
    )
    #expect(commands.contains("CommandGroup(after: .sidebar)"))
    #expect(commands.contains("CommandGroup(before: .help)"))
    #expect(commands.contains("selected: session.route == route"))
    #expect(commands.contains("select: { session.route = route }"))
    #expect(commands.contains("NSMenu.didBeginTrackingNotification"))
    #expect(!commands.contains("menu.delegate = self"))
    #expect(commands.contains("keyboardShortcut(KeyEquivalent(key), modifiers: command.specification.modifiers)"))
    #expect(commands.contains("Self.shortcutRow(.keyboardShortcuts)"))
    #expect(commands.contains("(\"Clear Search Selection and Details\", \"Esc\")"))
    #expect(commands.contains("(\"Dismiss Current Brain Focus\", \"Esc\")"))

    let search = try String(
        contentsOf: sourceRoot.appendingPathComponent("SearchView.swift"),
        encoding: .utf8
    )
    #expect(search.contains("isPresented: $searchIsPresented"))
    #expect(search.contains("searchIsPresented = false"))
    #expect(search.contains("onFocusRequestConsumed(focusRequestID)"))
    #expect(search.contains("SearchInspectorLifecycle.activated"))
    #expect(search.contains("SearchInspectorLifecycle.selectionChanged"))
    #expect(search.contains(".onExitCommand"))
    #expect(search.contains(".accessibilityIdentifier(\"search-results-table\")"))
    #expect(search.contains(".accessibilityIdentifier(\"search-inspector-close\")"))
    #expect(search.contains("showsFilters || showsBulkTagSheet"))
    #expect(search.contains("forgetConfirmation != nil || model.isMutating"))
    #expect(search.contains("model.inspectedMemoryID != nil && !model.isMutating"))
    #expect(search.contains("clearBulkSelectionAndRestoreFocus"))
    #expect(search.contains("dismissCurrentSearchFocusAndRestoreFocus"))
}

@Test func searchInspectorLifecycleSeparatesSelectionFromPresentation() {
    var state = SearchInspectorLifecycle.activated(memoryID: "m1")
    #expect(state == .init(inspectedMemoryID: "m1", isPresented: true))

    state = SearchInspectorLifecycle.hidden(from: state)
    #expect(state == .init(inspectedMemoryID: "m1", isPresented: false))
    #expect(SearchInspectorLifecycle.toggled(from: state, inspectedMemoryIsAvailable: true)
        == .init(inspectedMemoryID: "m1", isPresented: true))
    #expect(SearchInspectorLifecycle.toggled(from: state, inspectedMemoryIsAvailable: false) == state)

    let multiSelection = SearchInspectorLifecycle.selectionChanged(
        from: state,
        selection: ["m1", "m2"]
    )
    #expect(multiSelection == state)
    #expect(SearchInspectorLifecycle.selectionChanged(from: state, selection: ["m2", "m3"])
        == state)
    #expect(SearchInspectorLifecycle.selectionChanged(from: state, selection: [])
        == state)
    #expect(SearchInspectorLifecycle.cleared == .init(inspectedMemoryID: nil, isPresented: false))
}

@MainActor
@Test func searchRefreshPreservesVisibleInspectionAndClearsRemovedResults() async throws {
    let decoder = JSONDecoder.sageDashboard()
    let memories = try decoder.decode([MemorySummary].self, from: Data(#"""
    [
      {"memory_id":"m1","submitting_agent":"a1","content":"First","content_hash":"h1","memory_type":"fact","domain_tag":"native","provider":"local","confidence_score":0.9,"status":"committed","created_at":"2026-08-23T00:00:00Z","corroboration_count":1},
      {"memory_id":"m2","submitting_agent":"a2","content":"Second","content_hash":"h2","memory_type":"observation","domain_tag":"native","provider":"local","confidence_score":0.8,"status":"committed","created_at":"2026-08-23T00:01:00Z","corroboration_count":0}
    ]
    """#.utf8))
    let api = MutationTestAPI(forgetResults: [], searchMemories: memories)
    let model = SearchViewModel(api: api)
    await model.refresh()
    model.selection = ["m1"]
    model.inspectedMemoryID = "m1"

    await model.refresh()
    #expect(model.selection == ["m1"])
    #expect(model.inspectedMemoryID == "m1")

    await api.setSearchMemories([memories[1]])
    await model.refresh()
    #expect(model.selection.isEmpty)
    #expect(model.inspectedMemoryID == nil)
}

@MainActor
@Test func searchForgetPreservesUnsettledTargetsAndClearsOnlyDeprecatedOnes() async throws {
    let memory = try JSONDecoder.sageDashboard().decode(MemorySummary.self, from: Data(#"""
    {"memory_id":"m1","submitting_agent":"a1","content":"Governed","content_hash":"h1","memory_type":"fact","domain_tag":"native","provider":"local","confidence_score":0.9,"status":"committed","created_at":"2026-08-23T00:00:00Z","corroboration_count":1}
    """#.utf8))
    let challengedAPI = MutationTestAPI(
        forgetResults: [.success(.init(status: "challenge_opened", message: nil))],
        searchMemories: [memory]
    )
    let challenged = SearchViewModel(api: challengedAPI)
    await challenged.refresh()
    challenged.selection = [memory.id]
    challenged.inspectedMemoryID = memory.id
    await challengedAPI.setSearchMemories([])
    await challenged.forget(ids: [memory.id])
    #expect(challenged.selection == [memory.id])
    #expect(challenged.inspectedMemoryID == memory.id)

    let deprecatedAPI = MutationTestAPI(
        forgetResults: [.success(.init(status: "deprecated", message: nil))],
        searchMemories: [memory]
    )
    let deprecated = SearchViewModel(api: deprecatedAPI)
    await deprecated.refresh()
    deprecated.selection = [memory.id]
    deprecated.inspectedMemoryID = memory.id
    await deprecatedAPI.setSearchMemories([])
    await deprecated.forget(ids: [memory.id])
    #expect(deprecated.selection.isEmpty)
    #expect(deprecated.inspectedMemoryID == nil)
}

@MainActor
@Test func staleSearchTagResponseCannotPopulateANewInspector() async throws {
    let api = MutationTestAPI(
        forgetResults: [],
        memoryTagsByID: ["m1": ["old"], "m2": ["current"]],
        memoryTagDelays: ["m1": .milliseconds(60)]
    )
    let model = SearchViewModel(api: api)
    model.inspectedMemoryID = "m1"
    let staleLoad = Task { await model.loadInspectedTags() }
    try await Task.sleep(for: .milliseconds(10))
    model.inspectedMemoryID = "m2"
    await model.loadInspectedTags()
    await staleLoad.value
    #expect(model.inspectedMemoryID == "m2")
    #expect(model.inspectedTags == ["current"])
}

@Test func brainUsesPlainLanguageAliasesForPrimaryControls() {
    #expect(BrainMode.memory.title == "Memory Map")
    #expect(BrainMode.connectome.title == "Agent Network")
    #expect(BrainPresentation.mri.title == "Interactive Map")
    #expect(BrainPresentation.table.title == "List View")
    #expect(BrainMode.memory.accessibilityTitle.contains("MRI"))
    #expect(BrainMode.connectome.accessibilityTitle.contains("Connectome"))
    #expect(BrainPresentation.mri.accessibilityTitle.contains("MRI"))
    #expect(BrainPresentation.table.accessibilityTitle.contains("Accessible Table"))
}

@Test func nativeTypographyReservesRoundedDesignForBrandAndHeroAccents() throws {
    let packageRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
    let sources = packageRoot.appendingPathComponent("Sources/SAGECerebrumNative")
    let roundedOccurrences = try FileManager.default.contentsOfDirectory(
        at: sources,
        includingPropertiesForKeys: nil
    )
    .filter { $0.pathExtension == "swift" }
    .reduce(into: [String: Int]()) { result, file in
        let source = try String(contentsOf: file, encoding: .utf8)
        let count = source.components(separatedBy: ".fontDesign(.rounded)").count - 1
        if count > 0 { result[file.lastPathComponent] = count }
    }

    #expect(roundedOccurrences == ["OverviewView.swift": 1, "RootView.swift": 1])
}

@MainActor
@Test func overviewStatusSeparatesSnapshotCompletenessFromEventTransport() async {
    let model = OverviewViewModel(api: MutationTestAPI(forgetResults: []))
    let updated = Date(timeIntervalSince1970: 1_700_000_000)

    #expect(model.dataStatus == .init(snapshot: .loading, events: .connecting))
    model.hasCompletedRefresh = true
    #expect(model.dataStatus.snapshot == .unavailable)

    model.lastUpdated = updated
    #expect(model.dataStatus.snapshot == .available(updatedAt: updated))
    model.healthIsStale = true
    model.statsAreStale = true
    #expect(model.dataStatus.snapshot == .partial(
        updatedAt: updated, detail: "2 of 5 sources not refreshed"
    ))
    model.agentsAreStale = true
    model.validatorsAreStale = true
    model.federationIsStale = true
    #expect(model.dataStatus.snapshot == .refreshFailed(updatedAt: updated))

    model.handleEventStreamElement(.state(.connected))
    #expect(model.dataStatus.events == .connected)
    #expect(model.dataStatus.snapshot == .refreshFailed(updatedAt: updated))
    model.handleEventStreamElement(.state(.reconnecting))
    #expect(model.dataStatus.events == .reconnecting)
}

@MainActor
@Test func searchStatusPreservesEmptySnapshotAgeAndPinnedUpdates() async throws {
    let model = SearchViewModel(api: MutationTestAPI(forgetResults: []))

    model.hasCompletedRefresh = true
    #expect(model.dataStatus.snapshot == .unavailable)
    await model.refresh()
    let updated = try #require(model.lastUpdated)
    #expect(model.dataStatus.snapshot == .available(updatedAt: updated))
    model.updatesAvailable = true
    #expect(model.dataStatus.snapshot == .available(updatedAt: updated))
    #expect(model.dataStatus.hasPendingUpdate)
    model.isStale = true
    #expect(model.dataStatus.snapshot == .refreshFailed(updatedAt: updated))

    model.handleEventStreamElement(.state(.connected))
    #expect(model.dataStatus.events == .connected)
    model.handleEventStreamElement(.state(.reconnecting))
    #expect(model.dataStatus.events == .reconnecting)
}

@MainActor
@Test func brainStatusKeepsMemoryAndAgentNetworkSnapshotAgesIndependent() async {
    let model = BrainViewModel(api: MutationTestAPI(forgetResults: []))

    await model.refresh()
    let memoryUpdatedAt = model.lastUpdated
    #expect(memoryUpdatedAt != nil)
    #expect(model.dataStatus.snapshot == memoryUpdatedAt.map(CerebrumSnapshotState.available))

    model.mode = .connectome
    #expect(model.lastUpdated == nil)
    #expect(model.dataStatus.snapshot == .loading)
    await model.refresh()
    let networkUpdatedAt = model.lastUpdated
    #expect(networkUpdatedAt != nil)
    #expect(model.dataStatus.snapshot == networkUpdatedAt.map(CerebrumSnapshotState.available))

    model.mode = .memory
    #expect(model.lastUpdated == memoryUpdatedAt)
    model.handleEventStreamElement(.state(.reconnecting))
    #expect(model.dataStatus.events == .reconnecting)
}

@Test func dataStatusLanguageDoesNotConfuseTransportWithFreshness() {
    #expect(CerebrumEventStreamState.allCases.map(\.title) == [
        "Connecting event updates", "Event updates connected", "Reconnecting event updates",
        "Event updates stopped",
    ])
    #expect(CerebrumSnapshotState.partial(
        updatedAt: .distantPast, detail: "2 of 5 sources not refreshed"
    ).detail == "2 of 5 sources not refreshed")
    #expect(CerebrumSnapshotState.available(updatedAt: .distantPast).title == "Updated")
    #expect(CerebrumSnapshotState.refreshFailed(updatedAt: .distantPast).title == "Refresh failed")
}

@MainActor
@Test func domainEventNamesCannotImpersonateTransportState() async {
    let model = OverviewViewModel(api: MutationTestAPI(forgetResults: []))
    #expect(model.eventStreamState == .connecting)

    model.handleEventStreamElement(.event(.init(
        name: "connected", data: "{}", receivedAt: .now
    )))

    #expect(model.eventStreamState == .connecting)
    model.handleEventStreamElement(.state(.connected))
    #expect(model.eventStreamState == .connected)
}

@MainActor
@Test func unauthorizedEventTerminationStopsTransportAndPolling() async {
    let api = MutationTestAPI(forgetResults: [], eventStreamError: .unauthorized)
    let overview = OverviewViewModel(api: api)
    let search = SearchViewModel(api: api)
    let brain = BrainViewModel(api: api)
    search.lastUpdated = .now
    search.hasCompletedRefresh = true
    brain.graph = .init(
        nodes: [], edges: [], total: 1, domainCounts: [:], domainLast: [:],
        continuationRequired: false, projection: nil
    )
    brain.lastUpdated = .now

    await overview.runLiveUpdates()
    await search.runLiveUpdates()
    await brain.runLiveUpdates()

    #expect(overview.eventStreamState == .stopped)
    #expect(overview.lastUpdated == nil)
    #expect(search.eventStreamState == .stopped)
    #expect(search.lastUpdated == nil)
    #expect(brain.eventStreamState == .stopped)
    #expect(brain.graph == nil)
}

@MainActor
@Test func snapshotStatusIsKeyedToTheVisibleRequestScope() async {
    let api = MutationTestAPI(forgetResults: [])
    let search = SearchViewModel(api: api)
    await search.refresh()
    #expect(search.dataStatus.snapshot.updatedAt != nil)
    search.updatesAvailable = true
    search.domain = "another-domain"
    #expect(search.dataStatus.snapshot == .loading)
    #expect(!search.dataStatus.hasPendingUpdate)

    let brain = BrainViewModel(api: api)
    await brain.refresh()
    #expect(brain.dataStatus.snapshot.updatedAt != nil)
    brain.updatesAvailable = true
    brain.selectedDomain = "another-domain"
    #expect(brain.dataStatus.snapshot == .loading)
    #expect(!brain.dataStatus.hasPendingUpdate)
}

@MainActor
@Test func rollingSearchDatePresetsUseStableSnapshotScopes() async {
    let model = SearchViewModel(api: MutationTestAPI(forgetResults: []))
    for preset in [MemoryDatePreset.hour, .day, .week, .month] {
        model.datePreset = preset
        await model.refresh()
        #expect(model.dataStatus.snapshot.updatedAt != nil)
        model.updatesAvailable = true
        #expect(model.dataStatus.hasPendingUpdate)
    }
}

@MainActor
@Test func brainNeverRestoresMetadataOverAnotherFiltersPayload() async {
    let model = BrainViewModel(api: MutationTestAPI(forgetResults: []))
    model.selectedDomain = "alpha"
    await model.refresh()
    #expect(model.lastUpdated != nil)

    model.selectedDomain = "beta"
    await model.refresh()
    #expect(model.lastUpdated != nil)

    model.selectedDomain = "alpha"
    #expect(model.graph == nil)
    #expect(model.lastUpdated == nil)
    #expect(model.dataStatus.snapshot == .loading)
}

@MainActor
@Test func backendPartialitySurvivesPendingUpdateSignals() async {
    let projection = MemoryProjection(
        complete: false,
        partial: true,
        verifiedOnly: true,
        state: "partial",
        hiddenCount: 2,
        message: "2 memories are still being verified."
    )
    let search = SearchViewModel(api: MutationTestAPI(forgetResults: [], searchProjection: projection))
    await search.refresh()
    search.updatesAvailable = true

    guard case let .partial(_, detail) = search.dataStatus.snapshot else {
        Issue.record("Expected the partial backend projection to remain visible")
        return
    }
    #expect(detail == projection.message)
    #expect(search.dataStatus.hasPendingUpdate)
    search.domain = "another-domain"
    #expect(!search.hasSnapshotForCurrentScope)
}

@MainActor
@Test func brainPartialitySurvivesPendingUpdateSignals() async {
    let projection = MemoryProjection(
        complete: false, partial: true, verifiedOnly: true, state: "partial",
        hiddenCount: 2, message: "2 memories are still being verified."
    )
    let graph = BrainGraphEnvelope(
        nodes: [], edges: [], total: 2, domainCounts: [:], domainLast: [:],
        continuationRequired: false, projection: projection
    )
    let brain = BrainViewModel(api: MutationTestAPI(forgetResults: [], graph: graph))
    await brain.refresh()
    brain.updatesAvailable = true

    guard case let .partial(_, detail) = brain.dataStatus.snapshot else {
        Issue.record("Expected the Brain partial projection to remain visible")
        return
    }
    #expect(detail == projection.message)
    #expect(brain.dataStatus.hasPendingUpdate)
}

@Suite(.serialized)
struct HostedBrainAcceptance {}

#if DEBUG
@Test func nativeAppSceneAcceptanceFixtureRequiresExactPreviewGate() {
    let commit = String(repeating: "a", count: 40)
    let sourceState = "clean:" + String(repeating: "b", count: 64)
    let runID = "20260824T180000Z-app-scene-42"
    #expect(NativeAppSceneAcceptanceFixture(environment: [:]) == nil)
    #expect(NativeAppSceneAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
    ]) == nil)
    #expect(NativeAppSceneAcceptanceFixture(environment: [
        "SAGE_NATIVE_APP_SCENE_ACCEPTANCE": NativeAppSceneAcceptanceFixture.scenario,
        "SAGE_NATIVE_APP_SCENE_COMMIT": commit,
        "SAGE_NATIVE_APP_SCENE_SOURCE_STATE": sourceState,
        "SAGE_NATIVE_APP_SCENE_RUN_ID": runID,
    ]) == nil)
    #expect(NativeAppSceneAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "0",
        "SAGE_NATIVE_APP_SCENE_ACCEPTANCE": NativeAppSceneAcceptanceFixture.scenario,
        "SAGE_NATIVE_APP_SCENE_COMMIT": commit,
        "SAGE_NATIVE_APP_SCENE_SOURCE_STATE": sourceState,
        "SAGE_NATIVE_APP_SCENE_RUN_ID": runID,
    ]) == nil)
    #expect(NativeAppSceneAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_APP_SCENE_ACCEPTANCE": "unknown",
        "SAGE_NATIVE_APP_SCENE_COMMIT": commit,
        "SAGE_NATIVE_APP_SCENE_SOURCE_STATE": sourceState,
        "SAGE_NATIVE_APP_SCENE_RUN_ID": runID,
    ]) == nil)
    #expect(NativeAppSceneAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_APP_SCENE_ACCEPTANCE": NativeAppSceneAcceptanceFixture.scenario,
        "SAGE_NATIVE_APP_SCENE_COMMIT": "not-a-commit",
        "SAGE_NATIVE_APP_SCENE_SOURCE_STATE": sourceState,
        "SAGE_NATIVE_APP_SCENE_RUN_ID": runID,
    ]) == nil)
    let fixture = NativeAppSceneAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_APP_SCENE_ACCEPTANCE": NativeAppSceneAcceptanceFixture.scenario,
        "SAGE_NATIVE_APP_SCENE_COMMIT": commit,
        "SAGE_NATIVE_APP_SCENE_SOURCE_STATE": sourceState,
        "SAGE_NATIVE_APP_SCENE_RUN_ID": runID,
    ])
    #expect(fixture?.commit == commit)
    #expect(fixture?.sourceState == sourceState)
    #expect(fixture?.runID == runID)
}

@Test func nativeAXAcceptanceFixtureIsExplicitAndBounded() {
    #expect(NativeAXAcceptanceFixture(environment: [:]) == nil)
    #expect(NativeAXAcceptanceFixture(environment: [
        "SAGE_NATIVE_AX_METAL": "unavailable",
        "SAGE_NATIVE_AX_RETRY_RESULT": "fail",
    ]) == nil)
    #expect(NativeAXAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_AX_METAL": "available",
        "SAGE_NATIVE_AX_RETRY_RESULT": "fail",
    ]) == nil)
    #expect(NativeAXAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_AX_METAL": "unavailable",
        "SAGE_NATIVE_AX_RETRY_RESULT": "unknown",
    ]) == nil)
    #expect(NativeAXAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_AX_METAL": "unavailable",
        "SAGE_NATIVE_AX_RETRY_RESULT": "restore",
        "SAGE_NATIVE_AX_RETRY_DELAY_MS": "99",
    ]) == nil)

    let fixture = NativeAXAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_AX_METAL": "unavailable",
        "SAGE_NATIVE_AX_RETRY_RESULT": "fail",
        "SAGE_NATIVE_AX_RETRY_DELAY_MS": "800",
    ])
    #expect(fixture?.retryResult == .fail)
    #expect(fixture?.retryDelay == .milliseconds(800))
    #expect(NativeAXAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_AX_METAL": "unavailable",
        "SAGE_NATIVE_AX_RETRY_RESULT": "restore",
    ])?.retryDelay == .milliseconds(750))
    for boundary in [100, 5_000] {
        #expect(NativeAXAcceptanceFixture(environment: [
            "SAGE_NATIVE_DESIGN_PREVIEW": "1",
            "SAGE_NATIVE_AX_METAL": "unavailable",
            "SAGE_NATIVE_AX_RETRY_RESULT": "fail",
            "SAGE_NATIVE_AX_RETRY_DELAY_MS": String(boundary),
        ]) != nil)
    }
    #expect(NativeAXAcceptanceFixture(environment: [
        "SAGE_NATIVE_DESIGN_PREVIEW": "1",
        "SAGE_NATIVE_AX_METAL": "unavailable",
        "SAGE_NATIVE_AX_RETRY_RESULT": "fail",
        "SAGE_NATIVE_AX_RETRY_DELAY_MS": "5001",
    ]) == nil)
}
#endif

@MainActor
@Test(.enabled(if: ProcessInfo.processInfo.environment["SAGE_REQUIRE_METAL_HARDWARE"] == "1"))
func nativeBrainRendererCompilesItsMetalPipelineFamily() throws {
    let renderer = try #require(BrainMetalRenderer(onPick: { _ in }))
    #expect(!renderer.metalDevice.name.isEmpty)

    renderer.hullOpacity = 0
    renderer.update(nodes: [], edges: [], highlightedEdge: nil, topologyFocusID: nil, layout: .memory)
    let clear = try renderer.renderOffscreenProbe(width: 160, height: 120, bloomEnabled: false)
    renderer.update(
        nodes: [
            .init(
                id: "probe", content: "Probe", domain: "native", confidence: 1,
                status: "committed", memoryType: "fact", createdAt: .now,
                agent: "test", agentLabel: nil, agentIsRoot: false,
                tags: nil, corroborationCount: 1
            ),
        ],
        edges: [], highlightedEdge: nil, topologyFocusID: nil, layout: .memory
    )
    let scene = try renderer.renderOffscreenProbe(width: 160, height: 120, bloomEnabled: false)
    let probe = try renderer.renderOffscreenProbe(width: 160, height: 120)
    #expect(probe.width == 160)
    #expect(probe.height == 120)
    #expect(probe.bloomEncoded)
    #expect(probe.nonBackgroundPixelCount > 64)
    #expect(scene.changedPixelCount(comparedTo: clear) > 16)
    #expect(probe.changedPixelCount(comparedTo: scene, tolerance: 2) > 16)
    #expect(probe.rgbEnergy > scene.rgbEnergy)
    #expect(throws: BrainMetalOffscreenError.self) {
        try renderer.renderOffscreenProbe(width: 0, height: 120)
    }
    #expect(throws: BrainMetalOffscreenError.self) {
        try renderer.renderOffscreenProbe(width: 2_049, height: 120)
    }
}

@Test func metalPresentationPolicyFallsBackOnlyWhenRenderingIsUnavailable() {
    #expect(BrainPresentationPolicy.resolve(
        requested: .mri, capability: .probing
    ) == .init(effectivePresentation: .mri, mriEnabled: true))
    #expect(BrainPresentationPolicy.resolve(
        requested: .mri, capability: .available
    ) == .init(effectivePresentation: .mri, mriEnabled: true))
    #expect(BrainPresentationPolicy.resolve(
        requested: .mri, capability: .unavailable(.rendererInitialization)
    ) == .init(effectivePresentation: .table, mriEnabled: false))
    #expect(BrainPresentationPolicy.resolve(
        requested: .table, capability: .unavailable(.rendererInitialization)
    ) == .init(effectivePresentation: .table, mriEnabled: false))
}

@Test func brainResponsivePolicyHasStableBoundariesAndFitsItsVerticalBudget() {
    let compact = BrainResponsiveLayoutPolicy.resolve(
        size: CGSize(width: 619, height: 540), trainVisible: true
    )
    let regular = BrainResponsiveLayoutPolicy.resolve(
        size: CGSize(width: 620, height: 600), trainVisible: true
    )
    let regularUpper = BrainResponsiveLayoutPolicy.resolve(
        size: CGSize(width: 839, height: 700), trainVisible: false
    )
    let expanded = BrainResponsiveLayoutPolicy.resolve(
        size: CGSize(width: 840, height: 760), trainVisible: true
    )

    #expect(compact.tier == .compact)
    #expect(regular.tier == .regular)
    #expect(regularUpper.tier == .regular)
    #expect(expanded.tier == .expanded)
    #expect(!compact.showsInlineNavigator)
    #expect(!regular.showsInlineNavigator)
    #expect(expanded.showsInlineNavigator)
    #expect(compact.usesCompactToolbar)
    #expect(!expanded.usesCompactToolbar)
    #expect(compact.inspectorMinimumWidth == expanded.inspectorMinimumWidth)
    #expect(compact.inspectorIdealWidth == expanded.inspectorIdealWidth)
    #expect(compact.inspectorMaximumWidth == expanded.inspectorMaximumWidth)

    for (plan, height) in [(compact, 540.0), (regular, 600.0), (expanded, 760.0)] {
        let contentHeight = height - (plan.pagePadding * 2)
        #expect(plan.surfaceMinimumHeight + plan.trainMinimumHeight <= contentHeight)
        #expect(plan.trainMinimumHeight <= plan.trainIdealHeight)
        #expect(plan.trainIdealHeight <= plan.trainMaximumHeight)
    }
}

@Test func metalRecoveryFailurePreservesIndependentFocusOwnership() {
    let initial = BrainMetalRecoveryState()
    let transition = BrainMetalRecoveryReducer.reduce(initial, event: .rendererReported(
        attemptID: 0,
        capability: .unavailable(.rendererInitialization),
        keyboardSurfaceOwned: true,
        accessibilitySurfaceOwned: false
    ))

    #expect(transition.state.presentation == .table)
    #expect(transition.state.effectivePresentation == .table)
    #expect(transition.state.capability == .unavailable(.rendererInitialization))
    #expect(transition.effects.keyboardFocus == .table)
    #expect(transition.effects.accessibilityFocus == nil)
    #expect(transition.effects.announcement == .unavailable)

    let duplicate = BrainMetalRecoveryReducer.reduce(transition.state, event: .rendererReported(
        attemptID: 0,
        capability: .unavailable(.rendererInitialization),
        keyboardSurfaceOwned: true,
        accessibilitySurfaceOwned: true
    ))
    #expect(duplicate.state == transition.state)
    #expect(duplicate.effects == .none)

    let stale = BrainMetalRecoveryReducer.reduce(initial, event: .rendererReported(
        attemptID: 42,
        capability: .unavailable(.rendererInitialization),
        keyboardSurfaceOwned: true,
        accessibilitySurfaceOwned: true
    ))
    #expect(stale.state == initial)
    #expect(stale.effects == .none)
}

@Test func selectingTableInvalidatesThePendingMRIAttempt() {
    let probing = BrainMetalRecoveryState(
        presentation: .mri, capability: .probing,
        retryInFlight: false, attemptID: 12
    )
    let selectedTable = BrainMetalRecoveryReducer.reduce(
        probing, event: .presentationSelected(.table)
    )
    #expect(selectedTable.state.presentation == .table)
    #expect(selectedTable.state.attemptID == 13)
    #expect(selectedTable.effects.discardPreparedRenderer)
    #expect(selectedTable.effects.keyboardFocus == .table)
    #expect(selectedTable.effects.accessibilityFocus == .table)

    let staleFailure = BrainMetalRecoveryReducer.reduce(
        selectedTable.state,
        event: .rendererReported(
            attemptID: 12,
            capability: .unavailable(.rendererInitialization),
            keyboardSurfaceOwned: true,
            accessibilitySurfaceOwned: true
        )
    )
    #expect(staleFailure.state == selectedTable.state)
    #expect(staleFailure.effects == .none)
}

@Test func metalRecoveryRetryKeepsTableMountedAndFencesCompletion() {
    let unavailable = BrainMetalRecoveryState(
        presentation: .table,
        capability: .unavailable(.rendererInitialization),
        retryInFlight: false,
        attemptID: 7
    )
    let requested = BrainMetalRecoveryReducer.reduce(unavailable, event: .retryRequested)
    #expect(requested.state.retryInFlight)
    #expect(requested.state.attemptID == 8)
    #expect(requested.state.effectivePresentation == .table)
    #expect(requested.effects.beginRetryAttempt == 8)

    let duplicate = BrainMetalRecoveryReducer.reduce(requested.state, event: .retryRequested)
    #expect(duplicate.state == requested.state)
    #expect(duplicate.effects == .none)

    let cancelled = BrainMetalRecoveryReducer.reduce(requested.state, event: .retryCancelled)
    #expect(!cancelled.state.retryInFlight)
    #expect(cancelled.state.attemptID == 9)
    #expect(cancelled.effects.discardPreparedRenderer)

    let changedMode = BrainMetalRecoveryReducer.reduce(requested.state, event: .modeChanged)
    #expect(!changedMode.state.retryInFlight)
    #expect(changedMode.state.attemptID == 9)
    #expect(changedMode.effects.discardPreparedRenderer)

    let staleSuccess = BrainMetalRecoveryReducer.reduce(
        changedMode.state,
        event: .retryCompleted(attemptID: 8, succeeded: true)
    )
    #expect(staleSuccess.state == changedMode.state)
    #expect(staleSuccess.effects == .none)

    let staleFailure = BrainMetalRecoveryReducer.reduce(
        changedMode.state,
        event: .retryCompleted(attemptID: 8, succeeded: false)
    )
    #expect(staleFailure.state == changedMode.state)
    #expect(staleFailure.effects == .none)

    let newerRetry = BrainMetalRecoveryReducer.reduce(changedMode.state, event: .retryRequested)
    #expect(newerRetry.state.retryInFlight)
    #expect(newerRetry.state.attemptID == 10)
    let oldCompletionDuringNewerRetry = BrainMetalRecoveryReducer.reduce(
        newerRetry.state,
        event: .retryCompleted(attemptID: 8, succeeded: true)
    )
    #expect(oldCompletionDuringNewerRetry.state == newerRetry.state)
    #expect(oldCompletionDuringNewerRetry.effects == .none)
}

@Test func metalRecoveryRetryProducesDeterministicFailureAndSuccessEffects() {
    let unavailable = BrainMetalRecoveryState(
        presentation: .table,
        capability: .unavailable(.rendererInitialization),
        retryInFlight: false,
        attemptID: 3
    )
    let retry = BrainMetalRecoveryReducer.reduce(unavailable, event: .retryRequested)

    let failed = BrainMetalRecoveryReducer.reduce(
        retry.state,
        event: .retryCompleted(attemptID: 4, succeeded: false)
    )
    #expect(failed.state.effectivePresentation == .table)
    #expect(!failed.state.retryInFlight)
    #expect(failed.effects.keyboardFocus == .retryButton)
    #expect(failed.effects.accessibilityFocus == .retryButton)
    #expect(failed.effects.announcement == .stillUnavailable)

    let retryAgain = BrainMetalRecoveryReducer.reduce(failed.state, event: .retryRequested)
    let prepared = BrainMetalRecoveryReducer.reduce(
        retryAgain.state,
        event: .retryCompleted(attemptID: 5, succeeded: true)
    )
    #expect(prepared.state.presentation == .mri)
    #expect(prepared.state.capability == .probing)
    #expect(prepared.state.retryInFlight)
    #expect(prepared.state.effectivePresentation == .mri)
    #expect(prepared.effects.acceptPreparedRenderer)
    #expect(prepared.effects.keyboardFocus == nil)
    #expect(prepared.effects.accessibilityFocus == nil)
    #expect(prepared.effects.announcement == nil)

    let mounted = BrainMetalRecoveryReducer.reduce(
        prepared.state,
        event: .rendererReported(
            attemptID: 5, capability: .available,
            keyboardSurfaceOwned: false, accessibilitySurfaceOwned: false
        )
    )
    #expect(mounted.state.capability == .available)
    #expect(!mounted.state.retryInFlight)
    #expect(mounted.effects.keyboardFocus == .surface)
    #expect(mounted.effects.accessibilityFocus == .surface)
    #expect(mounted.effects.announcement == .restored)

    let blockedPickerMRI = BrainMetalRecoveryReducer.reduce(
        unavailable,
        event: .presentationSelected(.mri)
    )
    #expect(blockedPickerMRI.state == unavailable)
    #expect(blockedPickerMRI.effects == .none)
}

@MainActor
@Test func metalCoordinatorReportsAnInjectedBootstrapFailure() {
    let coordinator = BrainMetalCoordinator(onPick: { _ in }) { _ in
        .failure(.rendererInitialization)
    }

    #expect(coordinator.renderer == nil)
    #expect(coordinator.capability == .unavailable(.rendererInitialization))
}

extension HostedBrainAcceptance {
@MainActor
@Test func hostedBrainMountsAccessibleFallbackWithoutClearingSelection() async throws {
    let selected = BrainNode(
        id: "selected-memory", content: "Selected native memory", domain: "native",
        confidence: 0.96, status: "committed", memoryType: "fact", createdAt: .now,
        agent: "test", agentLabel: "Test Agent", agentIsRoot: false,
        tags: ["native"], corroborationCount: 2
    )
    let graph = BrainGraphEnvelope(
        nodes: [selected], edges: [], total: 1, domainCounts: ["native": 1],
        domainLast: nil, continuationRequired: false, projection: nil
    )
    let api = MutationTestAPI(forgetResults: [], graph: graph)
    let model = BrainViewModel(api: api)
    model.graph = graph
    model.selectedNodeID = selected.id
    let recorder = BrainHostRecorder()
    let view = BrainView(
        model: model,
        rendererBootstrap: { _ in .failure(.rendererInitialization) },
        accessibilityAnnouncer: { recorder.announcements.append($0) },
        surfaceObserver: { surface, mounted in
            recorder.update(surface, mounted: mounted)
        }
    )
    let window = NSWindow(
        contentRect: NSRect(x: 0, y: 0, width: 1_180, height: 760),
        styleMask: [.titled, .resizable], backing: .buffered, defer: false
    )
    let host = NSHostingView(rootView: view)
    window.contentView = host
    window.makeKeyAndOrderFront(nil)
    host.layoutSubtreeIfNeeded()
    let surfaces = try await waitForMountedSurfaces(
        recorder,
        required: [.metalFallbackNotice, .metalRetryButton, .memoryTable],
        forbidden: [.memoryMRI]
    )

    #expect(surfaces.contains(.metalFallbackNotice))
    #expect(surfaces.contains(.metalRetryButton))
    #expect(surfaces.contains(.memoryTable))
    #expect(!surfaces.contains(.memoryMRI))
    #expect(model.selectedNodeID == selected.id)
    #expect(recorder.announcements == ["Interactive MRI unavailable. Showing Accessible Table."])
}
}

@Test func brainInspectorHideAndEscapeKeepDistinctSemantics() throws {
    let packageRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
    let source = try String(
        contentsOf: packageRoot.appendingPathComponent("Sources/SAGECerebrumNative/BrainView.swift"),
        encoding: .utf8
    )
    let closeStart = try #require(source.range(of: "BrainInspectorCloseButton(model: model)"))
    let closeEnd = try #require(source.range(of: ".help(\"Hide Inspector\")", range: closeStart.lowerBound ..< source.endIndex))
    let closeAction = source[closeStart.lowerBound ..< closeEnd.lowerBound]

    #expect(source.contains("model.inspectorIsPresented = false"))
    #expect(source.contains("model.inspectorVisibilityIsUserControlled = true"))
    #expect(!closeAction.contains("clearSelection"))
    #expect(!closeAction.contains("selectedNodeID"))
    #expect(source.contains("selected != nil && !model.inspectorVisibilityIsUserControlled"))
    #expect(source.contains("Label(inspectorIsPresented ? \"Hide Inspector\" : \"Show Inspector\""))
    #expect(source.contains(".onExitCommand"))
    #expect(source.contains("dismissCurrentSelectionAndRestoreFocus()"))
    #expect(source.contains("HSplitView"))
    #expect(source.contains("minWidth: layoutPlan.inspectorMinimumWidth"))
    #expect(source.contains("idealWidth: layoutPlan.inspectorIdealWidth"))
    #expect(source.contains("maxWidth: layoutPlan.inspectorMaximumWidth"))
    #expect(closeAction.contains("requestFocus(returnFocusTarget)"))
    #expect(source.contains("blocksGlobalCommands: showsNavigator || showsViewOptions"))
    #expect(source.contains("model.mode == .memory ? \"brain-memory-table\" : \"brain-connectome-table\""))
    #expect(source.contains("case .inspectorClose:\n            \"brain-inspector-close\""))
    #expect(source.contains("private struct BrainNativeTableIdentityBridge: NSViewRepresentable"))
    #expect(source.contains("private struct BrainInspectorCloseButton: NSViewRepresentable"))
}

extension HostedBrainAcceptance {
@MainActor
@Test func hostedBrainRetryControlPerformsARealAccessibilityPressAndKeepsFallbackStable() async throws {
    let selected = BrainNode(
        id: "retry-memory", content: "Retry-preserved native memory", domain: "native",
        confidence: 0.98, status: "committed", memoryType: "fact", createdAt: .now,
        agent: "test", agentLabel: "Test Agent", agentIsRoot: false,
        tags: ["retry"], corroborationCount: 4
    )
    let graph = BrainGraphEnvelope(
        nodes: [selected], edges: [], total: 1, domainCounts: ["native": 1],
        domainLast: nil, continuationRequired: false, projection: nil
    )
    let api = MutationTestAPI(forgetResults: [], graph: graph)
    let model = BrainViewModel(api: api)
    model.graph = graph
    model.selectedNodeID = selected.id
    let recorder = BrainHostRecorder()
    let view = BrainView(
        model: model,
        rendererBootstrap: { _ in
            recorder.bootstrapAttempts += 1
            return .failure(.rendererInitialization)
        },
        accessibilityAnnouncer: { recorder.recordAnnouncement($0) },
        surfaceObserver: { recorder.update($0, mounted: $1) }
    )
    let window = NSWindow(
        contentRect: NSRect(x: 0, y: 0, width: 1_180, height: 760),
        styleMask: [.titled, .resizable], backing: .buffered, defer: false
    )
    let host = NSHostingView(rootView: view)
    window.contentView = host
    window.makeKeyAndOrderFront(nil)
    host.layoutSubtreeIfNeeded()
    _ = try await waitForMountedSurfaces(
        recorder,
        required: [.metalFallbackNotice, .metalRetryButton, .memoryTable],
        forbidden: [.memoryMRI]
    )
    let attemptsBeforeRetry = recorder.bootstrapAttempts
    try await pressAccessibilityElement(identifier: "brain-metal-retry", in: host)
    try await waitForRetryAttempts(recorder, count: attemptsBeforeRetry + 1)
    try await waitForAnnouncements(recorder, count: 2)
    let retryButton = try await waitForNativeAccessibilityButton(
        identifier: "brain-metal-retry",
        in: host,
        where: { $0.isEnabled }
    )
    try await waitUntil { window.firstResponder === retryButton }

    let surfaces = try await waitForMountedSurfaces(
        recorder,
        required: [.metalFallbackNotice, .metalRetryButton, .memoryTable],
        forbidden: [.memoryMRI]
    )
    #expect(surfaces.contains(.memoryTable))
    #expect(model.selectedNodeID == selected.id)
    #expect(recorder.bootstrapAttempts == attemptsBeforeRetry + 1)
    #expect(recorder.announcements == [
        "Interactive MRI unavailable. Showing Accessible Table.",
        "Interactive MRI is still unavailable. Accessible Table remains active.",
    ])
}

@MainActor
@Test(.enabled(if: ProcessInfo.processInfo.environment["SAGE_REQUIRE_METAL_HARDWARE"] == "1"))
func hostedBrainHoldsRetryProgressAndDiscardsDelayedSuccessAfterModeChange() async throws {
    let selected = BrainNode(
        id: "held-retry-memory", content: "Held retry native memory", domain: "native",
        confidence: 0.99, status: "committed", memoryType: "fact", createdAt: .now,
        agent: "test", agentLabel: "Test Agent", agentIsRoot: false,
        tags: ["retry", "stale"], corroborationCount: 6
    )
    let graph = BrainGraphEnvelope(
        nodes: [selected], edges: [], total: 1, domainCounts: ["native": 1],
        domainLast: nil, continuationRequired: false, projection: nil
    )
    let connectome = ConnectomeEnvelope(
        neurons: [.init(agentID: "retry-agent", name: "Retry Agent", role: "member", domain: "native")],
        synapses: []
    )
    let api = MutationTestAPI(forgetResults: [], graph: graph, connectome: connectome)
    let model = BrainViewModel(api: api)
    model.graph = graph
    model.connectome = connectome
    model.selectedNodeID = selected.id
    let recorder = BrainHostRecorder()
    let gate = BrainRetryGate()
    defer { gate.open() }
    var delayedRenderer: BrainMetalRenderer? = BrainMetalRenderer(onPick: { _ in })
    weak let releasedRenderer = delayedRenderer
    let view = BrainView(
        model: model,
        rendererBootstrap: { _ in
            recorder.bootstrapAttempts += 1
            return .failure(.rendererInitialization)
        },
        retryRendererBootstrap: {
            recorder.retryBootstrapAttempts += 1
            await gate.wait()
            recorder.retryBootstrapReturned = true
            guard let renderer = delayedRenderer else {
                return .failure(.rendererInitialization)
            }
            delayedRenderer = nil
            return .success(renderer)
        },
        accessibilityAnnouncer: { recorder.recordAnnouncement($0) },
        surfaceObserver: { recorder.update($0, mounted: $1) }
    )
    let window = NSWindow(
        contentRect: NSRect(x: 0, y: 0, width: 1_180, height: 760),
        styleMask: [.titled, .resizable], backing: .buffered, defer: false
    )
    let host = NSHostingView(rootView: view)
    window.contentView = host
    window.makeKeyAndOrderFront(nil)
    host.layoutSubtreeIfNeeded()
    _ = try await waitForMountedSurfaces(
        recorder,
        required: [.metalFallbackNotice, .metalRetryButton, .memoryTable],
        forbidden: [.memoryMRI]
    )
    try await waitForAnnouncements(recorder, count: 1)

    try await pressAccessibilityElement(identifier: "brain-metal-retry", in: host)
    try await waitUntil { recorder.retryBootstrapAttempts == 1 && gate.isWaiting }
    let retryButton = try await waitForNativeAccessibilityButton(
        identifier: "brain-metal-retry",
        in: host,
        where: { !$0.isEnabled }
    )
    #expect(retryButton.accessibilityRole() == .button)
    #expect(!retryButton.isAccessibilityEnabled())
    #expect(retryButton.accessibilityLabel() == "Trying MRI")
    #expect(retryButton.accessibilityValue() as? String == "In progress")
    #expect(!retryButton.accessibilityPerformPress())
    #expect(recorder.retryBootstrapAttempts == 1)
    #expect(model.selectedNodeID == selected.id)
    _ = try await waitForMountedSurfaces(
        recorder,
        required: [.metalFallbackNotice, .metalRetryButton, .memoryTable],
        forbidden: [.memoryMRI]
    )

    model.mode = .connectome
    gate.open()
    try await waitUntil { recorder.retryBootstrapReturned && releasedRenderer == nil }
    _ = try await waitForMountedSurfaces(
        recorder,
        required: [.metalFallbackNotice, .metalRetryButton, .connectomeTable],
        forbidden: [.memoryMRI, .memoryTable, .connectomeMRI]
    )
    let resetRetryButton = try await waitForNativeAccessibilityButton(
        identifier: "brain-metal-retry",
        in: host,
        where: { $0.isEnabled }
    )
    #expect(resetRetryButton.accessibilityLabel() == "Try MRI Again")
    #expect(resetRetryButton.isAccessibilityEnabled())

    #expect(model.mode == .connectome)
    #expect(model.selectedNodeID == selected.id)
    #expect(recorder.retryBootstrapAttempts == 1)
    #expect(recorder.announcements == [
        "Interactive MRI unavailable. Showing Accessible Table.",
    ])
    #expect(!recorder.surfaces.contains(.memoryMRI))
    #expect(!recorder.surfaces.contains(.connectomeMRI))
}

@MainActor
@Test(.enabled(if: ProcessInfo.processInfo.environment["SAGE_REQUIRE_METAL_HARDWARE"] == "1"))
func hostedBrainRetryControlRestoresMRIOnlyAfterTheRendererMounts() async throws {
    let selected = BrainNode(
        id: "restored-memory", content: "Renderer-restored native memory", domain: "native",
        confidence: 0.99, status: "committed", memoryType: "fact", createdAt: .now,
        agent: "test", agentLabel: "Test Agent", agentIsRoot: false,
        tags: ["retry", "metal"], corroborationCount: 5
    )
    let graph = BrainGraphEnvelope(
        nodes: [selected], edges: [], total: 1, domainCounts: ["native": 1],
        domainLast: nil, continuationRequired: false, projection: nil
    )
    let api = MutationTestAPI(forgetResults: [], graph: graph)
    let model = BrainViewModel(api: api)
    model.graph = graph
    model.selectedNodeID = selected.id
    let recorder = BrainHostRecorder()
    recorder.successRenderer = BrainMetalRenderer(onPick: { _ in })
    let view = BrainView(
        model: model,
        rendererBootstrap: { _ in
            recorder.bootstrapAttempts += 1
            if recorder.servesSuccessRenderer, let renderer = recorder.successRenderer {
                recorder.servesSuccessRenderer = false
                recorder.successRenderer = nil
                return .success(renderer)
            }
            return .failure(.rendererInitialization)
        },
        accessibilityAnnouncer: { recorder.recordAnnouncement($0) },
        surfaceObserver: { recorder.update($0, mounted: $1) }
    )
    let window = NSWindow(
        contentRect: NSRect(x: 0, y: 0, width: 1_180, height: 760),
        styleMask: [.titled, .resizable], backing: .buffered, defer: false
    )
    let host = NSHostingView(rootView: view)
    window.contentView = host
    window.makeKeyAndOrderFront(nil)
    host.layoutSubtreeIfNeeded()
    _ = try await waitForMountedSurfaces(
        recorder,
        required: [.metalFallbackNotice, .metalRetryButton, .memoryTable],
        forbidden: [.memoryMRI]
    )

    let attemptsBeforeFailure = recorder.bootstrapAttempts
    try await pressAccessibilityElement(identifier: "brain-metal-retry", in: host)
    try await waitForRetryAttempts(recorder, count: attemptsBeforeFailure + 1)
    try await waitForAnnouncements(recorder, count: 2)
    _ = try await waitForMountedSurfaces(
        recorder,
        required: [.metalFallbackNotice, .metalRetryButton, .memoryTable],
        forbidden: [.memoryMRI]
    )

    recorder.servesSuccessRenderer = true
    let attemptsBeforeSuccess = recorder.bootstrapAttempts
    try await pressAccessibilityElement(identifier: "brain-metal-retry", in: host)
    try await waitForRetryAttempts(recorder, count: attemptsBeforeSuccess + 1)
    try await waitForAnnouncements(recorder, count: 3)
    let surfaces = try await waitForMountedSurfaces(
        recorder,
        required: [.memoryMRI],
        forbidden: [.metalFallbackNotice, .metalRetryButton, .memoryTable]
    )
    let metalSurface = try await waitForNativeMetalSurface(
        identifier: "brain-memory-metal-surface",
        in: host
    )
    try await waitUntil { window.firstResponder === metalSurface }

    #expect(surfaces.contains(.memoryMRI))
    #expect(model.selectedNodeID == selected.id)
    #expect(recorder.bootstrapAttempts == attemptsBeforeSuccess + 1)
    #expect(recorder.announcements == [
        "Interactive MRI unavailable. Showing Accessible Table.",
        "Interactive MRI is still unavailable. Accessible Table remains active.",
        "Interactive MRI restored.",
    ])
    let mountedIndex = try #require(recorder.events.lastIndex(of: .surface(.memoryMRI, true)))
    let restoredIndex = try #require(recorder.events.lastIndex(of: .announcement("Interactive MRI restored.")))
    #expect(mountedIndex < restoredIndex)
}
}

extension HostedBrainAcceptance {
@MainActor
@Test func hostedBrainAdaptsAcrossNarrowAndExpandedWidthsWithoutLosingSelection() async throws {
    let selected = BrainNode(
        id: "responsive-memory", content: "Responsive native memory", domain: "native",
        confidence: 0.97, status: "committed", memoryType: "fact", createdAt: .now,
        agent: "test", agentLabel: "Test Agent", agentIsRoot: false,
        tags: ["responsive"], corroborationCount: 3
    )
    let related = RelatedMemoryEnvelope(
        id: selected.id, domain: selected.domain, content: selected.content, related: []
    )
    let graph = BrainGraphEnvelope(
        nodes: [selected], edges: [], total: 1, domainCounts: ["native": 1],
        domainLast: nil, continuationRequired: false, projection: nil
    )
    let api = MutationTestAPI(forgetResults: [], graph: graph, related: related)
    let model = BrainViewModel(api: api)
    model.graph = graph
    model.selectedNodeID = selected.id
    model.relatedMemories = related
    let recorder = BrainHostRecorder()
    let view = BrainView(
        model: model,
        rendererBootstrap: { _ in .failure(.rendererInitialization) },
        accessibilityAnnouncer: { recorder.announcements.append($0) },
        surfaceObserver: { surface, mounted in
            recorder.update(surface, mounted: mounted)
        },
        layoutObserver: { recorder.layouts.append($0) }
    )
    let window = NSWindow(
        contentRect: NSRect(x: 0, y: 0, width: 620, height: 540),
        styleMask: [.titled, .resizable], backing: .buffered, defer: false
    )
    let host = NSHostingView(rootView: view)
    window.contentView = host
    window.makeKeyAndOrderFront(nil)
    host.layoutSubtreeIfNeeded()

    let narrow = try await waitForLayoutTier(recorder, tier: .compact)
    let narrowSurfaces = try await waitForMountedSurfaces(
        recorder,
        required: [.compactNavigatorTrigger, .metalFallbackNotice, .metalRetryButton, .memoryTable],
        forbidden: [.inlineNavigator, .memoryMRI]
    )
    #expect(narrow.surfaceMinimumHeight + narrow.trainMinimumHeight <= 540 - (narrow.pagePadding * 2))
    #expect(narrowSurfaces.contains(.compactNavigatorTrigger))
    #expect(model.selectedNodeID == selected.id)

    window.setContentSize(NSSize(width: 1_500, height: 760))
    let expanded = try await waitForLayoutTier(recorder, tier: .expanded)
    let expandedSurfaces = try await waitForMountedSurfaces(
        recorder,
        required: [.inlineNavigator, .metalFallbackNotice, .memoryTable],
        forbidden: [.compactNavigatorTrigger, .memoryMRI]
    )
    #expect(expanded.showsInlineNavigator)
    #expect(expandedSurfaces.contains(.inlineNavigator))
    #expect(model.selectedNodeID == selected.id)
    #expect(model.relatedMemories?.id == selected.id)
}
}

extension HostedBrainAcceptance {
@MainActor
@Test func hostedMetalSurfaceAcceptsNativeFirstResponderFocus() {
    let window = NSWindow(
        contentRect: NSRect(x: 0, y: 0, width: 320, height: 240),
        styleMask: [.borderless],
        backing: .buffered,
        defer: false
    )
    let surface = InteractiveMetalView(frame: NSRect(x: 0, y: 0, width: 320, height: 240), device: nil)
    window.contentView = surface

    #expect(surface.window === window)
    #expect(surface.acceptsFirstResponder)
    #expect(window.makeFirstResponder(surface))
    #expect(window.firstResponder === surface)
}
}

@Test func APIBaseURLRejectsRemoteAndCredentialedOrigins() {
    #expect(SAGEAPIClient.isSafeLoopback(URL(string: "http://127.0.0.1:8080")!))
    #expect(SAGEAPIClient.isSafeLoopback(URL(string: "https://[::1]:8443")!))
    #expect(!SAGEAPIClient.isSafeLoopback(URL(string: "https://sage.example:8443")!))
    #expect(!SAGEAPIClient.isSafeLoopback(URL(string: "http://user:secret@127.0.0.1:8080")!))
    #expect(!SAGEAPIClient.isSafeLoopback(URL(string: "file:///tmp/sage")!))
    #expect(!SAGEAPIClient.isSafeLoopback(URL(string: "http://127.0.0.1:8080/admin")!))
    #expect(!SAGEAPIClient.isSafeLoopback(URL(string: "http://127.0.0.1:8080/?next=remote")!))
}

@Test func shellControlRequiresCompatibleReadyContract() throws {
    let generation = String(repeating: "A", count: 43)
    let ready = ShellControlStatus(
        controlProtocol: 1,
        daemonVersion: "12.0.0-beta.1",
        apiSchema: 1,
        minimumShellProtocol: 1,
        maximumShellProtocol: 1,
        instanceGeneration: generation,
        state: .ready,
        uiOrigin: "http://127.0.0.1:49152/",
        startupProof: nil
    )
    try ShellControlClient.validate(ready)

    let prematureOrigin = ShellControlStatus(
        controlProtocol: 1,
        daemonVersion: "12.0.0-beta.1",
        apiSchema: 1,
        minimumShellProtocol: 1,
        maximumShellProtocol: 1,
        instanceGeneration: generation,
        state: .starting,
        uiOrigin: "http://127.0.0.1:49152/",
        startupProof: nil
    )
    #expect(throws: ShellControlError.self) {
        try ShellControlClient.validate(prematureOrigin)
    }
}

@Test func SSEFramesPreserveNamedMultilineEventsAndIgnoreHeartbeats() {
    var parser = SSEEventAccumulator()
    #expect(parser.consume(": heartbeat") == nil)
    #expect(parser.consume("event: consensus") == nil)
    #expect(parser.consume("data: {\"height\":") == nil)
    #expect(parser.consume("data: 42}") == nil)
    let event = parser.consume("", receivedAt: Date(timeIntervalSince1970: 1))
    #expect(event?.name == "consensus")
    #expect(event?.data == "{\"height\":\n42}")
    #expect(parser.consume("") == nil)
}

@Test func overviewPayloadsDecodeFromCurrentWireShapes() throws {
    let healthData = Data(#"""
    {
      "sage":"running","version":"12.0.0-beta.1","encrypted":false,
      "vault_locked":false,"uptime":"1h2m3s",
      "chain":{"block_height":"42","block_time":"2026-08-23T00:00:00Z",
        "catching_up":false,"chain_id":"sage-test","moniker":"Mac",
        "app_version":"27","app_hash":"abcdef","mempool_txs":"0","peers":"2","idle":true},
      "embedder":{"provider":"hash","model":"hash-v1","dimension":768,
        "ready":true,"semantic":false,"online":true,
        "reranker":{"enabled":false,"model":""}}
    }
    """#.utf8)
    let statsData = Data(#"""
    {
      "total_memories":12,"by_domain":{"general":10,"work":2},
      "by_status":{"committed":12},"by_agent":{"agent-a":12},
      "db_size_bytes":4096,"last_activity":"2026-08-23T00:00:00Z"
    }
    """#.utf8)

    let decoder = JSONDecoder.sageDashboard()
    let health = try decoder.decode(DashboardHealth.self, from: healthData)
    let stats = try decoder.decode(DashboardStats.self, from: statsData)

    #expect(health.chain?.blockHeight == "42")
    #expect(health.chain?.peers == 2)
    #expect(health.embedder?.dimension == 768)
    #expect(stats.totalMemories == 12)
    #expect(stats.byDomain["work"] == 2)
}

@Test func memorySearchPayloadAndQueryMatchDashboardContract() throws {
    let data = Data(#"""
    {"memories":[{"memory_id":"m1","submitting_agent":"a1","content":"Native memory","content_hash":"AQID","memory_type":"fact","domain_tag":"architecture","provider":"openai","confidence_score":0.91,"status":"committed","created_at":"2026-08-23T00:00:00Z","corroboration_count":2}],"total":1,"limit":100,"offset":0,"author_labels":{"a1":"CEREBRUM Root"},"projection":{"complete":true,"partial":false,"verified_only":false,"state":"ready","hidden_count":0}}
    """#.utf8)
    let envelope = try JSONDecoder.sageDashboard().decode(MemoryListEnvelope.self, from: data)
    #expect(envelope.memories.first?.memoryID == "m1")
    #expect(envelope.memories.first?.confidenceScore == 0.91)
    #expect(envelope.authorLabels?["a1"] == "CEREBRUM Root")
    #expect(envelope.projection?.partial == false)

    let query = MemoryListQuery(text: "native", domain: "architecture", status: "committed", tag: "release", agent: "a1", sort: .confidence)
    let values = Dictionary(uniqueKeysWithValues: query.queryItems.compactMap { item in item.value.map { (item.name, $0) } })
    #expect(values["q"] == "native")
    #expect(values["domain"] == "architecture")
    #expect(values["status"] == "committed")
    #expect(values["limit"] == "200")
    #expect(values["sort"] == "confidence")

    let tags = try JSONDecoder().decode(TagEnvelope.self, from: Data(#"{"tags":[{"tag":"release","count":12}],"partial":false}"#.utf8))
    #expect(tags.tags.first?.tag == "release")
    #expect(tags.tags.first?.count == 12)

    let pending = try JSONDecoder().decode(MemoryMutationResponse.self, from: Data(#"{"status":"confirmation_pending","message":"Still confirming"}"#.utf8))
    let challenged = try JSONDecoder().decode(MemoryMutationResponse.self, from: Data(#"{"status":"challenge_opened"}"#.utf8))
    #expect(pending.status == "confirmation_pending")
    #expect(challenged.status == "challenge_opened")
    #expect(SearchViewModel.normalizeTag("  Native UI / V12!  ") == "native-ui-v12")
}

@MainActor
@Test func forgetRunsSequentiallyAndStopsAfterUncertainCommit() async {
    let api = MutationTestAPI(forgetResults: [
        .success(.init(status: "deprecated", message: nil)),
        .success(.init(status: "confirmation_pending", message: "Still confirming")),
        .success(.init(status: "deprecated", message: nil)),
    ])
    let model = SearchViewModel(api: api)
    await model.forget(ids: ["m1", "m2", "m3"])

    #expect(await api.forgetCalls == ["m1", "m2"])
    #expect(await api.maximumConcurrentForgetCalls == 1)
    #expect(model.operationTone == .warning)
    #expect(model.operationMessage?.contains("1 not attempted") == true)
}

@Test func brainGraphPayloadAndQueryMatchDashboardContract() throws {
    let data = Data(#"""
    {"nodes":[{"id":"m1","content":"A native memory","domain":"native","confidence":0.95,"status":"committed","memory_type":"fact","created_at":"2026-08-23T00:00:00Z","agent":"root","agent_label":"CEREBRUM Root","agent_is_root":true,"tags":["v12"],"corroboration_count":3}],"edges":[{"source":"m1","target":"m1","type":"related"}],"total":12000,"domain_counts":{"native":5000},"domain_last":{"native":"2026-08-23T00:00:00Z"},"continuation_required":true,"projection":{"complete":false,"partial":true,"verified_only":false,"state":"quarantined","hidden_count":2}}
    """#.utf8)
    let graph = try JSONDecoder.sageDashboard().decode(BrainGraphEnvelope.self, from: data)
    #expect(graph.nodes.first?.id == "m1")
    #expect(graph.nodes.first?.corroborationCount == 3)
    #expect(graph.edges.first?.type == "related")
    #expect(graph.edges.first?.lastFired == nil)
    #expect(graph.total == 12_000)
    #expect(graph.continuationRequired == true)
    #expect(graph.projection?.partial == true)

    let query = BrainGraphQuery(limit: 1_500, status: "all", domain: "native")
    let values = Dictionary(uniqueKeysWithValues: query.queryItems.compactMap { item in item.value.map { (item.name, $0) } })
    #expect(values == ["limit": "1500", "status": "all", "domain": "native"])
}

@Test func connectomeEngramAndRelatedPayloadsMatchDashboardContracts() throws {
    let decoder = JSONDecoder.sageDashboard()
    let connectome = try decoder.decode(ConnectomeEnvelope.self, from: Data(#"""
    {"neurons":[{"agent_id":"a1","name":"Agent One","role":"member","domain":"native"}],"synapses":[{"from_agent":"a1","to_agent":"a1","count":14,"last_fired":"2026-08-23T10:00:00.123456789Z"}]}
    """#.utf8))
    #expect(connectome.neurons.first?.agentID == "a1")
    #expect(connectome.synapses.first?.count == 14)
    #expect(connectome.synapses.first?.lastFiredDate != nil)

    let graphEdge = try decoder.decode(BrainEdge.self, from: Data(#"""
    {"source":"a1","target":"a2","type":"synapse","weight":14,"last_fired":"2026-08-23T10:00:00.123456789Z"}
    """#.utf8))
    #expect(graphEdge.lastFired == connectome.synapses.first?.lastFiredDate)

    let engrams = try decoder.decode(AgentEngramEnvelope.self, from: Data(#"""
    {"agent_id":"a1","engrams":[{"id":"m1","content":"Visible memory","domain":"native","confidence":0.93,"status":"committed","memory_type":"fact","created_at":"2026-08-23T10:00:00Z","corroboration_count":9,"tags":["v12"],"corroborators":["a2"]}],"continuation_required":true}
    """#.utf8))
    #expect(engrams.agentID == "a1")
    #expect(engrams.engrams.first?.corroborationCount == 9)
    #expect(engrams.engrams.first?.corroborators == ["a2"])

    let related = try decoder.decode(RelatedMemoryEnvelope.self, from: Data(#"""
    {"id":"m1","domain":"native","content":"Anchor","related":[{"id":"m2","content":"Related","domain":"native","confidence":0.8,"corroboration_count":2,"status":"committed","created_at":"2026-08-22T10:00:00Z","memory_type":"observation","kind":"do","relation":"same-topic","score":4.25}]}
    """#.utf8))
    #expect(related.related.first?.kind == "do")
    #expect(related.related.first?.relation == "same-topic")
}

@MainActor
@Test func accessEventsPurgeSensitiveNativeSnapshotsBeforeRefetch() async throws {
    let api = MutationTestAPI(forgetResults: [])
    let search = SearchViewModel(api: api)
    search.memories = try JSONDecoder.sageDashboard().decode([MemorySummary].self, from: Data(#"""
    [{"memory_id":"m1","submitting_agent":"a1","content":"Sensitive","content_hash":"h","memory_type":"fact","domain_tag":"private","provider":"local","confidence_score":0.9,"status":"committed","created_at":"2026-08-23T00:00:00Z","corroboration_count":0}]
    """#.utf8))
    search.selection = ["m1"]
    search.inspectedMemoryID = "m1"
    search.inspectedTags = ["private"]
    search.authorLabels = ["a1": "Agent One"]
    search.handleLiveEvent(.init(name: "access", data: "", receivedAt: .now))
    #expect(search.memories.isEmpty)
    #expect(search.selection.isEmpty)
    #expect(search.inspectedMemoryID == nil)
    #expect(search.inspectedTags.isEmpty)
    #expect(search.authorLabels.isEmpty)

    let brain = BrainViewModel(api: api)
    brain.graph = .init(nodes: [], edges: [], total: 1, domainCounts: [:], domainLast: [:], continuationRequired: false, projection: nil)
    brain.connectome = .init(neurons: [], synapses: [])
    brain.selectedNodeID = "m1"
    brain.relatedMemoryFocus = .init(anchorMemoryID: "m1", relatedMemoryID: "m2")
    brain.selectedAgentID = "a1"
    brain.selectedEngramID = "m1"
    brain.selectedConnection = .init(fromAgent: "a1", toAgent: "a2", count: 1, lastFired: "2026-08-23T00:00:00Z")
    brain.isDetailLoading = true
    brain.detailErrorMessage = "old authorization"
    brain.handleLiveEvent(.init(name: "access", data: "", receivedAt: .now))
    #expect(brain.graph == nil)
    #expect(brain.connectome == nil)
    #expect(brain.selectedNodeID == nil)
    #expect(brain.relatedMemoryFocus == nil)
    #expect(brain.selectedAgentID == nil)
    #expect(brain.selectedEngramID == nil)
    #expect(brain.selectedConnection == nil)
    #expect(!brain.isDetailLoading)
    #expect(brain.detailErrorMessage == nil)
}

@MainActor
@Test func relatedMemoryFocusPreservesItsTypedAnchorAndReconcilesOnRefresh() async {
    let related = RelatedMemory(
        id: "m2", content: "Related", domain: "native", confidence: 0.8,
        corroborationCount: 2, status: "committed", createdAt: .now,
        memoryType: "observation", kind: "do", relation: "same-topic", score: 4.25
    )
    let envelope = RelatedMemoryEnvelope(id: "m1", domain: "native", content: "Anchor", related: [related])
    let api = MutationTestAPI(forgetResults: [], related: envelope)
    let brain = BrainViewModel(api: api)
    brain.graph = .init(
        nodes: [
            .init(id: "m1", content: "Anchor", domain: "native", confidence: 1, status: "committed", memoryType: "fact", createdAt: .now, agent: "a", agentLabel: nil, agentIsRoot: false, tags: nil, corroborationCount: 1),
            .init(id: "m2", content: "Related", domain: "native", confidence: 0.8, status: "committed", memoryType: "observation", createdAt: .now, agent: "a", agentLabel: nil, agentIsRoot: false, tags: nil, corroborationCount: 2),
        ],
        edges: [], total: 2, domainCounts: ["native": 2], domainLast: nil,
        continuationRequired: false, projection: nil
    )
    brain.selectedNodeID = "m1"
    brain.relatedMemories = envelope

    brain.selectRelatedMemory(related)
    #expect(brain.selectedNodeID == "m1")
    #expect(brain.relatedMemoryFocus == .init(anchorMemoryID: "m1", relatedMemoryID: "m2"))
    #expect(brain.selectedRelatedMemory?.id == "m2")
    #expect(brain.sceneFocusedMemoryID == "m2")

    await brain.loadRelatedForSelection()
    #expect(brain.relatedMemoryFocus?.relatedMemoryID == "m2")

    brain.selectRelatedMemory(related)
    #expect(brain.relatedMemoryFocus == nil)
    #expect(brain.sceneFocusedMemoryID == "m1")

    brain.selectRelatedMemory(related)
    await api.setRelated(.init(id: "m1", domain: "native", content: "Anchor", related: []))
    await brain.loadRelatedForSelection()
    #expect(brain.relatedMemoryFocus == nil)

    brain.relatedMemories = envelope
    brain.selectRelatedMemory(related)
    brain.selectedNodeID = "m2"
    #expect(brain.relatedMemoryFocus == nil)
    #expect(brain.relatedMemories == nil)
}

@MainActor
@Test func connectomeFocusUsesTypedSceneIDsAndPreservesItsAgentAnchor() async {
    let connectome = ConnectomeEnvelope(
        neurons: [
            .init(agentID: "shared-id", name: "Agent One", role: "member", domain: "native"),
            .init(agentID: "a2", name: "Agent Two", role: "member", domain: "native"),
        ],
        synapses: [
            .init(fromAgent: "shared-id", toAgent: "a2", count: .max, lastFired: "2026-08-23T00:00:00Z"),
            .init(fromAgent: "a2", toAgent: "shared-id", count: .max, lastFired: "2026-08-23T00:01:00Z"),
        ]
    )
    let engrams = AgentEngramEnvelope(
        agentID: "shared-id",
        engrams: [.init(id: "shared-id", content: "Collision-safe memory", domain: "native", confidence: 0.9,
                        status: "committed", memoryType: "fact", createdAt: .now, corroborationCount: 1,
                        tags: nil, corroborators: ["a2"])],
        continuationRequired: false,
        projection: nil
    )
    let api = MutationTestAPI(forgetResults: [], connectome: connectome, engrams: engrams)
    let brain = BrainViewModel(api: api)
    brain.mode = .connectome
    await brain.refresh()
    brain.selectConnectomeSceneNode("agent:shared-id")
    await brain.loadEngramsForSelection()

    #expect(Set(brain.connectomeSceneNodes.map(\.id)).contains("agent:shared-id"))
    #expect(Set(brain.connectomeSceneNodes.map(\.id)).contains("engram:shared-id"))
    let firstSynapseEdge = brain.connectomeSceneEdges.first {
        $0.source == "agent:shared-id" && $0.target == "agent:a2" && $0.type == "synapse"
    }
    #expect(firstSynapseEdge?.weight == Double(Int64.max))
    #expect(firstSynapseEdge?.lastFired == connectome.synapses[0].lastFiredDate)
    #expect(BrainMetalRenderer.isSameDirectedEdge(
        brain.selectedConnectionEdge,
        .init(source: "agent:shared-id", target: "agent:a2", type: "synapse", weight: 42)
    ) == false)
    #expect(brain.totalTraffic(for: "shared-id") == .max)

    brain.selectConnectomeSceneNode("engram:shared-id")
    #expect(brain.selectedAgentID == "shared-id")
    #expect(brain.selectedEngramID == "shared-id")
    brain.selectConnectomeSceneNode(nil)
    #expect(brain.selectedAgentID == "shared-id")
    #expect(brain.selectedEngramID == nil)
    brain.selectConnectomeSceneNode(nil)
    #expect(brain.selectedAgentID == nil)

    brain.selectedAgentID = "shared-id"
    brain.selectedConnection = connectome.synapses[0]
    #expect(brain.selectedConnectionEdge?.lastFired == connectome.synapses[0].lastFiredDate)
    #expect(BrainMetalRenderer.isSameDirectedEdge(
        brain.selectedConnectionEdge,
        .init(source: "agent:shared-id", target: "agent:a2", type: "synapse", weight: 42)
    ))
    brain.selectConnectomeAgent("shared-id")
    #expect(brain.selectedAgentID == "shared-id")
    #expect(brain.selectedConnectionID == nil)
    brain.selectedConnection = nil
    brain.selectedAgentID = nil
    brain.selectConnectomeSceneEdge(.init(source: "agent:shared-id", target: "agent:a2", type: "synapse"))
    #expect(brain.selectedAgentID == "shared-id")
    #expect(brain.selectedConnectionID == .init(fromAgent: "shared-id", toAgent: "a2"))
    brain.selectConnectomeSceneEdge(.init(source: "agent:shared-id", target: "agent:a2", type: "synapse"))
    #expect(brain.selectedConnectionID == nil)
    brain.selectConnectomeSceneEdge(.init(source: "engram:shared-id", target: "agent:a2", type: "corroborates"))
    #expect(brain.selectedConnectionID == nil)
    brain.selectedConnection = connectome.synapses[0]
    let updated = ConnectomeEnvelope(neurons: connectome.neurons, synapses: [
        .init(fromAgent: "shared-id", toAgent: "a2", count: 7, lastFired: "2026-08-23T00:05:00Z"),
    ])
    await api.setConnectome(updated)
    await brain.refresh()
    #expect(brain.selectedConnection?.count == 7)

    brain.mode = .memory
    #expect(!brain.hasVisibleInspector)
    brain.graph = .init(
        nodes: [.init(id: "m1", content: "Memory", domain: "native", confidence: 1, status: "committed",
                      memoryType: "fact", createdAt: .now, agent: "shared-id", agentLabel: nil,
                      agentIsRoot: false, tags: nil, corroborationCount: 0)],
        edges: [], total: 1, domainCounts: ["native": 1], domainLast: nil,
        continuationRequired: false, projection: nil
    )
    brain.selectedNodeID = "m1"
    #expect(brain.hasVisibleInspector)
    brain.mode = .connectome
    #expect(brain.hasVisibleInspector)
}

@MainActor
@Test func connectomeCanonicalizationPrecedesCapsAndIsPermutationStable() async {
    let neurons = [
        ConnectomeNeuron(agentID: "a", name: "Zulu", role: "member", domain: "z"),
        ConnectomeNeuron(agentID: "a", name: "Alpha", role: "member", domain: "a"),
        ConnectomeNeuron(agentID: "b", name: "Beta", role: "member", domain: "b"),
    ]
    let synapses = [
        ConnectomeSynapse(fromAgent: "a", toAgent: "b", count: 3, lastFired: ""),
        ConnectomeSynapse(fromAgent: "a", toAgent: "b", count: 9, lastFired: "2026-08-23T00:05:00Z"),
    ]
    let api = MutationTestAPI(
        forgetResults: [], connectome: .init(neurons: Array(neurons.reversed()), synapses: Array(synapses.reversed()))
    )
    let brain = BrainViewModel(api: api)
    brain.mode = .connectome
    await brain.refresh()
    #expect(brain.connectome?.neurons.count == 2)
    #expect(brain.connectome?.neurons.first { $0.agentID == "a" }?.name == "Alpha")
    #expect(brain.connectome?.synapses.count == 1)
    #expect(brain.connectome?.synapses.first?.count == 9)
    #expect(brain.connectome?.synapses.first?.lastFired == "2026-08-23T00:05:00Z")
}

@Test func bloomTexturePlanRoundsUpAndRejectsEmptyDrawables() {
    #expect(BrainBloomTexturePlan(drawableWidth: 1, drawableHeight: 1) == .init(drawableWidth: 1, drawableHeight: 1))
    #expect(BrainBloomTexturePlan(drawableWidth: 5, drawableHeight: 7) == .init(drawableWidth: 6, drawableHeight: 8))
    #expect(BrainBloomTexturePlan(drawableWidth: 5, drawableHeight: 7)?.width == 3)
    #expect(BrainBloomTexturePlan(drawableWidth: 5, drawableHeight: 7)?.height == 4)
    #expect(BrainBloomTexturePlan(drawableWidth: 0, drawableHeight: 7) == nil)
    #expect(BrainBloomTexturePlan(drawableWidth: 7, drawableHeight: 0) == nil)
}

@Test func connectomePlasticityUsesThirtyMinuteHalfLifeAndHonestFloor() {
    let now = Date(timeIntervalSince1970: 10_000)
    #expect(BrainMetalRenderer.edgePlasticity(lastFired: nil, now: now) == 0.15)
    #expect(BrainMetalRenderer.edgePlasticity(lastFired: now, now: now) == 1)
    let halfLife = BrainMetalRenderer.edgePlasticity(lastFired: now.addingTimeInterval(-1_800), now: now)
    #expect(abs(halfLife - 0.575) < 0.0001)
    let old = BrainMetalRenderer.edgePlasticity(lastFired: now.addingTimeInterval(-86_400), now: now)
    #expect(old >= 0.15 && old < 0.151)
}

@Test func metalFlowABIClockAndSemanticPhaseAreStable() {
    #expect(BrainMetalRenderer.metalABIStrides["vertex"] == 32)
    #expect(BrainMetalRenderer.metalABIStrides["ribbon"] == 96)
    #expect(BrainMetalRenderer.metalABIStrides["flow"] == 80)
    #expect(BrainMetalRenderer.metalABIStrides["uniforms"] == 96)
    #expect(BrainMetalRenderer.metalABILayouts["vertex"] == [32, 32, 16])
    #expect(BrainMetalRenderer.metalABILayouts["ribbon"] == [92, 96, 16])
    #expect(BrainMetalRenderer.metalABILayouts["flow"] == [80, 80, 16])
    #expect(BrainMetalRenderer.metalABILayouts["uniforms"] == [88, 96, 16])

    #expect(BrainMetalRenderer.flowProgress(time: 0, phase: 0.25) == 0.25)
    #expect(abs(BrainMetalRenderer.flowProgress(time: 10, phase: 0.2, speed: 0.1) - 0.2) < 0.0001)
    #expect(abs(BrainMetalRenderer.flowProgress(time: -1, phase: 0, speed: 0.25) - 0.75) < 0.0001)
    #expect(abs(BrainMetalRenderer.flowProgress(time: 100, phase: 0.37) - 0.37) < 0.0001)

    let synapse = BrainEdge(source: "a", target: "b", type: "synapse")
    let engram = BrainEdge(source: "a", target: "b", type: "engram")
    let first = BrainMetalRenderer.flowPhase(for: synapse)
    #expect(first >= 0 && first < 1)
    #expect(first == BrainMetalRenderer.flowPhase(for: synapse))
    #expect(first != BrainMetalRenderer.flowPhase(for: engram))
}

@Test func ribbonLODIsBoundedDeterministicAndPreservesSelection() {
    let edges = (0 ..< 10).map {
        BrainEdge(source: "agent:\($0)", target: "agent:\($0 + 1)", type: "synapse", weight: Double($0))
    }
    let highlighted = edges[0]
    let visible = Set(edges.flatMap { [$0.source, $0.target] })
    let policy = BrainEdgeLODPolicy(maximumEdges: 4)
    let first = BrainEdgeLOD.select(
        edges, visibleNodeIDs: visible, highlighted: highlighted,
        selectedAgentSceneID: nil, policy: policy
    )
    let second = BrainEdgeLOD.select(
        Array(edges.reversed()), visibleNodeIDs: visible, highlighted: highlighted,
        selectedAgentSceneID: nil, policy: policy
    )
    #expect(first.count == 4)
    #expect(first.contains { BrainMetalRenderer.isSameDirectedEdge(highlighted, $0) })
    #expect(first.map(edgeIdentity) == second.map(edgeIdentity))
}

@Test func ribbonLODIsStableBelowCapAndCanonicalizesDuplicateIdentities() {
    let recent = Date(timeIntervalSince1970: 2_000)
    let edges = [
        BrainEdge(source: "a", target: "b", type: "synapse", weight: 3),
        BrainEdge(source: "b", target: "c", type: "synapse", weight: 7),
        BrainEdge(source: "a", target: "b", type: "synapse", weight: 9, lastFired: recent),
        BrainEdge(source: "c", target: "a", type: "synapse", weight: .infinity),
        BrainEdge(source: "hidden", target: "a", type: "synapse", weight: 100),
    ]
    let visible: Set<String> = ["a", "b", "c"]
    let policy = BrainEdgeLODPolicy(maximumEdges: 20)
    let forward = BrainEdgeLOD.select(
        edges, visibleNodeIDs: visible, highlighted: nil,
        selectedAgentSceneID: nil, policy: policy
    )
    let reversed = BrainEdgeLOD.select(
        Array(edges.reversed()), visibleNodeIDs: visible, highlighted: nil,
        selectedAgentSceneID: nil, policy: policy
    )
    #expect(forward.map(edgeIdentity) == reversed.map(edgeIdentity))
    #expect(forward.count == 3)
    #expect(forward.first { $0.source == "a" && $0.target == "b" }?.weight == 9)
    #expect(forward.first { $0.source == "a" && $0.target == "b" }?.lastFired == recent)
    #expect(!forward.contains { $0.source == "hidden" })

    let unknown = BrainEdgeLOD.select(
        [
            BrainEdge(source: "a", target: "b", type: "engram"),
            BrainEdge(source: "a", target: "b", type: "engram"),
        ],
        visibleNodeIDs: visible, highlighted: nil, selectedAgentSceneID: nil,
        policy: policy
    )
    #expect(unknown.first?.lastFired == nil)
}

@Test func ribbonLODReservesSelectedDirectionsAndGeneralTopology() {
    let selectedEdges = [
        BrainEdge(source: "selected", target: "out-1", type: "synapse", weight: 1),
        BrainEdge(source: "selected", target: "out-2", type: "synapse", weight: 2),
        BrainEdge(source: "in-1", target: "selected", type: "synapse", weight: 3),
        BrainEdge(source: "in-2", target: "selected", type: "synapse", weight: 4),
    ]
    let overview = (0 ..< 12).map {
        BrainEdge(source: "hub", target: "peer-\($0)", type: "synapse", weight: Double(100 - $0))
    }
    let edges = selectedEdges + overview
    let visible = Set(edges.flatMap { [$0.source, $0.target] })
    let policy = BrainEdgeLODPolicy(
        maximumEdges: 8, incomingPerVisibleNode: 1, outgoingPerVisibleNode: 1,
        selectedIncoming: 2, selectedOutgoing: 2
    )
    let selected = BrainEdgeLOD.select(
        edges, visibleNodeIDs: visible, highlighted: nil,
        selectedAgentSceneID: "selected", policy: policy
    )
    #expect(selected.count == 8)
    #expect(selectedEdges.allSatisfy { edge in selected.contains { edgeIdentity($0) == edgeIdentity(edge) } })
    #expect(selected.contains { $0.source == "hub" })
}

private func edgeIdentity(_ edge: BrainEdge) -> String {
    "\(edge.source)\u{0}\(edge.target)\u{0}\(edge.type)"
}

@MainActor
private final class BrainHostRecorder {
    enum Event: Equatable {
        case announcement(String)
        case surface(BrainMountedSurface, Bool)
    }

    var announcements: [String] = []
    var layouts: [BrainResponsiveLayoutPlan] = []
    var bootstrapAttempts = 0
    var retryBootstrapAttempts = 0
    var retryBootstrapReturned = false
    var servesSuccessRenderer = false
    var successRenderer: BrainMetalRenderer?
    var events: [Event] = []
    private var surfaceCounts: [BrainMountedSurface: Int] = [:]

    var surfaces: Set<BrainMountedSurface> {
        Set(surfaceCounts.compactMap { $0.value > 0 ? $0.key : nil })
    }

    func update(_ surface: BrainMountedSurface, mounted: Bool) {
        surfaceCounts[surface] = max(0, (surfaceCounts[surface] ?? 0) + (mounted ? 1 : -1))
        events.append(.surface(surface, mounted))
    }

    func recordAnnouncement(_ announcement: String) {
        announcements.append(announcement)
        events.append(.announcement(announcement))
    }
}

@MainActor
private final class BrainRetryGate {
    private var continuation: CheckedContinuation<Void, Never>?

    var isWaiting: Bool { continuation != nil }

    func wait() async {
        await withCheckedContinuation { continuation = $0 }
    }

    func open() {
        continuation?.resume()
        continuation = nil
    }
}

private enum HostedBrainTestError: Error {
    case surfacesDidNotMount(Set<BrainMountedSurface>)
    case layoutTierDidNotMount(BrainResponsiveTier)
    case accessibilityElementDidNotPress(String)
    case retryAttemptsDidNotReach(Int)
    case announcementCountDidNotReach(Int)
    case nativeAccessibilityButtonDidNotAppear(String)
    case nativeMetalSurfaceDidNotAppear(String)
    case conditionDidNotBecomeTrue
}

@MainActor
private func waitUntil(
    timeout: Duration = .seconds(6),
    condition: @MainActor () -> Bool
) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if condition() { return }
        try await Task.sleep(for: .milliseconds(10))
    }
    if condition() { return }
    throw HostedBrainTestError.conditionDidNotBecomeTrue
}

@MainActor
private func pressAccessibilityElement(
    identifier: String,
    in root: NSView,
    timeout: Duration = .seconds(3)
) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        var visited: Set<ObjectIdentifier> = []
        if findAndPressAccessibilityElement(identifier: identifier, candidate: root, visited: &visited) {
            return
        }
        try await Task.sleep(for: .milliseconds(10))
    }
    throw HostedBrainTestError.accessibilityElementDidNotPress(identifier)
}

@MainActor
private func findAndPressAccessibilityElement(
    identifier: String,
    candidate: Any,
    visited: inout Set<ObjectIdentifier>
) -> Bool {
    guard let object = candidate as? NSObject else { return false }
    let identity = ObjectIdentifier(object)
    guard visited.insert(identity).inserted else { return false }

    if let view = object as? NSView {
        if view.accessibilityIdentifier() == identifier || view.identifier?.rawValue == identifier {
            guard view is NSButton else { return false }
            return view.accessibilityPerformPress()
        }
        for child in view.subviews {
            if findAndPressAccessibilityElement(identifier: identifier, candidate: child, visited: &visited) {
                return true
            }
        }
        for child in view.accessibilityChildren() ?? [] {
            if findAndPressAccessibilityElement(identifier: identifier, candidate: child, visited: &visited) {
                return true
            }
        }
    } else if let element = object as? NSAccessibilityElement {
        if element.accessibilityIdentifier() == identifier {
            guard element.accessibilityRole() == .button else { return false }
            return element.accessibilityPerformPress()
        }
        for child in element.accessibilityChildren() ?? [] {
            if findAndPressAccessibilityElement(identifier: identifier, candidate: child, visited: &visited) {
                return true
            }
        }
    }
    return false
}

@MainActor
private func waitForNativeAccessibilityButton(
    identifier: String,
    in root: NSView,
    timeout: Duration = .seconds(6),
    where predicate: @MainActor (NSButton) -> Bool = { _ in true }
) async throws -> NSButton {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if let button = findNativeAccessibilityButton(identifier: identifier, in: root), predicate(button) {
            return button
        }
        try await Task.sleep(for: .milliseconds(10))
    }
    if let button = findNativeAccessibilityButton(identifier: identifier, in: root), predicate(button) {
        return button
    }
    throw HostedBrainTestError.nativeAccessibilityButtonDidNotAppear(identifier)
}

@MainActor
private func findNativeAccessibilityButton(identifier: String, in root: NSView) -> NSButton? {
    if let button = root as? NSButton,
       button.accessibilityIdentifier() == identifier || button.identifier?.rawValue == identifier {
        return button
    }
    for child in root.subviews {
        if let button = findNativeAccessibilityButton(identifier: identifier, in: child) {
            return button
        }
    }
    return nil
}

@MainActor
private func waitForNativeMetalSurface(
    identifier: String,
    in root: NSView,
    timeout: Duration = .seconds(6)
) async throws -> InteractiveMetalView {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if let surface = findNativeMetalSurface(identifier: identifier, in: root) {
            return surface
        }
        try await Task.sleep(for: .milliseconds(10))
    }
    if let surface = findNativeMetalSurface(identifier: identifier, in: root) {
        return surface
    }
    throw HostedBrainTestError.nativeMetalSurfaceDidNotAppear(identifier)
}

@MainActor
private func findNativeMetalSurface(identifier: String, in root: NSView) -> InteractiveMetalView? {
    if let surface = root as? InteractiveMetalView,
       surface.accessibilityIdentifier() == identifier || surface.identifier?.rawValue == identifier {
        return surface
    }
    for child in root.subviews {
        if let surface = findNativeMetalSurface(identifier: identifier, in: child) {
            return surface
        }
    }
    return nil
}

@MainActor
private func waitForRetryAttempts(
    _ recorder: BrainHostRecorder,
    count: Int,
    timeout: Duration = .seconds(3)
) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if recorder.bootstrapAttempts >= count { return }
        try await Task.sleep(for: .milliseconds(10))
    }
    throw HostedBrainTestError.retryAttemptsDidNotReach(count)
}

@MainActor
private func waitForAnnouncements(
    _ recorder: BrainHostRecorder,
    count: Int,
    timeout: Duration = .seconds(3)
) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if recorder.announcements.count >= count { return }
        try await Task.sleep(for: .milliseconds(10))
    }
    throw HostedBrainTestError.announcementCountDidNotReach(count)
}

@MainActor
private func waitForLayoutTier(
    _ recorder: BrainHostRecorder,
    tier: BrainResponsiveTier,
    timeout: Duration = .seconds(3)
) async throws -> BrainResponsiveLayoutPlan {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if let layout = recorder.layouts.last, layout.tier == tier { return layout }
        try await Task.sleep(for: .milliseconds(10))
    }
    throw HostedBrainTestError.layoutTierDidNotMount(tier)
}

@MainActor
private func waitForMountedSurfaces(
    _ recorder: BrainHostRecorder,
    required: Set<BrainMountedSurface>,
    forbidden: Set<BrainMountedSurface> = [],
    timeout: Duration = .seconds(6)
) async throws -> Set<BrainMountedSurface> {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if required.isSubset(of: recorder.surfaces), forbidden.isDisjoint(with: recorder.surfaces) {
            return recorder.surfaces
        }
        try await Task.sleep(for: .milliseconds(10))
    }
    if required.isSubset(of: recorder.surfaces), forbidden.isDisjoint(with: recorder.surfaces) {
        return recorder.surfaces
    }
    throw HostedBrainTestError.surfacesDidNotMount(recorder.surfaces)
}

private actor MutationTestAPI: SAGEAPI {
    var forgetResults: [Result<MemoryMutationResponse, SAGEAPIError>]
    var forgetCalls: [String] = []
    var concurrentForgetCalls = 0
    var maximumConcurrentForgetCalls = 0
    var brainConnectome: ConnectomeEnvelope
    var brainMemoryGraph: BrainGraphEnvelope
    let searchProjection: MemoryProjection?
    let eventStreamError: SAGEAPIError?
    let brainEngrams: AgentEngramEnvelope?
    var brainRelated: RelatedMemoryEnvelope?
    var searchMemories: [MemorySummary]
    let memoryTagsByID: [String: [String]]
    let memoryTagDelays: [String: Duration]

    init(
        forgetResults: [Result<MemoryMutationResponse, SAGEAPIError>],
        graph: BrainGraphEnvelope = .init(
            nodes: [], edges: [], total: 0, domainCounts: [:], domainLast: [:],
            continuationRequired: false, projection: nil
        ),
        connectome: ConnectomeEnvelope = .init(neurons: [], synapses: []),
        engrams: AgentEngramEnvelope? = nil,
        related: RelatedMemoryEnvelope? = nil,
        searchMemories: [MemorySummary] = [],
        memoryTagsByID: [String: [String]] = [:],
        memoryTagDelays: [String: Duration] = [:],
        searchProjection: MemoryProjection? = nil,
        eventStreamError: SAGEAPIError? = nil
    ) {
        self.forgetResults = forgetResults
        self.brainMemoryGraph = graph
        self.brainConnectome = connectome
        self.searchProjection = searchProjection
        self.eventStreamError = eventStreamError
        self.brainEngrams = engrams
        self.brainRelated = related
        self.searchMemories = searchMemories
        self.memoryTagsByID = memoryTagsByID
        self.memoryTagDelays = memoryTagDelays
    }

    func authStatus() async throws -> AuthStatus { .init(authRequired: false, authenticated: true) }
    func login(passphrase: String) async throws -> LoginResult { .init(ok: true, error: nil) }
    func lock() async throws {}
    func health() async throws -> DashboardHealth { throw SAGEAPIError.invalidResponse }
    func stats() async throws -> DashboardStats { throw SAGEAPIError.invalidResponse }
    func agents() async throws -> AgentOverviewEnvelope { .init(agents: []) }
    func validators() async throws -> ValidatorOverview { .init(count: 0, totalVotingPower: "0", validators: [], error: nil) }
    func federation() async throws -> FederationOverview { .disabled }
    func memories(_ query: MemoryListQuery) async throws -> MemoryListEnvelope {
        .init(
            memories: searchMemories, total: searchMemories.count, limit: 100, offset: 0, nextCursor: nil,
            continuationRequired: nil, authorLabels: nil, projection: searchProjection
        )
    }
    func setSearchMemories(_ memories: [MemorySummary]) { searchMemories = memories }
    func tags() async throws -> TagEnvelope { .init(tags: [], partial: false) }
    func memoryTags(id: String) async throws -> MemoryTagsEnvelope {
        if let delay = memoryTagDelays[id] { try await Task.sleep(for: delay) }
        return .init(memoryID: id, tags: memoryTagsByID[id] ?? [])
    }
    func setMemoryTags(id: String, tags: [String]) async throws -> MemoryTagsEnvelope { .init(memoryID: id, tags: tags) }
    func addTag(_ tag: String, to ids: [String]) async throws -> BulkMemoryUpdateResponse { .init(status: "updated", updated: ids.count, total: ids.count) }
    func brainGraph(_ query: BrainGraphQuery) async throws -> BrainGraphEnvelope {
        brainMemoryGraph
    }
    func connectome() async throws -> ConnectomeEnvelope { brainConnectome }
    func setConnectome(_ value: ConnectomeEnvelope) { brainConnectome = value }
    func setRelated(_ value: RelatedMemoryEnvelope?) { brainRelated = value }
    func agentEngrams(agentID: String) async throws -> AgentEngramEnvelope {
        if let brainEngrams, brainEngrams.agentID == agentID { return brainEngrams }
        return .init(agentID: agentID, engrams: [], continuationRequired: false, projection: nil)
    }
    func relatedMemories(memoryID: String, limit: Int) async throws -> RelatedMemoryEnvelope {
        if let brainRelated, brainRelated.id == memoryID { return brainRelated }
        return .init(id: memoryID, domain: "", content: "", related: [])
    }
    func forgetMemory(id: String) async throws -> MemoryMutationResponse {
        forgetCalls.append(id)
        concurrentForgetCalls += 1
        maximumConcurrentForgetCalls = max(maximumConcurrentForgetCalls, concurrentForgetCalls)
        defer { concurrentForgetCalls -= 1 }
        try? await Task.sleep(for: .milliseconds(2))
        return try forgetResults.removeFirst().get()
    }
    func events() async -> AsyncThrowingStream<DashboardEventStreamElement, Error> {
        AsyncThrowingStream { continuation in
            continuation.yield(.state(.connected))
            if let eventStreamError { continuation.finish(throwing: eventStreamError) }
            else { continuation.finish() }
        }
    }
}
