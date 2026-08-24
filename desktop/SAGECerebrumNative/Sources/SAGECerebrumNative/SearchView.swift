import AppKit
import SwiftUI

struct SearchView: View {
    @Environment(\.scenePhase) private var scenePhase
    @State private var model: SearchViewModel
    @State private var showsFilters = false
    @State private var showsBulkTagSheet = false
    @State private var bulkTag = ""
    @State private var forgetConfirmation: ForgetConfirmation?
    @State private var searchIsPresented = false
    @State private var inspectorIsPresented = false
    @State private var focusGeneration = 0
    @FocusState private var keyboardFocus: SearchFocusTarget?
    @AccessibilityFocusState private var accessibilityFocus: SearchFocusTarget?
    private let focusRequestID: UInt64
    private let consumedFocusRequestID: UInt64
    private let onFocusRequestConsumed: (UInt64) -> Void

    init(
        api: any SAGEAPI,
        focusRequestID: UInt64 = 0,
        consumedFocusRequestID: UInt64 = 0,
        onFocusRequestConsumed: @escaping (UInt64) -> Void = { _ in }
    ) {
        _model = State(initialValue: SearchViewModel(api: api))
        self.focusRequestID = focusRequestID
        self.consumedFocusRequestID = consumedFocusRequestID
        self.onFocusRequestConsumed = onFocusRequestConsumed
    }

    var body: some View {
        ZStack {
            CerebrumBackdrop()
            VStack(alignment: .leading, spacing: 18) {
                header
                operationNotice
                updateNotice
                projectionNotice
                resultsSurface
            }
            .padding(CerebrumTheme.pagePadding)
        }
        .navigationTitle("Search")
        .searchable(
            text: $model.query,
            isPresented: $searchIsPresented,
            placement: .toolbar,
            prompt: "Search sovereign memory"
        )
        .focusedSceneValue(\.cerebrumRouteCommandActions, routeCommandActions)
        .toolbar { searchToolbar }
        .inspector(isPresented: Binding(
            get: { inspectorIsPresented && model.inspectedMemoryID != nil },
            set: { if !$0 { hideInspectorAndRestoreFocus() } }
        )) {
            if let memory = model.inspectedMemory {
                VStack(spacing: 0) {
                    HStack {
                        Spacer()
                        Button("Hide Inspector", systemImage: "xmark") {
                            hideInspectorAndRestoreFocus()
                        }
                        .labelStyle(.iconOnly)
                        .help("Hide Inspector")
                        .focused($keyboardFocus, equals: .inspectorClose)
                        .accessibilityFocused($accessibilityFocus, equals: .inspectorClose)
                        .accessibilityLabel("Hide Search inspector")
                        .accessibilityIdentifier("search-inspector-close")
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)

                    Divider()

                    MemoryInspectorView(
                        memory: memory,
                        authorLabel: model.authorLabels[memory.submittingAgent],
                        tags: model.inspectedTags,
                        tagsAreLoading: model.tagsAreLoading,
                        tagsError: model.tagsError,
                        newTag: $model.newTag,
                        isMutating: model.isMutating,
                        onAddTag: { Task { await model.addInspectedTag() } },
                        onRemoveTag: { tag in Task { await model.removeInspectedTag(tag) } },
                        onForget: { requestForget([memory.id]) }
                    )
                }
                    .inspectorColumnWidth(min: 320, ideal: 380, max: 520)
            } else if model.inspectedMemoryID != nil {
                ContentUnavailableView {
                    Label("Updating memory details", systemImage: "clock.arrow.circlepath")
                } description: {
                    Text("CEREBRUM is reconciling this memory with the current results.")
                }
                .inspectorColumnWidth(min: 320, ideal: 380, max: 520)
            }
        }
        .task {
            await model.loadMetadata()
        }
        .task(id: refreshKey) {
            if !model.query.isEmpty { try? await Task.sleep(for: .milliseconds(250)) }
            guard !Task.isCancelled else { return }
            await model.refresh()
        }
        .task(id: scenePhase) {
            guard scenePhase == .active else { return }
            await model.runLiveUpdates()
        }
        .task(id: model.inspectedMemoryID) {
            await model.loadInspectedTags()
        }
        .task(id: focusRequestID) {
            guard focusRequestID > consumedFocusRequestID else { return }
            searchIsPresented = false
            await Task.yield()
            guard !Task.isCancelled else { return }
            searchIsPresented = true
            await Task.yield()
            guard !Task.isCancelled else { return }
            onFocusRequestConsumed(focusRequestID)
        }
        .onChange(of: model.selection) { _, selection in
            applyInspectorState(
                SearchInspectorLifecycle.selectionChanged(from: inspectorState, selection: selection)
            )
        }
        .onChange(of: model.inspectedMemoryID) { _, inspectedMemoryID in
            if inspectedMemoryID == nil, inspectorIsPresented {
                inspectorIsPresented = false
                requestFocus(.results)
                postAccessibilityAnnouncement("The inspected memory is no longer in these results.")
            }
        }
        .onChange(of: model.customFrom) { _, from in
            if model.customTo < from { model.customTo = from }
        }
        .sheet(isPresented: $showsBulkTagSheet) { bulkTagSheet }
        .alert(item: $forgetConfirmation) { confirmation in
            Alert(
                title: Text(confirmation.title),
                message: Text(confirmation.message(total: model.total, loaded: model.memories.count)),
                primaryButton: .cancel(),
                secondaryButton: .destructive(Text(confirmation.confirmLabel)) {
                    if confirmation.needsDurableFollowUp {
                        Task { @MainActor in
                            await Task.yield()
                            forgetConfirmation = .durable(confirmation.ids)
                        }
                    } else {
                        Task { await model.forget(ids: confirmation.ids) }
                    }
                }
            )
        }
        .onExitCommand {
            if !model.isMutating,
               !model.selection.isEmpty || model.inspectedMemoryID != nil {
                dismissCurrentSearchFocusAndRestoreFocus()
            }
        }
    }

    private var refreshKey: SearchRefreshKey {
        .init(
            query: model.query, domain: model.domain, status: model.status,
            tag: model.tag, agent: model.agent, sort: model.sort,
            datePreset: model.datePreset, customFrom: model.customFrom, customTo: model.customTo
        )
    }

    private var routeCommandActions: CerebrumRouteCommandActions {
        .init(
            route: .search,
            isRefreshing: model.isLoading,
            refresh: refresh,
            blocksGlobalCommands: showsFilters || showsBulkTagSheet ||
                forgetConfirmation != nil || model.isMutating,
            search: .init(
                inspectorIsPresented: inspectorIsPresented && model.inspectedMemoryID != nil,
                hasInspector: model.inspectedMemoryID != nil,
                hasSelection: !model.selection.isEmpty,
                toggleInspector: toggleInspectorPresentation,
                clearSelection: clearBulkSelectionAndRestoreFocus
            )
        )
    }

    private func refresh() {
        Task { await model.refresh() }
    }


    private var header: some View {
        CerebrumPageContextBar(routeTitle: "Search", context: resultSummary) {
            CerebrumDataStatusView(status: model.dataStatus)
        }
    }

    @ViewBuilder
    private var operationNotice: some View {
        if let message = model.operationMessage {
            HStack {
                Label(message, systemImage: operationSymbol)
                    .font(.callout)
                    .foregroundStyle(operationColor)
                Spacer()
                Button("Dismiss") { model.operationMessage = nil }
                    .buttonStyle(.plain)
                    .foregroundStyle(.secondary)
            }
            .padding(12)
            .background(operationColor.opacity(0.08), in: RoundedRectangle(cornerRadius: 10))
        }
    }

    @ViewBuilder
    private var updateNotice: some View {
        if model.updatesAvailable {
            HStack {
                Label("Updated memories are available. Your selection has been preserved.", systemImage: "sparkles")
                    .font(.callout)
                Spacer()
                Button("Refresh Results") { Task { await model.refresh() } }
                    .buttonStyle(.borderedProminent)
            }
            .padding(12)
            .background(CerebrumTheme.cyan.opacity(0.09), in: RoundedRectangle(cornerRadius: 10))
        }
        if model.isStale {
            Label("Showing the last successful result set. \(model.errorMessage ?? "Refresh is temporarily unavailable.")", systemImage: "clock.badge.exclamationmark")
                .font(.callout)
                .padding(12)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(CerebrumTheme.amber.opacity(0.09), in: RoundedRectangle(cornerRadius: 10))
        }
    }

    @ViewBuilder
    private var projectionNotice: some View {
        if model.hasSnapshotForCurrentScope,
           let projection = model.projection, projection.partial == true {
            Label(
                projection.message ?? "Showing the verified local projection; some records are temporarily hidden.",
                systemImage: "eye.trianglebadge.exclamationmark"
            )
            .font(.callout)
            .padding(12)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(CerebrumTheme.amber.opacity(0.09), in: RoundedRectangle(cornerRadius: 10))
        }
    }

    private var resultsSurface: some View {
        Group {
            if let error = model.errorMessage, !model.hasSnapshotForCurrentScope {
                ContentUnavailableView {
                    Label("Couldn’t load memory", systemImage: "exclamationmark.triangle")
                } description: {
                    Text(error)
                } actions: {
                    Button("Try Again") { Task { await model.refresh() } }
                }
            } else if model.isLoading && !model.hasSnapshotForCurrentScope {
                VStack(spacing: 12) {
                    ProgressView()
                    Text("Searching sovereign memory…").foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if !model.hasSnapshotForCurrentScope || model.memories.isEmpty {
                ContentUnavailableView.search(text: model.query)
            } else {
                VStack(spacing: 0) {
                    if !model.selection.isEmpty { selectionToolbar }
                    Table(model.memories, selection: $model.selection) {
                        TableColumn("Memory") { memory in
                            VStack(alignment: .leading, spacing: 3) {
                                Text(memory.content.isEmpty ? "No content" : memory.content)
                                    .lineLimit(2)
                                Text(memory.memoryType.capitalized)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            .padding(.vertical, 4)
                        }
                        .width(min: 280, ideal: 520)
                        TableColumn("Domain", value: \.domainTag)
                            .width(min: 90, ideal: 130)
                        TableColumn("Confidence") { memory in
                            Text(memory.confidenceScore, format: .percent.precision(.fractionLength(0)))
                                .monospacedDigit()
                        }
                        .width(86)
                        TableColumn("Author") { memory in
                            Text(model.authorLabels[memory.submittingAgent] ?? memory.submittingAgent)
                                .lineLimit(1)
                        }
                        .width(min: 100, ideal: 150)
                        TableColumn("Created") { memory in
                            Text(memory.createdAt, style: .relative)
                        }
                        .width(min: 90, ideal: 115)
                    }
                    .contextMenu(forSelectionType: MemorySummary.ID.self) { ids in
                        Button("Inspect", systemImage: "sidebar.right") {
                            inspectMemory(ids.first)
                        }
                        .disabled(ids.count != 1 || model.isMutating)
                        Button("Copy Memory ID", systemImage: "doc.on.doc") {
                            if let id = ids.first { NSPasteboard.general.clearContents(); NSPasteboard.general.setString(id, forType: .string) }
                        }
                        .disabled(ids.count != 1)
                    } primaryAction: { ids in
                        inspectMemory(ids.count == 1 ? ids.first : nil)
                    }
                    .focused($keyboardFocus, equals: .results)
                    .accessibilityFocused($accessibilityFocus, equals: .results)
                    .accessibilityIdentifier("search-results-table")

                    if model.nextCursor != nil {
                        Divider()
                        Button {
                            Task { await model.loadOlder() }
                        } label: {
                            if model.isLoadingOlder { ProgressView().controlSize(.small) }
                            else { Label("Load older memories", systemImage: "clock.arrow.circlepath") }
                        }
                        .buttonStyle(.plain)
                        .padding(11)
                        .disabled(model.isLoadingOlder)
                    }
                }
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(nsColor: .controlBackgroundColor).opacity(0.94), in: RoundedRectangle(cornerRadius: 14))
        .overlay(RoundedRectangle(cornerRadius: 14).stroke(Color(nsColor: .separatorColor).opacity(0.48)))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }

    private var selectionToolbar: some View {
        HStack(spacing: 12) {
            Text("\(model.selection.count) selected")
                .font(.callout.weight(.semibold))
            Button("Tag…", systemImage: "tag") {
                bulkTag = ""
                showsBulkTagSheet = true
            }
            .disabled(model.isMutating)
            Button("Forget…", systemImage: "archivebox") {
                requestForget(model.memories.filter { model.selection.contains($0.id) }.map(\.id))
            }
            .foregroundStyle(.red)
            .disabled(model.isMutating)
            Spacer()
            Button(model.selection.count == model.memories.count ? "Deselect All" : "Select Loaded") {
                if model.selection.count == model.memories.count { model.selection.removeAll() }
                else { model.selection = Set(model.memories.map(\.id)) }
            }
            .buttonStyle(.plain)
            .disabled(model.isMutating)
            Button("Clear") { model.selection.removeAll() }
                .buttonStyle(.plain)
                .disabled(model.isMutating)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 9)
        .background(CerebrumTheme.cyan.opacity(0.07))
        .overlay(alignment: .bottom) { Divider() }
    }

    private var bulkTagSheet: some View {
        VStack(alignment: .leading, spacing: 18) {
            VStack(alignment: .leading, spacing: 5) {
                Text("Tag Selected Memories")
                    .font(.title2.weight(.bold))
                Text("Add one normalized tag to \(model.selection.count) loaded memories.")
                    .foregroundStyle(.secondary)
            }
            TextField("Tag", text: $bulkTag)
                .textFieldStyle(.roundedBorder)
                .onSubmit { applyBulkTag() }
            HStack {
                Spacer()
                Button("Cancel") { showsBulkTagSheet = false }
                    .keyboardShortcut(.cancelAction)
                Button("Add Tag") { applyBulkTag() }
                    .buttonStyle(.borderedProminent)
                    .keyboardShortcut(.defaultAction)
                    .disabled(bulkTag.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || model.isMutating)
            }
        }
        .padding(24)
        .frame(width: 420)
    }

    private func applyBulkTag() {
        Task {
            if await model.addBulkTag(bulkTag) { showsBulkTagSheet = false }
        }
    }

    private func requestForget(_ ids: [String]) {
        guard !ids.isEmpty else { return }
        let factCount = model.memories.filter { ids.contains($0.id) && $0.memoryType == "fact" }.count
        if ids.count == 1 {
            forgetConfirmation = .single(ids, durable: factCount == 1)
        } else {
            forgetConfirmation = .bulk(ids, durableCount: factCount)
        }
    }

    private var operationColor: Color {
        switch model.operationTone {
        case .success: CerebrumTheme.green
        case .warning: CerebrumTheme.amber
        case .error: .red
        }
    }

    private var operationSymbol: String {
        switch model.operationTone {
        case .success: "checkmark.circle"
        case .warning: "clock.badge.exclamationmark"
        case .error: "exclamationmark.circle"
        }
    }

    @ToolbarContentBuilder
    private var searchToolbar: some ToolbarContent {
        ToolbarItemGroup {
            Button {
                showsFilters.toggle()
            } label: {
                Label(model.activeFilterCount == 0 ? "Filters" : "Filters (\(model.activeFilterCount))", systemImage: "line.3.horizontal.decrease.circle")
            }
            .popover(isPresented: $showsFilters, arrowEdge: .bottom) { filterPopover }
            .disabled(model.isMutating)

            Button(action: refresh) {
                Label("Refresh", systemImage: "arrow.clockwise")
            }
            .disabled(model.isLoading || model.isMutating)
            .accessibilityIdentifier("search-toolbar-refresh")

            Button(action: toggleInspectorPresentation) {
                Label(inspectorIsPresented ? "Hide Inspector" : "Show Inspector", systemImage: "sidebar.trailing")
            }
            .disabled(model.inspectedMemoryID == nil || model.isMutating)
            .help(inspectorIsPresented ? "Hide Search Inspector" : "Show Search Inspector")
            .accessibilityIdentifier("search-inspector-toggle")
        }
    }

    private func inspectMemory(_ id: MemorySummary.ID?) {
        guard let id, !model.isMutating else { return }
        applyInspectorState(SearchInspectorLifecycle.activated(memoryID: id))
        requestFocus(.inspectorClose)
        let memory = model.memories.first { $0.id == id }
        let context = memory.map { " in \($0.domainTag), ID \(String(id.prefix(8)))" } ?? ""
        postAccessibilityAnnouncement("Showing details for memory\(context).")
    }

    private func toggleInspectorPresentation() {
        let next = SearchInspectorLifecycle.toggled(
            from: inspectorState,
            inspectedMemoryIsAvailable: model.inspectedMemoryID != nil && !model.isMutating
        )
        guard next != inspectorState else { return }
        applyInspectorState(next)
        requestFocus(next.isPresented ? .inspectorClose : .results)
        postAccessibilityAnnouncement(next.isPresented ? "Showing Search inspector." : "Search inspector hidden.")
    }

    private func hideInspectorAndRestoreFocus() {
        let next = SearchInspectorLifecycle.hidden(from: inspectorState)
        guard next != inspectorState else { return }
        applyInspectorState(next)
        requestFocus(.results)
        postAccessibilityAnnouncement("Search inspector hidden.")
    }

    private func clearBulkSelectionAndRestoreFocus() {
        model.selection.removeAll()
        requestFocus(.results)
        postAccessibilityAnnouncement("Search selection cleared.")
    }

    private func dismissCurrentSearchFocusAndRestoreFocus() {
        model.selection.removeAll()
        applyInspectorState(SearchInspectorLifecycle.cleared)
        requestFocus(.results)
        postAccessibilityAnnouncement("Search selection and details cleared.")
    }

    private var inspectorState: SearchInspectorState {
        .init(inspectedMemoryID: model.inspectedMemoryID, isPresented: inspectorIsPresented)
    }

    private func applyInspectorState(_ state: SearchInspectorState) {
        model.inspectedMemoryID = state.inspectedMemoryID
        inspectorIsPresented = state.isPresented
    }

    private func requestFocus(_ target: SearchFocusTarget) {
        focusGeneration += 1
        let generation = focusGeneration
        Task { @MainActor in
            await Task.yield()
            guard generation == focusGeneration else { return }
            keyboardFocus = target
            accessibilityFocus = target
        }
    }

    private func postAccessibilityAnnouncement(_ message: String) {
        NSAccessibility.post(
            element: NSApplication.shared,
            notification: .announcementRequested,
            userInfo: [
                .announcement: message,
                .priority: NSAccessibilityPriorityLevel.medium.rawValue,
            ]
        )
    }

    private var filterPopover: some View {
        Form {
            Picker("Status", selection: $model.status) {
                Text("Active").tag("active")
                Text("Committed").tag("committed")
                Text("Proposed").tag("proposed")
                Text("Deprecated").tag("deprecated")
            }
            Picker("Domain", selection: $model.domain) {
                Text("All domains").tag("")
                ForEach(model.domains, id: \.self) { Text($0).tag($0) }
            }
            Picker("Tag", selection: $model.tag) {
                Text("All tags").tag("")
                ForEach(model.tags) { item in
                    Text("\(item.tag)  ·  \(item.count)").tag(item.tag)
                }
            }
            Picker("Agent", selection: $model.agent) {
                Text("All agents").tag("")
                ForEach(model.agents) { agent in
                    Text(agent.name ?? agent.registeredName ?? agent.agentID).tag(agent.agentID)
                }
            }
            Picker("Date", selection: $model.datePreset) {
                ForEach(MemoryDatePreset.allCases) { Text($0.title).tag($0) }
            }
            if model.datePreset == .custom {
                DatePicker("From", selection: $model.customFrom, displayedComponents: .date)
                DatePicker("Through", selection: $model.customTo, in: model.customFrom..., displayedComponents: .date)
            }
            Picker("Sort", selection: $model.sort) {
                ForEach(MemorySort.allCases) { Text($0.title).tag($0) }
            }
            Divider()
            Button("Reset Filters") { model.resetFilters() }
                .disabled(model.activeFilterCount == 0)
        }
        .formStyle(.grouped)
        .frame(width: 330)
        .padding(.vertical, 8)
    }

    private var resultSummary: String {
        if model.isLoading { return "Updating…" }
        if !model.hasSnapshotForCurrentScope { return "No current results" }
        let shown = model.memories.count.formatted()
        if model.total > model.memories.count || model.continuationRequired {
            return "\(shown) loaded · at least \(model.total.formatted()) matching"
        }
        return "\(shown) results"
    }
}

private struct ForgetConfirmation: Identifiable {
    let id = UUID()
    let ids: [String]
    let title: String
    let baseMessage: String
    let confirmLabel: String
    let needsDurableFollowUp: Bool

    static func single(_ ids: [String], durable: Bool) -> Self {
        .init(
            ids: ids,
            title: durable ? "Forget durable fact?" : "Forget memory?",
            baseMessage: durable
                ? "This durable fact will stop appearing in normal recall and search, but remains in the on-chain audit history."
                : "This memory will stop appearing in normal recall and search, but remains in the on-chain audit history.",
            confirmLabel: durable ? "Forget Fact" : "Forget Memory",
            needsDurableFollowUp: false
        )
    }

    static func bulk(_ ids: [String], durableCount: Int) -> Self {
        .init(
            ids: ids,
            title: "Forget selected memories?",
            baseMessage: "The selected memories will stop appearing in normal recall and search, but remain in the on-chain audit history.",
            confirmLabel: "Continue",
            needsDurableFollowUp: durableCount > 0
        )
    }

    static func durable(_ ids: [String]) -> Self {
        .init(
            ids: ids,
            title: "Durable FACT warning",
            baseMessage: "This selection contains durable facts: high-confidence, long-term knowledge. Forget them anyway?",
            confirmLabel: "Forget Facts",
            needsDurableFollowUp: false
        )
    }

    func message(total: Int, loaded: Int) -> String {
        guard ids.count > 1, total > loaded else { return baseMessage }
        return baseMessage + " Only the \(ids.count) selected loaded memories are affected—not every matching result."
    }
}

private struct SearchRefreshKey: Hashable {
    let query: String
    let domain: String
    let status: String
    let tag: String
    let agent: String
    let sort: MemorySort
    let datePreset: MemoryDatePreset
    let customFrom: Date
    let customTo: Date
}

private enum SearchFocusTarget: Hashable {
    case results
    case inspectorClose
}

struct SearchInspectorState: Equatable, Sendable {
    var inspectedMemoryID: MemorySummary.ID?
    var isPresented: Bool
}

enum SearchInspectorLifecycle {
    static func activated(memoryID: MemorySummary.ID) -> SearchInspectorState {
        .init(inspectedMemoryID: memoryID, isPresented: true)
    }

    static func selectionChanged(
        from state: SearchInspectorState,
        selection _: Set<MemorySummary.ID>
    ) -> SearchInspectorState {
        state
    }

    static func toggled(
        from state: SearchInspectorState,
        inspectedMemoryIsAvailable: Bool
    ) -> SearchInspectorState {
        guard inspectedMemoryIsAvailable, state.inspectedMemoryID != nil else { return state }
        return .init(inspectedMemoryID: state.inspectedMemoryID, isPresented: !state.isPresented)
    }

    static func hidden(from state: SearchInspectorState) -> SearchInspectorState {
        .init(inspectedMemoryID: state.inspectedMemoryID, isPresented: false)
    }

    static var cleared: SearchInspectorState {
        .init(inspectedMemoryID: nil, isPresented: false)
    }
}
