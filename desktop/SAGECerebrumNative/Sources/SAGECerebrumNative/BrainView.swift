import AppKit
import SwiftUI

struct BrainView: View {
    @Environment(\.scenePhase) private var scenePhase
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @State private var model: BrainViewModel
    @State private var metalRecovery = BrainMetalRecoveryState()
    @State private var preparedMetalRenderer: BrainMetalRendererHandoff?
    @State private var scanning = true
    @State private var flow = true
    @State private var memoryHullOpacity = 0.08
    @State private var connectomeHullOpacity = 0.03
    @State private var showsDisplayControls = false
    @State private var showsNavigator = false
    @State private var showsInspector = false
    @State private var availableSize = CGSize(width: 1_180, height: 760)
    @State private var mountedMetalSurfaces: Set<BrainMountedSurface> = []
    @State private var pendingMetalRestorationAttemptID: UInt64?
    @State private var retryTask: Task<Void, Never>?
    @State private var announcementGeneration = 0
    @State private var keyboardFocusGeneration = 0
    @State private var accessibilityFocusGeneration = 0
    @FocusState private var keyboardFocus: BrainFocusTarget?
    @AccessibilityFocusState private var accessibilityFocus: BrainFocusTarget?
    private let rendererBootstrap: BrainMetalRendererFactory
    private let retryRendererBootstrap: BrainMetalRetryBootstrap
    private let accessibilityAnnouncer: @MainActor (String) -> Void
    private let surfaceObserver: @MainActor (BrainMountedSurface, Bool) -> Void
    private let layoutObserver: @MainActor (BrainResponsiveLayoutPlan) -> Void

    init(
        api: any SAGEAPI,
        rendererBootstrap: @escaping BrainMetalRendererFactory = { onPick in
            guard let renderer = BrainMetalRenderer(onPick: onPick) else {
                return .failure(.rendererInitialization)
            }
            return .success(renderer)
        },
        retryRendererBootstrap: BrainMetalRetryBootstrap? = nil,
        accessibilityAnnouncer: @escaping @MainActor (String) -> Void = BrainView.systemAccessibilityAnnouncement,
        surfaceObserver: @escaping @MainActor (BrainMountedSurface, Bool) -> Void = { _, _ in },
        layoutObserver: @escaping @MainActor (BrainResponsiveLayoutPlan) -> Void = { _ in }
    ) {
        _model = State(initialValue: BrainViewModel(api: api))
        self.rendererBootstrap = rendererBootstrap
        self.retryRendererBootstrap = retryRendererBootstrap ?? {
            rendererBootstrap({ _ in })
        }
        self.accessibilityAnnouncer = accessibilityAnnouncer
        self.surfaceObserver = surfaceObserver
        self.layoutObserver = layoutObserver
    }

    init(
        model: BrainViewModel,
        rendererBootstrap: @escaping BrainMetalRendererFactory,
        retryRendererBootstrap: BrainMetalRetryBootstrap? = nil,
        accessibilityAnnouncer: @escaping @MainActor (String) -> Void,
        surfaceObserver: @escaping @MainActor (BrainMountedSurface, Bool) -> Void,
        layoutObserver: @escaping @MainActor (BrainResponsiveLayoutPlan) -> Void = { _ in }
    ) {
        _model = State(initialValue: model)
        _showsInspector = State(initialValue: model.hasVisibleInspector)
        self.rendererBootstrap = rendererBootstrap
        self.retryRendererBootstrap = retryRendererBootstrap ?? {
            rendererBootstrap({ _ in })
        }
        self.accessibilityAnnouncer = accessibilityAnnouncer
        self.surfaceObserver = surfaceObserver
        self.layoutObserver = layoutObserver
    }

    var body: some View {
        GeometryReader { proxy in
            ZStack {
                CerebrumBackdrop()
                if layoutPlan.tier == .compact {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 16) {
                            header
                            notices
                            brainSurface.frame(height: layoutPlan.surfaceMinimumHeight)
                            if trainOfThoughtVisible {
                                trainOfThoughtPane.frame(minHeight: layoutPlan.trainIdealHeight)
                            }
                        }
                        .padding(layoutPlan.pagePadding)
                    }
                } else {
                    VStack(alignment: .leading, spacing: 16) {
                        header
                        notices
                        if trainOfThoughtVisible {
                            VSplitView {
                                brainSurface.frame(minHeight: layoutPlan.surfaceMinimumHeight)
                                trainOfThoughtPane
                                    .frame(
                                        minHeight: layoutPlan.trainMinimumHeight,
                                        idealHeight: layoutPlan.trainIdealHeight,
                                        maxHeight: layoutPlan.trainMaximumHeight
                                    )
                            }
                        } else {
                            brainSurface
                        }
                    }
                    .padding(layoutPlan.pagePadding)
                }
            }
            .onAppear { updateAvailableSize(proxy.size) }
            .onChange(of: proxy.size) { _, size in updateAvailableSize(size) }
        }
        .navigationTitle("Brain")
        .toolbar { brainToolbar }
        .inspector(isPresented: Binding(
            get: { showsInspector && model.hasVisibleInspector },
            set: { showsInspector = $0 }
        )) {
            VStack(spacing: 0) {
                HStack {
                    Spacer()
                    Button("Close Selection", systemImage: "xmark") {
                        clearSelectionAndRestoreFocus()
                    }
                    .labelStyle(.iconOnly)
                    .help("Close selection (Escape)")
                    .focused($keyboardFocus, equals: .inspectorClose)
                    .accessibilityFocused($accessibilityFocus, equals: .inspectorClose)
                    .accessibilityLabel("Close Brain selection")
                }
                .padding(.horizontal, 12)
                .padding(.vertical, 8)

                Divider()

                if model.mode == .memory, let node = model.selectedNode {
                    BrainNodeInspectorView(node: node)
                } else if model.mode == .connectome, let neuron = model.selectedNeuron {
                    AgentNeuronInspectorView(neuron: neuron, model: model)
                }
            }
            .inspectorColumnWidth(
                min: layoutPlan.inspectorMinimumWidth,
                ideal: layoutPlan.inspectorIdealWidth,
                max: layoutPlan.inspectorMaximumWidth
            )
        }
        .task(id: BrainRefreshKey(mode: model.mode, domain: model.selectedDomain, status: model.status)) {
            await model.refresh()
        }
        .task(id: BrainDetailKey(mode: model.mode, memoryID: model.selectedNodeID, agentID: model.selectedAgentID)) {
            if model.mode == .memory { await model.loadRelatedForSelection() }
            else { await model.loadEngramsForSelection() }
        }
        .task(id: scenePhase) {
            guard scenePhase == .active else { return }
            await model.runLiveUpdates()
        }
        .onExitCommand {
            guard hasSelection else { return }
            dismissCurrentSelectionAndRestoreFocus()
        }
        .onChange(of: model.selectedNodeID) { _, selected in
            if selected != nil { showsInspector = true }
            scheduleSelectionAnnouncement()
        }
        .onChange(of: model.relatedMemoryFocus) { _, _ in scheduleSelectionAnnouncement() }
        .onChange(of: model.selectedAgentID) { _, selected in
            if selected != nil { showsInspector = true }
            scheduleSelectionAnnouncement()
        }
        .onChange(of: model.selectedEngramID) { _, _ in scheduleSelectionAnnouncement() }
        .onChange(of: model.selectedConnectionID) { _, _ in scheduleSelectionAnnouncement() }
        .onChange(of: model.mode) { _, _ in
            applyMetalEvent(.modeChanged)
        }
        .onChange(of: trainOfThoughtVisible) { _, _ in
            layoutObserver(layoutPlan)
        }
        .onDisappear {
            applyMetalEvent(.retryCancelled)
        }
    }

    private var trainOfThoughtVisible: Bool {
        model.mode == .memory && model.selectedNodeID != nil &&
            (model.relatedMemories != nil || model.isDetailLoading || model.detailErrorMessage != nil)
    }

    private var layoutPlan: BrainResponsiveLayoutPlan {
        BrainResponsiveLayoutPolicy.resolve(size: availableSize, trainVisible: trainOfThoughtVisible)
    }

    private func updateAvailableSize(_ size: CGSize) {
        guard size != availableSize else { return }
        availableSize = size
        layoutObserver(BrainResponsiveLayoutPolicy.resolve(size: size, trainVisible: trainOfThoughtVisible))
    }

    private var header: some View {
        CerebrumPageHeader(
            eyebrow: model.mode == .memory ? "Memory MRI" : "Agent Connectome",
            title: "Brain",
            subtitle: model.mode == .memory
                ? "Explore how sovereign memory forms, connects, and consolidates."
                : "See visible authorized agents as neurons and retained local message history as directed synapses."
        ) {
            VStack(alignment: .trailing, spacing: 5) {
                CerebrumLiveIndicator(connected: model.liveEventsConnected)
                Text(graphSummary)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
            }
        }
    }

    @ViewBuilder
    private var notices: some View {
        if case .unavailable = metalRecovery.capability {
            ViewThatFits(in: .horizontal) {
                metalFallbackContent(stacked: false)
                metalFallbackContent(stacked: true)
            }
            .padding(11)
            .background(CerebrumTheme.amber.opacity(0.08), in: RoundedRectangle(cornerRadius: 10))
            .accessibilityElement(children: .contain)
            .accessibilityIdentifier("brain-metal-fallback-notice")
            .onAppear { surfaceObserver(.metalFallbackNotice, true) }
            .onDisappear { surfaceObserver(.metalFallbackNotice, false) }
        }
        if model.updatesAvailable {
            notice(
                model.mode == .memory
                    ? "Updated memory structure is available. Your focused memory remains pinned."
                    : "Updated retained traffic or visible engrams are available. Your selected agent remains pinned.",
                systemImage: "sparkles", color: CerebrumTheme.cyan,
                action: ("Refresh Brain", { Task { await model.refreshIncludingPinnedDetail() } })
            )
        } else if model.isStale {
            notice(
                "Showing the last verified brain snapshot. \(model.errorMessage ?? "Refresh is temporarily unavailable.")",
                systemImage: "clock.badge.exclamationmark", color: CerebrumTheme.amber
            )
        }
        if model.mode == .memory, model.graph?.projection?.partial == true {
            notice(
                model.graph?.projection?.message ?? "The MRI contains only canonically verified visible memories while projection recovery continues.",
                systemImage: "eye.trianglebadge.exclamationmark", color: CerebrumTheme.amber
            )
        }
        if model.mode == .memory, model.graph?.continuationRequired == true {
            notice(
                "This MRI is a bounded representative snapshot; more visible memories exist beyond the current scan budget.",
                systemImage: "circle.grid.cross", color: .secondary
            )
        }
        if model.mode == .connectome, model.connectomeWasTruncated {
            notice(
                "This Connectome exceeds the native safety budget. Showing up to 3,840 authorized agents and 15,360 directed synapses, with capacity reserved for the selected engram bloom.",
                systemImage: "gauge.with.dots.needle.67percent", color: .secondary
            )
        }
    }

    @ViewBuilder
    private func metalFallbackContent(stacked: Bool) -> some View {
        let message = model.mode == .memory
            ? "The interactive MRI couldn’t be displayed. Your verified memories remain available in the table."
            : "The interactive MRI couldn’t be displayed. Your verified Connectome remains available in the table."
        if stacked {
            VStack(alignment: .leading, spacing: 10) {
                metalFallbackMessage(message)
                metalRetryButton.frame(maxWidth: .infinity, alignment: .leading)
            }
        } else {
            HStack(spacing: 12) {
                metalFallbackMessage(message)
                Spacer()
                metalRetryButton
            }
            .fixedSize(horizontal: true, vertical: false)
        }
    }

    private func metalFallbackMessage(_ message: String) -> some View {
        HStack(alignment: .top, spacing: 12) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .foregroundStyle(CerebrumTheme.amber)
                    .accessibilityHidden(true)
                VStack(alignment: .leading, spacing: 2) {
                    Text("Interactive MRI unavailable").font(.callout.weight(.semibold))
                    Text(message)
                        .font(.caption).foregroundStyle(.secondary)
                }
                .accessibilityElement(children: .combine)
        }
    }

    private var metalRetryButton: some View {
        BrainMetalRetryButton(inFlight: metalRecovery.retryInFlight, action: retryMetal)
        .fixedSize()
        .onAppear { surfaceObserver(.metalRetryButton, true) }
        .onDisappear { surfaceObserver(.metalRetryButton, false) }
        .focused($keyboardFocus, equals: .metalRetry)
        .accessibilityFocused($accessibilityFocus, equals: .metalRetry)
    }

    private func notice(
        _ text: String,
        systemImage: String,
        color: Color,
        action: (String, () -> Void)? = nil
    ) -> some View {
        ViewThatFits(in: .horizontal) {
            HStack {
                Label(text, systemImage: systemImage).font(.callout).foregroundStyle(color)
                Spacer()
                if let action { Button(action.0, action: action.1).buttonStyle(.bordered) }
            }
            .fixedSize(horizontal: true, vertical: false)
            VStack(alignment: .leading, spacing: 9) {
                Label(text, systemImage: systemImage).font(.callout).foregroundStyle(color)
                if let action { Button(action.0, action: action.1).buttonStyle(.bordered) }
            }
        }
        .padding(11)
        .background(color.opacity(0.08), in: RoundedRectangle(cornerRadius: 10))
    }

    private var brainSurface: some View {
        HStack(spacing: 0) {
            if layoutPlan.showsInlineNavigator {
                if model.mode == .memory { domainSidebar }
                else { agentSidebar }
                Divider()
            }
            Group {
                if model.mode == .memory, let graph = model.graph {
                    if graph.nodes.isEmpty { emptyState }
                    else if metalRecovery.effectivePresentation == .mri { memoryMRI(graph) }
                    else { memoryTable(graph) }
                } else if model.mode == .connectome, let connectome = model.connectome {
                    if connectome.neurons.isEmpty { connectomeEmptyState }
                    else if metalRecovery.effectivePresentation == .mri { connectomeMRI(connectome) }
                    else { connectomeTable(connectome) }
                } else if model.isLoading {
                    VStack(spacing: 12) {
                        ProgressView()
                        Text(model.mode == .memory ? "Scanning sovereign memory…" : "Mapping agent synapses…")
                            .foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else {
                    ContentUnavailableView {
                        Label(model.mode == .memory ? "Brain unavailable" : "Connectome unavailable", systemImage: "brain.head.profile")
                    } description: {
                        Text(model.errorMessage ?? "The verified memory graph could not be loaded.")
                    } actions: {
                        Button("Try Again") { Task { await model.refresh() } }
                    }
                }
            }
        }
        .background(Color(nsColor: .controlBackgroundColor).opacity(0.95), in: RoundedRectangle(cornerRadius: 14))
        .overlay(RoundedRectangle(cornerRadius: 14).stroke(Color(nsColor: .separatorColor).opacity(0.5)))
        .clipShape(RoundedRectangle(cornerRadius: 14))
        .frame(minHeight: layoutPlan.surfaceMinimumHeight)
    }

    private var domainSidebar: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("MEMORY LOBES")
                    .font(.caption2.weight(.bold))
                    .tracking(0.9)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(model.domains.count.formatted())
                    .font(.caption.monospacedDigit())
                    .foregroundStyle(.tertiary)
            }
            List(selection: $model.selectedDomain) {
                Label("Whole Brain", systemImage: "brain").tag("")
                ForEach(model.domains, id: \.self) { domain in
                    HStack {
                        Circle().fill(domainColor(domain)).frame(width: 7, height: 7)
                        Text(domain).lineLimit(1)
                        Spacer()
                        if let count = model.graph?.domainCounts?[domain] {
                            Text(count.formatted()).font(.caption.monospacedDigit()).foregroundStyle(.tertiary)
                        }
                    }
                    .tag(domain)
                }
            }
            .listStyle(.sidebar)
        }
        .padding(13)
        .frame(width: 230)
        .background(.thinMaterial)
        .accessibilityIdentifier("brain-inline-navigator")
        .onAppear { surfaceObserver(.inlineNavigator, true) }
        .onDisappear { surfaceObserver(.inlineNavigator, false) }
    }

    private var agentSidebar: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("VISIBLE NEURONS")
                    .font(.caption2.weight(.bold)).tracking(0.9).foregroundStyle(.secondary)
                Spacer()
                Text((model.connectome?.neurons.count ?? 0).formatted())
                    .font(.caption.monospacedDigit()).foregroundStyle(.tertiary)
            }
            List(model.connectome?.neurons ?? [], selection: Binding(
                get: { model.selectedAgentID },
                set: { model.selectConnectomeAgent($0) }
            )) { neuron in
                HStack(spacing: 9) {
                    Circle().fill(domainColor(neuron.domain ?? "unassigned")).frame(width: 8, height: 8)
                    VStack(alignment: .leading, spacing: 1) {
                        Text(neuron.name).lineLimit(1)
                        Text(neuron.role.capitalized).font(.caption2).foregroundStyle(.secondary)
                    }
                    Spacer()
                    Text(model.totalTraffic(for: neuron.agentID).formatted())
                        .font(.caption.monospacedDigit()).foregroundStyle(.tertiary)
                }
                .tag(neuron.agentID)
            }
            .listStyle(.sidebar)
        }
        .padding(13)
        .frame(width: 230)
        .background(.thinMaterial)
        .accessibilityIdentifier("brain-inline-navigator")
        .onAppear { surfaceObserver(.inlineNavigator, true) }
        .onDisappear { surfaceObserver(.inlineNavigator, false) }
    }

    private func memoryMRI(_ graph: BrainGraphEnvelope) -> some View {
        ZStack(alignment: .topLeading) {
            MetalBrainView(
                nodes: graph.nodes,
                edges: graph.edges,
                selectedID: model.sceneFocusedMemoryID,
                topologyFocusID: nil,
                highlightedEdge: nil,
                layout: .memory,
                autoRotate: scanning && !reduceMotion,
                flow: flow && !reduceMotion,
                hullOpacity: currentHullOpacity,
                onPick: { pick in
                    switch pick {
                    case let .node(id):
                        model.selectedNodeID = id
                        requestFocus(.surface)
                    case .edge: break
                    case .background: dismissCurrentSelectionAndRestoreFocus()
                    }
                },
                attemptID: metalRecovery.attemptID,
                onCapabilityChange: { handleMetalCapability($1, attemptID: $0) },
                rendererFactory: metalRendererFactory
            )
            VStack(alignment: .leading, spacing: 4) {
                Text("CEREBRUM · MRI")
                    .font(.system(.caption, design: .monospaced).weight(.bold))
                Text(scanning && !reduceMotion ? "● SCANNING" : "○ PAUSED")
                    .font(.system(.caption2, design: .monospaced))
                    .foregroundStyle(scanning && !reduceMotion ? CerebrumTheme.green : .secondary)
            }
            .foregroundStyle(.white.opacity(0.90))
            .padding(11)
            .background(.black.opacity(0.34), in: RoundedRectangle(cornerRadius: 9))
            .padding(14)
            .allowsHitTesting(false)
        }
        .accessibilityElement(children: .contain)
        .accessibilityIdentifier("brain-memory-mri")
        .onAppear { reportMetalSurface(.memoryMRI, mounted: true) }
        .onDisappear { reportMetalSurface(.memoryMRI, mounted: false) }
        .focusable()
        .focused($keyboardFocus, equals: .surface)
        .accessibilityFocused($accessibilityFocus, equals: .surface)
        .accessibilityHint("Use the Accessible Table presentation for keyboard navigation and detailed selection.")
    }

    private func memoryTable(_ graph: BrainGraphEnvelope) -> some View {
        Table(graph.nodes, selection: Binding(
            get: { model.selectedNodeID },
            set: {
                model.selectedNodeID = $0
                if $0 != nil { requestFocus(.table) }
            }
        )) {
            TableColumn("Memory") { node in
                Text(node.content).lineLimit(2).padding(.vertical, 4)
            }
            .width(min: 240, ideal: 460)
            TableColumn("Domain", value: \.domain).width(min: 90, ideal: 130)
            TableColumn("Type", value: \.memoryType).width(90)
            TableColumn("Status", value: \.status).width(90)
            TableColumn("Stored Confidence") { node in
                Text(node.confidence, format: .percent.precision(.fractionLength(0))).monospacedDigit()
            }
            .width(112)
            TableColumn("Corroborations") { node in
                Text(node.corroborationCount.formatted()).monospacedDigit()
            }
            .width(105)
            TableColumn("Created") { node in Text(node.createdAt, style: .relative) }
                .width(100)
            TableColumn("Author") { node in Text(node.agentLabel ?? node.agent).lineLimit(1) }
                .width(min: 100, ideal: 140)
        }
        .accessibilityLabel("Memory brain table")
        .accessibilityIdentifier("brain-memory-table")
        .onAppear { surfaceObserver(.memoryTable, true) }
        .onDisappear { surfaceObserver(.memoryTable, false) }
        .focused($keyboardFocus, equals: .table)
        .accessibilityFocused($accessibilityFocus, equals: .table)
    }

    private func connectomeMRI(_ connectome: ConnectomeEnvelope) -> some View {
        ZStack(alignment: .topLeading) {
            MetalBrainView(
                nodes: model.connectomeSceneNodes,
                edges: model.connectomeSceneEdges,
                selectedID: model.selectedConnectomeSceneID,
                topologyFocusID: model.selectedAgentID.map { "agent:\($0)" },
                highlightedEdge: model.selectedConnectionEdge,
                layout: .connectome,
                autoRotate: scanning && !reduceMotion,
                flow: flow && !reduceMotion,
                hullOpacity: currentHullOpacity,
                onPick: { pick in
                    switch pick {
                    case let .node(id):
                        model.selectConnectomeSceneNode(id)
                        requestFocus(.surface)
                    case let .edge(edge):
                        model.selectConnectomeSceneEdge(edge)
                        requestFocus(.surface)
                    case .background:
                        dismissCurrentSelectionAndRestoreFocus()
                    }
                },
                attemptID: metalRecovery.attemptID,
                onCapabilityChange: { handleMetalCapability($1, attemptID: $0) },
                rendererFactory: metalRendererFactory
            )
            VStack(alignment: .leading, spacing: 4) {
                Text("CEREBRUM · CONNECTOME")
                    .font(.system(.caption, design: .monospaced).weight(.bold))
                Text("\(connectome.neurons.count) NEURONS · \(connectome.synapses.count) SYNAPSES")
                    .font(.system(.caption2, design: .monospaced))
                    .foregroundStyle(CerebrumTheme.green)
            }
            .foregroundStyle(.white.opacity(0.9))
            .padding(11)
            .background(.black.opacity(0.34), in: RoundedRectangle(cornerRadius: 9))
            .padding(14)
            .allowsHitTesting(false)
        }
        .accessibilityLabel("Connectome MRI, \(connectome.neurons.count) visible agents, \(connectome.synapses.count) directed retained-traffic synapses. Use Accessible Table to inspect.")
        .accessibilityIdentifier("brain-connectome-mri")
        .onAppear { reportMetalSurface(.connectomeMRI, mounted: true) }
        .onDisappear { reportMetalSurface(.connectomeMRI, mounted: false) }
        .focusable()
        .focused($keyboardFocus, equals: .surface)
        .accessibilityFocused($accessibilityFocus, equals: .surface)
        .accessibilityHint("Use the Accessible Table presentation for keyboard navigation and detailed selection.")
    }

    private func connectomeTable(_ connectome: ConnectomeEnvelope) -> some View {
        Table(connectome.neurons, selection: Binding(
            get: { model.selectedAgentID },
            set: {
                model.selectConnectomeAgent($0)
                if $0 != nil { requestFocus(.table) }
            }
        )) {
            TableColumn("Agent") { neuron in Text(neuron.name).fontWeight(.medium) }
                .width(min: 150, ideal: 220)
            TableColumn("Role", value: \.role).width(90)
            TableColumn("Domain") { neuron in Text(neuron.domain ?? "—") }.width(120)
            TableColumn("Incoming") { neuron in
                Text(model.incomingTraffic(for: neuron.agentID).formatted()).monospacedDigit()
            }.width(85)
            TableColumn("Outgoing") { neuron in
                Text(model.outgoingTraffic(for: neuron.agentID).formatted()).monospacedDigit()
            }.width(85)
            TableColumn("Connections") { neuron in
                Text(model.peerCount(for: neuron.agentID).formatted()).monospacedDigit()
            }.width(95)
            TableColumn("Last retained activity") { neuron in
                if let date = model.lastActivity(for: neuron.agentID) {
                    Text(date, style: .relative)
                } else { Text("No retained traffic").foregroundStyle(.secondary) }
            }.width(min: 130, ideal: 170)
        }
        .accessibilityLabel("Agent connectome table")
        .accessibilityIdentifier("brain-connectome-table")
        .onAppear { surfaceObserver(.connectomeTable, true) }
        .onDisappear { surfaceObserver(.connectomeTable, false) }
        .focused($keyboardFocus, equals: .table)
        .accessibilityFocused($accessibilityFocus, equals: .table)
    }

    private var emptyState: some View {
        ContentUnavailableView {
            Label(model.selectedDomain.isEmpty ? "No memories yet" : "No memories in this lobe", systemImage: "brain")
        } description: {
            Text(model.selectedDomain.isEmpty
                 ? "Import an existing SAGE export or connect an agent to begin forming this brain."
                 : "Show the whole brain or choose another domain.")
        } actions: {
            if !model.selectedDomain.isEmpty {
                Button("Show Whole Brain") { model.selectedDomain = "" }
            }
        }
    }

    private var connectomeEmptyState: some View {
        ContentUnavailableView {
            Label("No visible agents", systemImage: "point.3.connected.trianglepath.dotted")
        } description: {
            Text("The current authorized agent registry contains no ordinary active neurons. This is not a connectivity or presence signal.")
        }
    }

    @ViewBuilder
    private var trainOfThoughtPane: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text("TRAIN OF THOUGHT").font(.caption2.weight(.bold)).tracking(1).foregroundStyle(CerebrumTheme.cyan)
                    if let related = model.relatedMemories {
                        Text(related.content).font(.callout.weight(.medium)).lineLimit(1)
                    }
                }
                Spacer()
                if let count = model.relatedMemories?.related.count {
                    Text("\(count) related").font(.caption).foregroundStyle(.secondary)
                }
                Button("Close", systemImage: "xmark") {
                    clearSelectionAndRestoreFocus()
                }
                .labelStyle(.iconOnly)
                .help("Close Train of Thought")
                .accessibilityLabel("Close Train of Thought")
            }
            if model.isDetailLoading {
                ProgressView("Tracing related memory…").controlSize(.small)
            } else if let error = model.detailErrorMessage {
                ContentUnavailableView("Train unavailable", systemImage: "exclamationmark.triangle", description: Text(error))
            } else if let related = model.relatedMemories {
                ScrollView(.horizontal) {
                    HStack(alignment: .top, spacing: 12) {
                        ForEach(["do", "dont", "observation", "note"], id: \.self) { kind in
                            let memories = kind == "note"
                                ? related.related.filter { !["do", "dont", "observation"].contains($0.kind) }
                                : related.related.filter { $0.kind == kind }
                            trainGroup(kind: kind, memories: memories)
                        }
                    }
                }
            }
        }
        .padding(14)
        .background(.thinMaterial)
        .accessibilityElement(children: .contain)
    }

    private func trainGroup(kind: String, memories: [RelatedMemory]) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            Text(trainTitle(kind)).font(.caption.weight(.semibold)).foregroundStyle(.secondary)
            if memories.isEmpty {
                Text("No visible items").font(.caption).foregroundStyle(.tertiary).frame(width: 210, alignment: .leading)
            } else {
                ForEach(memories) { memory in
                    let isSelected = model.relatedMemoryFocus?.relatedMemoryID == memory.id
                    Button {
                        model.selectRelatedMemory(memory)
                        requestFocus(.relatedMemory(memory.id))
                    } label: {
                        VStack(alignment: .leading, spacing: 5) {
                            HStack(alignment: .top, spacing: 8) {
                                Text(memory.content).font(.callout).lineLimit(3).multilineTextAlignment(.leading)
                                Spacer(minLength: 0)
                                if isSelected {
                                    Image(systemName: "checkmark.circle.fill")
                                        .foregroundStyle(CerebrumTheme.cyan)
                                        .transition(.opacity)
                                }
                            }
                            HStack {
                                Text(memory.relation.replacingOccurrences(of: "-", with: " ").capitalized)
                                Spacer()
                                Text(memory.confidence, format: .percent.precision(.fractionLength(0)))
                            }.font(.caption2).foregroundStyle(.secondary)
                        }
                        .padding(9).frame(width: 230, alignment: .leading)
                        .background(
                            isSelected ? CerebrumTheme.cyan.opacity(0.13) : Color.secondary.opacity(0.08),
                            in: RoundedRectangle(cornerRadius: 9)
                        )
                        .overlay {
                            RoundedRectangle(cornerRadius: 9)
                                .stroke(isSelected ? CerebrumTheme.cyan.opacity(0.82) : .clear, lineWidth: 1)
                        }
                        .shadow(color: isSelected ? CerebrumTheme.cyan.opacity(0.14) : .clear, radius: 8, y: 2)
                    }
                    .buttonStyle(.plain)
                    .focused($keyboardFocus, equals: .relatedMemory(memory.id))
                    .accessibilityFocused($accessibilityFocus, equals: .relatedMemory(memory.id))
                    .accessibilityLabel("\(trainTitle(memory.kind)) related memory. \(memory.content)")
                    .accessibilityValue("\(memory.relation.replacingOccurrences(of: "-", with: " ")), \(memory.confidence.formatted(.percent.precision(.fractionLength(0)))), \(memory.domain)")
                    .accessibilityHint("Focuses this related memory while keeping the primary memory open.")
                    .accessibilityAddTraits(isSelected ? .isSelected : [])
                    .animation(reduceMotion ? nil : .snappy(duration: 0.18), value: isSelected)
                }
            }
        }.accessibilityElement(children: .contain).accessibilityLabel(trainTitle(kind))
    }

    private func trainTitle(_ kind: String) -> String {
        switch kind {
        case "do": "Do"
        case "dont": "Don’t"
        case "observation": "Observations"
        default: "Notes"
        }
    }

    @ToolbarContentBuilder
    private var brainToolbar: some ToolbarContent {
        ToolbarItemGroup {
            if layoutPlan.usesCompactToolbar {
                Menu {
                    Picker("Brain mode", selection: $model.mode) {
                        ForEach(BrainMode.allCases) { Label($0.title, systemImage: $0.systemImage).tag($0) }
                    }
                } label: {
                    Label(model.mode.title, systemImage: model.mode.systemImage)
                }

                Menu {
                    Picker("Presentation", selection: presentationBinding) {
                        presentationOptions
                    }
                } label: {
                    Label(metalRecovery.effectivePresentation.title, systemImage: metalRecovery.effectivePresentation.systemImage)
                }
                .help(presentationHelp)

                Button {
                    showsNavigator.toggle()
                } label: {
                    Label(model.mode == .memory ? "Memory Lobes" : "Visible Neurons", systemImage: "line.3.horizontal.decrease.circle")
                }
                .popover(isPresented: $showsNavigator) { compactNavigator }
                .accessibilityIdentifier("brain-compact-navigator-trigger")
                .onAppear { surfaceObserver(.compactNavigatorTrigger, true) }
                .onDisappear { surfaceObserver(.compactNavigatorTrigger, false) }
            } else {
                Picker("Brain mode", selection: $model.mode) {
                    ForEach(BrainMode.allCases) { Label($0.title, systemImage: $0.systemImage).tag($0) }
                }
                .pickerStyle(.segmented)
                .frame(width: 210)

                Picker("Presentation", selection: presentationBinding) {
                    presentationOptions
                }
                .pickerStyle(.segmented)
                .frame(width: 190)
                .help(presentationHelp)
            }

            if model.hasVisibleInspector && !showsInspector {
                Button {
                    showsInspector = true
                    requestFocus(.inspectorClose)
                } label: {
                    Label("Show Inspector", systemImage: "sidebar.trailing")
                }
                .help("Show details for the current Brain selection")
            }

            Button {
                scanning.toggle()
            } label: {
                Label(scanning ? "Pause Scan" : "Resume Scan", systemImage: scanning ? "pause.circle" : "play.circle")
            }
            .disabled(reduceMotion || metalRecovery.effectivePresentation == .table)

            Button {
                showsDisplayControls.toggle()
            } label: {
                Label("Display", systemImage: "slider.horizontal.3")
            }
            .popover(isPresented: $showsDisplayControls) {
                VStack(alignment: .leading, spacing: 16) {
                    Toggle("Synaptic flow", isOn: $flow).disabled(reduceMotion)
                    VStack(alignment: .leading, spacing: 6) {
                        Text("Brain shell opacity").font(.callout)
                        Slider(value: hullOpacityBinding, in: 0 ... 0.35)
                    }
                    if reduceMotion {
                        Label("Motion effects follow Reduce Motion.", systemImage: "figure.walk.motion")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                }
                .padding(18)
                .frame(width: 280)
            }

            Button {
                clearSelectionAndRestoreFocus()
                model.selectedDomain = ""
            } label: {
                Label("Whole Brain", systemImage: "arrow.uturn.backward.circle")
            }
            .disabled(model.selectedNodeID == nil && model.selectedAgentID == nil && model.selectedDomain.isEmpty)

            Button { Task { await model.refreshIncludingPinnedDetail() } } label: {
                Label("Refresh", systemImage: "arrow.clockwise")
            }
            .keyboardShortcut("r", modifiers: .command)
            .disabled(model.isLoading)
        }
    }

    @ViewBuilder
    private var presentationOptions: some View {
        ForEach(BrainPresentation.allCases) { option in
            Label(option.title, systemImage: option.systemImage)
                .tag(option)
                .disabled(option == .mri && !BrainPresentationPolicy.resolve(
                    requested: option, capability: metalRecovery.capability
                ).mriEnabled)
        }
    }

    private var presentationHelp: String {
        metalRecovery.capability.isUnavailable
            ? "Metal rendering is unavailable. Choose Try MRI Again to recheck."
            : "Choose the interactive MRI or synchronized accessible table."
    }

    @ViewBuilder
    private var compactNavigator: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(model.mode == .memory ? "MEMORY LOBES" : "VISIBLE NEURONS")
                .font(.caption2.weight(.bold))
                .tracking(0.9)
                .foregroundStyle(.secondary)
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 11) {
                    if model.mode == .memory {
                        Button {
                            model.selectedDomain = ""
                            showsNavigator = false
                        } label: {
                            Label("Whole Brain", systemImage: "brain")
                        }
                        .buttonStyle(.plain)
                        Divider()
                        ForEach(model.domains, id: \.self) { domain in
                            Button {
                                model.selectedDomain = domain
                                showsNavigator = false
                            } label: {
                                HStack {
                                    Circle().fill(domainColor(domain)).frame(width: 7, height: 7)
                                    Text(domain).lineLimit(1)
                                    Spacer()
                                    if let count = model.graph?.domainCounts?[domain] {
                                        Text(count.formatted()).foregroundStyle(.secondary)
                                    }
                                }
                            }
                            .buttonStyle(.plain)
                        }
                    } else {
                        ForEach(model.connectome?.neurons ?? []) { neuron in
                            Button {
                                model.selectConnectomeAgent(neuron.agentID)
                                showsNavigator = false
                            } label: {
                                HStack {
                                    Circle().fill(domainColor(neuron.domain ?? "unassigned")).frame(width: 7, height: 7)
                                    VStack(alignment: .leading) {
                                        Text(neuron.name).lineLimit(1)
                                        Text(neuron.role.capitalized).font(.caption2).foregroundStyle(.secondary)
                                    }
                                }
                            }
                            .buttonStyle(.plain)
                        }
                    }
                }
            }
            .frame(maxHeight: 420)
        }
        .padding(16)
        .frame(width: 280, alignment: .leading)
    }

    private var graphSummary: String {
        if model.mode == .connectome {
            guard let connectome = model.connectome else { return model.isLoading ? "Mapping…" : "No verified snapshot" }
            return "\(connectome.neurons.count.formatted()) agents · \(connectome.synapses.count.formatted()) directed synapses"
        }
        guard let graph = model.graph else { return model.isLoading ? "Scanning…" : "No verified snapshot" }
        let rendered = graph.nodes.count.formatted()
        if let total = graph.total, total > graph.nodes.count { return "\(rendered) rendered · \(total.formatted()) available" }
        return "\(rendered) memories · \(graph.edges.count.formatted()) links"
    }

    private var currentHullOpacity: Double {
        model.mode == .memory ? memoryHullOpacity : connectomeHullOpacity
    }

    private var hasSelection: Bool {
        model.selectedNodeID != nil || model.selectedAgentID != nil ||
            model.selectedEngramID != nil || model.selectedConnection != nil || model.relatedMemoryFocus != nil
    }

    private var returnFocusTarget: BrainFocusTarget {
        metalRecovery.effectivePresentation == .table ? .table : .surface
    }

    private var presentationBinding: Binding<BrainPresentation> {
        Binding(
            get: { metalRecovery.presentation },
            set: { applyMetalEvent(.presentationSelected($0)) }
        )
    }

    private func handleMetalCapability(_ capability: BrainMetalCapability, attemptID: UInt64) {
        applyMetalEvent(.rendererReported(
            attemptID: attemptID,
            capability: capability,
            keyboardSurfaceOwned: keyboardFocus == .surface,
            accessibilitySurfaceOwned: accessibilityFocus == .surface
        ))
        if capability == .available, attemptID == pendingMetalRestorationAttemptID {
            preparedMetalRenderer = nil
        }
        completePendingMetalRestorationIfReady()
    }

    private func reportMetalSurface(_ surface: BrainMountedSurface, mounted: Bool) {
        if mounted {
            guard mountedMetalSurfaces.insert(surface).inserted else { return }
        } else {
            guard mountedMetalSurfaces.remove(surface) != nil else { return }
        }
        surfaceObserver(surface, mounted)
        completePendingMetalRestorationIfReady()
    }

    private func completePendingMetalRestorationIfReady() {
        guard let pendingAttemptID = pendingMetalRestorationAttemptID,
              pendingAttemptID == metalRecovery.attemptID,
              metalRecovery.capability == .available else { return }
        let expectedSurface: BrainMountedSurface = model.mode == .memory ? .memoryMRI : .connectomeMRI
        guard mountedMetalSurfaces.contains(expectedSurface) else { return }
        pendingMetalRestorationAttemptID = nil
        requestKeyboardFocus(.surface)
        requestAccessibilityFocus(.surface)
        postAccessibilityAnnouncement(announcementText(.restored))
    }

    private func retryMetal() {
        applyMetalEvent(.retryRequested)
    }

    private func beginMetalRetry(attemptID: UInt64) {
        let retryMode = model.mode
        retryTask?.cancel()
        retryTask = Task { @MainActor in
            await Task.yield()
            let result = await retryRendererBootstrap()
            guard !Task.isCancelled, model.mode == retryMode else { return }
            switch result {
            case let .success(renderer):
                applyMetalEvent(
                    .retryCompleted(attemptID: attemptID, succeeded: true),
                    preparedRenderer: renderer
                )
            case .failure:
                applyMetalEvent(.retryCompleted(attemptID: attemptID, succeeded: false))
            }
            if metalRecovery.attemptID == attemptID {
                retryTask = nil
            }
        }
    }

    private var metalRendererFactory: BrainMetalRendererFactory {
        { onPick in
            if let preparedMetalRenderer {
                guard let renderer = preparedMetalRenderer.take(onPick: onPick) else {
                    return .failure(.rendererInitialization)
                }
                return .success(renderer)
            }
            return rendererBootstrap(onPick)
        }
    }

    private func applyMetalEvent(
        _ event: BrainMetalRecoveryEvent,
        preparedRenderer: BrainMetalRenderer? = nil
    ) {
        let transition = BrainMetalRecoveryReducer.reduce(metalRecovery, event: event)
        let effects = transition.effects

        if effects.discardPreparedRenderer {
            retryTask?.cancel()
            retryTask = nil
            preparedMetalRenderer = nil
            pendingMetalRestorationAttemptID = nil
        }
        if effects.acceptPreparedRenderer, let preparedRenderer {
            preparedMetalRenderer = BrainMetalRendererHandoff(preparedRenderer)
        }
        metalRecovery = transition.state

        let defersRestoration = effects.announcement == .restored
        if defersRestoration {
            pendingMetalRestorationAttemptID = transition.state.attemptID
        } else if let announcement = effects.announcement {
            postAccessibilityAnnouncement(announcementText(announcement))
        }
        if !defersRestoration, let focus = effects.keyboardFocus {
            requestKeyboardFocus(focusTarget(focus))
        }
        if !defersRestoration, let focus = effects.accessibilityFocus {
            requestAccessibilityFocus(focusTarget(focus))
        }
        if let attemptID = effects.beginRetryAttempt {
            beginMetalRetry(attemptID: attemptID)
        }
    }

    private func focusTarget(_ destination: BrainMetalFocusDestination) -> BrainFocusTarget {
        switch destination {
        case .surface: .surface
        case .table: .table
        case .retryButton: .metalRetry
        }
    }

    private func announcementText(_ announcement: BrainMetalAnnouncement) -> String {
        switch announcement {
        case .unavailable: "Interactive MRI unavailable. Showing Accessible Table."
        case .stillUnavailable: "Interactive MRI is still unavailable. Accessible Table remains active."
        case .restored: "Interactive MRI restored."
        }
    }

    private func clearSelectionAndRestoreFocus() {
        showsInspector = false
        if model.mode == .memory {
            model.selectedNodeID = nil
            model.relatedMemories = nil
        } else {
            model.selectedAgentID = nil
            model.selectedEngramID = nil
            model.selectedConnection = nil
        }
        requestFocus(returnFocusTarget)
    }

    private func dismissCurrentSelectionAndRestoreFocus() {
        if model.mode == .memory {
            if let focus = model.relatedMemoryFocus {
                model.clearRelatedMemoryFocus()
                let cardIsMounted = !model.isDetailLoading && model.detailErrorMessage == nil &&
                    model.relatedMemories?.related.contains(where: { $0.id == focus.relatedMemoryID }) == true
                requestFocus(cardIsMounted ? .relatedMemory(focus.relatedMemoryID) : returnFocusTarget)
                return
            }
            model.selectedNodeID = nil
            model.relatedMemories = nil
            showsInspector = false
        } else {
            model.selectConnectomeSceneNode(nil)
            showsInspector = false
        }
        requestFocus(returnFocusTarget)
    }

    private func requestFocus(_ target: BrainFocusTarget) {
        requestKeyboardFocus(target)
        requestAccessibilityFocus(target)
    }

    private func requestKeyboardFocus(_ target: BrainFocusTarget) {
        keyboardFocusGeneration += 1
        let generation = keyboardFocusGeneration
        Task { @MainActor in
            await Task.yield()
            guard generation == keyboardFocusGeneration else { return }
            keyboardFocus = target
        }
    }

    private func requestAccessibilityFocus(_ target: BrainFocusTarget) {
        accessibilityFocusGeneration += 1
        let generation = accessibilityFocusGeneration
        Task { @MainActor in
            await Task.yield()
            guard generation == accessibilityFocusGeneration else { return }
            accessibilityFocus = target
        }
    }

    private func scheduleSelectionAnnouncement() {
        announcementGeneration += 1
        let generation = announcementGeneration
        Task { @MainActor in
            await Task.yield()
            guard generation == announcementGeneration else { return }
            postAccessibilityAnnouncement(selectionAnnouncement)
        }
    }

    private var selectionAnnouncement: String {
        let announcement: String
        if model.mode == .connectome {
            if let connection = model.selectedConnection {
                announcement = "Directed connection selected. \(model.agentName(connection.fromAgent)) to \(model.agentName(connection.toAgent)), \(connection.count) retained messages."
            } else if let engramID = model.selectedEngramID,
                      let engram = model.engrams?.engrams.first(where: { $0.id == engramID }) {
                announcement = "Engram selected in \(engram.domain), \(engram.corroborationCount) corroborations."
            } else if let neuron = model.selectedNeuron {
                announcement = "Agent selected. \(neuron.name), \(neuron.role), \(model.incomingTraffic(for: neuron.agentID)) incoming and \(model.outgoingTraffic(for: neuron.agentID)) outgoing retained messages."
            } else {
                announcement = "Brain selection cleared."
            }
        } else if let related = model.selectedRelatedMemory {
            announcement = "Related memory selected. \(trainTitle(related.kind)), \(related.domain), \(related.corroborationCount) corroborations."
        } else if let node = model.selectedNode {
            announcement = "Memory selected in \(node.domain), \(node.corroborationCount) corroborations."
        } else {
            announcement = "Brain selection cleared."
        }
        return boundedAnnouncementText(announcement)
    }

    private func boundedAnnouncementText(_ text: String) -> String {
        let collapsed = text.split(whereSeparator: \Character.isWhitespace).joined(separator: " ")
        guard collapsed.count > 140 else { return collapsed }
        return String(collapsed.prefix(137)) + "…"
    }

    private func postAccessibilityAnnouncement(_ message: String) {
        accessibilityAnnouncer(message)
    }

    private static func systemAccessibilityAnnouncement(_ message: String) {
        NSAccessibility.post(
            element: NSApplication.shared,
            notification: .announcementRequested,
            userInfo: [
                .announcement: message,
                .priority: NSAccessibilityPriorityLevel.medium.rawValue,
            ]
        )
    }

    private var hullOpacityBinding: Binding<Double> {
        Binding(
            get: { currentHullOpacity },
            set: {
                if model.mode == .memory { memoryHullOpacity = $0 }
                else { connectomeHullOpacity = $0 }
            }
        )
    }

    private func domainColor(_ domain: String) -> Color {
        let colors = [CerebrumTheme.cyan, CerebrumTheme.violet, CerebrumTheme.green, CerebrumTheme.amber, .pink, .blue]
        var hash = 7
        for scalar in domain.unicodeScalars { hash = (hash ^ Int(scalar.value)) &* 16_777_619 }
        return colors[abs(hash) % colors.count]
    }
}

private enum BrainFocusTarget: Hashable {
    case surface
    case table
    case inspectorClose
    case relatedMemory(String)
    case metalRetry
}

private struct BrainMetalRetryButton: NSViewRepresentable {
    let inFlight: Bool
    let action: @MainActor () -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(action: action)
    }

    func makeNSView(context: Context) -> BrainRetryNSButton {
        let button = BrainRetryNSButton(
            title: "Try MRI Again",
            target: context.coordinator,
            action: #selector(Coordinator.performAction)
        )
        button.bezelStyle = .rounded
        button.setButtonType(.momentaryPushIn)
        button.identifier = NSUserInterfaceItemIdentifier("brain-metal-retry")
        button.setAccessibilityIdentifier("brain-metal-retry")
        button.setAccessibilityRole(.button)
        button.setAccessibilityHelp("Attempts to restore the interactive Metal MRI while preserving the accessible table and current selection.")
        update(button)
        return button
    }

    func updateNSView(_ button: BrainRetryNSButton, context: Context) {
        context.coordinator.action = action
        update(button)
    }

    private func update(_ button: NSButton) {
        button.title = inFlight ? "Trying MRI…" : "Try MRI Again"
        button.isEnabled = !inFlight
        button.setAccessibilityLabel(inFlight ? "Trying MRI" : "Try MRI Again")
        button.setAccessibilityValue(inFlight ? "In progress" : nil)
    }

    final class BrainRetryNSButton: NSButton {
        override func accessibilityPerformPress() -> Bool {
            guard isEnabled else { return false }
            performClick(nil)
            return true
        }
    }

    @MainActor
    final class Coordinator: NSObject {
        var action: @MainActor () -> Void

        init(action: @escaping @MainActor () -> Void) {
            self.action = action
        }

        @objc func performAction() {
            action()
        }
    }
}

private struct BrainRefreshKey: Hashable {
    let mode: BrainMode
    let domain: String
    let status: String
}

private struct BrainDetailKey: Hashable {
    let mode: BrainMode
    let memoryID: String?
    let agentID: String?
}

private struct AgentNeuronInspectorView: View {
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    let neuron: ConnectomeNeuron
    @Bindable var model: BrainViewModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                VStack(alignment: .leading, spacing: 5) {
                    Text((neuron.domain ?? "UNASSIGNED").uppercased())
                        .font(.caption2.weight(.bold)).tracking(1).foregroundStyle(CerebrumTheme.cyan)
                    Text(neuron.name).font(.title2.weight(.bold)).fontDesign(.rounded)
                    Text(neuron.agentID).font(.system(.caption, design: .monospaced)).foregroundStyle(.secondary).textSelection(.enabled)
                }
                Text("Retained local traffic describes stored message history, not lifetime traffic or online presence.")
                    .font(.caption).foregroundStyle(.secondary)
                    .padding(10).background(.quaternary.opacity(0.3), in: RoundedRectangle(cornerRadius: 9))
                LabeledContent("Role", value: neuron.role.capitalized)
                LabeledContent("Incoming retained", value: model.incomingTraffic(for: neuron.agentID).formatted())
                LabeledContent("Outgoing retained", value: model.outgoingTraffic(for: neuron.agentID).formatted())
                LabeledContent("Visible peers", value: model.peerCount(for: neuron.agentID).formatted())
                Divider()
                HStack {
                    Text("DIRECTED CONNECTIONS").font(.caption2.weight(.bold)).tracking(0.8).foregroundStyle(.secondary)
                    Spacer()
                    Text(model.selectedAgentConnections.count.formatted()).font(.caption.monospacedDigit()).foregroundStyle(.tertiary)
                }
                if model.selectedAgentConnections.isEmpty {
                    Text("No retained directed traffic for this agent.").font(.caption).foregroundStyle(.secondary)
                } else {
                    ForEach(model.selectedAgentConnections, id: \.id) { connection in
                        Button {
                            withAnimation(reduceMotion ? nil : .snappy(duration: 0.2)) {
                                model.selectedConnection = model.selectedConnection == connection ? nil : connection
                                model.selectedEngramID = nil
                            }
                        } label: {
                            connectionRow(connection)
                        }
                        .buttonStyle(.plain)
                        .accessibilityLabel(connectionAccessibilityLabel(connection))
                    }
                }
                Divider()
                HStack {
                    Text("VISIBLE ENGRAMS").font(.caption2.weight(.bold)).tracking(0.8).foregroundStyle(.secondary)
                    Spacer()
                    if model.isDetailLoading { ProgressView().controlSize(.small) }
                }
                if let error = model.detailErrorMessage {
                    Text(error).font(.caption).foregroundStyle(CerebrumTheme.amber)
                } else if let engrams = model.engrams, engrams.agentID == neuron.agentID {
                    if engrams.engrams.isEmpty {
                        Text("No committed visible memories for this agent.").font(.caption).foregroundStyle(.secondary)
                    } else {
                        ForEach(engrams.engrams) { engram in
                            Button {
                                withAnimation(reduceMotion ? nil : .snappy(duration: 0.2)) {
                                    model.selectedEngramID = model.selectedEngramID == engram.id ? nil : engram.id
                                    model.selectedConnection = nil
                                }
                            } label: {
                                VStack(alignment: .leading, spacing: 5) {
                                    Text(engram.content).font(.callout).lineLimit(3).multilineTextAlignment(.leading)
                                    HStack {
                                        Text(engram.domain)
                                        Spacer()
                                        Text(engram.confidence, format: .percent.precision(.fractionLength(0)))
                                        Text("· \(engram.corroborationCount) corroborations")
                                    }.font(.caption2).foregroundStyle(.secondary)
                                    if let visible = engram.corroborators, !visible.isEmpty {
                                        let visibleNames = model.visibleCorroborators(visible).map { model.agentName($0) }
                                        if !visibleNames.isEmpty {
                                            Text("Visible bridges: \(visibleNames.joined(separator: ", "))")
                                                .font(.caption2).foregroundStyle(CerebrumTheme.violet)
                                        }
                                    }
                                }
                                .padding(10)
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .background(
                                    model.selectedEngramID == engram.id
                                        ? CerebrumTheme.violet.opacity(0.16)
                                        : Color(nsColor: .separatorColor).opacity(0.12),
                                    in: RoundedRectangle(cornerRadius: 9)
                                )
                                .overlay {
                                    if model.selectedEngramID == engram.id {
                                        RoundedRectangle(cornerRadius: 9).stroke(CerebrumTheme.violet.opacity(0.65))
                                    }
                                }
                            }
                            .buttonStyle(.plain)
                            .accessibilityLabel("Engram, \(engram.content), \(engram.domain), \(engram.corroborationCount) corroborations")
                        }
                    }
                    if engrams.continuationRequired == true {
                        Text("Showing the highest-confidence bounded engram sample.")
                            .font(.caption2).foregroundStyle(.secondary)
                    }
                }
            }.padding(20)
        }
        .navigationTitle("Agent Neuron")
    }

    private func connectionRow(_ connection: ConnectomeSynapse) -> some View {
        let outgoing = connection.fromAgent == neuron.agentID
        let peerID = outgoing ? connection.toAgent : connection.fromAgent
        return VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 7) {
                Image(systemName: outgoing ? "arrow.up.right" : "arrow.down.left")
                    .foregroundStyle(outgoing ? CerebrumTheme.cyan : CerebrumTheme.green)
                Text(outgoing ? "To \(model.agentName(peerID))" : "From \(model.agentName(peerID))")
                    .font(.callout.weight(.medium)).lineLimit(1)
                Spacer()
                Text(connection.count.formatted()).font(.caption.monospacedDigit())
            }
            HStack {
                Text("Retained messages")
                Spacer()
                if let date = connection.lastFiredDate { Text(date, style: .relative) }
                else { Text("Unknown activity time") }
            }
            .font(.caption2).foregroundStyle(.secondary)
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            model.selectedConnection == connection
                ? CerebrumTheme.cyan.opacity(0.14)
                : Color(nsColor: .separatorColor).opacity(0.12),
            in: RoundedRectangle(cornerRadius: 9)
        )
        .overlay {
            if model.selectedConnection == connection {
                RoundedRectangle(cornerRadius: 9).stroke(CerebrumTheme.cyan.opacity(0.62))
            }
        }
    }

    private func connectionAccessibilityLabel(_ connection: ConnectomeSynapse) -> String {
        let outgoing = connection.fromAgent == neuron.agentID
        let peerID = outgoing ? connection.toAgent : connection.fromAgent
        return "\(outgoing ? "Outgoing to" : "Incoming from") \(model.agentName(peerID)), \(connection.count) retained messages"
    }
}

private struct BrainNodeInspectorView: View {
    let node: BrainNode

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                VStack(alignment: .leading, spacing: 5) {
                    Text(node.domain.uppercased())
                        .font(.caption2.weight(.bold)).tracking(1).foregroundStyle(CerebrumTheme.cyan)
                    Text("Memory Focus")
                        .font(.title2.weight(.bold)).fontDesign(.rounded)
                }
                Text(node.content)
                    .textSelection(.enabled)
                    .lineSpacing(3)
                    .padding(14)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 11))
                LabeledContent("Stored confidence") {
                    Text(node.confidence, format: .percent.precision(.fractionLength(0))).monospacedDigit()
                }
                ProgressView(value: node.confidence).tint(CerebrumTheme.green)
                LabeledContent("Type", value: node.memoryType.capitalized)
                LabeledContent("Status", value: node.status.capitalized)
                LabeledContent("Corroborations", value: node.corroborationCount.formatted())
                LabeledContent("Created") { Text(node.createdAt.formatted(date: .abbreviated, time: .shortened)) }
                LabeledContent("Author") { Text(node.agentLabel ?? node.agent).textSelection(.enabled) }
                if let tags = node.tags, !tags.isEmpty {
                    VStack(alignment: .leading, spacing: 7) {
                        Text("TAGS").font(.caption2.weight(.bold)).tracking(0.8).foregroundStyle(.secondary)
                        Text(tags.joined(separator: "  ·  ")).font(.caption).foregroundStyle(CerebrumTheme.cyan)
                    }
                }
                Divider()
                VStack(alignment: .leading, spacing: 4) {
                    Text("MEMORY ID").font(.caption2.weight(.bold)).tracking(0.8).foregroundStyle(.secondary)
                    Text(node.id).font(.system(.caption, design: .monospaced)).textSelection(.enabled)
                }
            }
            .padding(20)
        }
        .navigationTitle("Memory Focus")
    }
}
