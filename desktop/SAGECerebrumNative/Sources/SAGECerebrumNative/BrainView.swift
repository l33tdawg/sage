import SwiftUI

struct BrainView: View {
    @Environment(\.scenePhase) private var scenePhase
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @State private var model: BrainViewModel
    @State private var presentation: BrainPresentation = .mri
    @State private var scanning = true
    @State private var flow = true
    @State private var memoryHullOpacity = 0.08
    @State private var connectomeHullOpacity = 0.03
    @State private var showsDisplayControls = false

    init(api: any SAGEAPI) {
        _model = State(initialValue: BrainViewModel(api: api))
    }

    var body: some View {
        ZStack {
            CerebrumBackdrop()
            VStack(alignment: .leading, spacing: 16) {
                header
                notices
                if model.mode == .memory, model.selectedNodeID != nil,
                   model.relatedMemories != nil || model.isDetailLoading || model.detailErrorMessage != nil {
                    GeometryReader { proxy in
                        VSplitView {
                            brainSurface.frame(minHeight: 240)
                            trainOfThoughtPane
                                .frame(
                                    minHeight: 150,
                                    idealHeight: min(220, proxy.size.height * 0.38),
                                    maxHeight: proxy.size.height * 0.52
                                )
                        }
                    }
                } else {
                    brainSurface
                }
            }
            .padding(CerebrumTheme.pagePadding)
        }
        .navigationTitle("Brain")
        .toolbar { brainToolbar }
        .inspector(isPresented: Binding(
            get: { model.hasVisibleInspector },
            set: {
                if !$0 {
                    if model.mode == .memory {
                        model.selectedNodeID = nil
                    } else {
                        model.selectedAgentID = nil
                        model.selectedEngramID = nil
                        model.selectedConnection = nil
                    }
                }
            }
        )) {
            if model.mode == .memory, let node = model.selectedNode {
                BrainNodeInspectorView(node: node)
                    .inspectorColumnWidth(min: 320, ideal: 380, max: 520)
            } else if model.mode == .connectome, let neuron = model.selectedNeuron {
                AgentNeuronInspectorView(neuron: neuron, model: model)
                    .inspectorColumnWidth(min: 340, ideal: 400, max: 540)
            }
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
        .onAppear {
            if reduceMotion { scanning = false; flow = false }
        }
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

    private func notice(
        _ text: String,
        systemImage: String,
        color: Color,
        action: (String, () -> Void)? = nil
    ) -> some View {
        HStack {
            Label(text, systemImage: systemImage).font(.callout).foregroundStyle(color)
            Spacer()
            if let action { Button(action.0, action: action.1).buttonStyle(.bordered) }
        }
        .padding(11)
        .background(color.opacity(0.08), in: RoundedRectangle(cornerRadius: 10))
    }

    private var brainSurface: some View {
        HStack(spacing: 0) {
            if model.mode == .memory { domainSidebar }
            else { agentSidebar }
            Divider()
            Group {
                if model.mode == .memory, let graph = model.graph {
                    if graph.nodes.isEmpty { emptyState }
                    else if presentation == .mri { memoryMRI(graph) }
                    else { memoryTable(graph) }
                } else if model.mode == .connectome, let connectome = model.connectome {
                    if connectome.neurons.isEmpty { connectomeEmptyState }
                    else if presentation == .mri { connectomeMRI(connectome) }
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
        .frame(minHeight: model.relatedMemories == nil ? 520 : 360)
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
    }

    private func memoryMRI(_ graph: BrainGraphEnvelope) -> some View {
        ZStack(alignment: .topLeading) {
            MetalBrainView(
                nodes: graph.nodes,
                edges: graph.edges,
                selectedID: model.selectedNodeID,
                highlightedEdge: nil,
                layout: .memory,
                autoRotate: scanning && !reduceMotion,
                flow: flow && !reduceMotion,
                hullOpacity: currentHullOpacity,
                onPick: { pick in
                    switch pick {
                    case let .node(id): model.selectedNodeID = id
                    case .edge: break
                    case .background: model.selectedNodeID = nil
                    }
                }
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
    }

    private func memoryTable(_ graph: BrainGraphEnvelope) -> some View {
        Table(graph.nodes, selection: $model.selectedNodeID) {
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
    }

    private func connectomeMRI(_ connectome: ConnectomeEnvelope) -> some View {
        ZStack(alignment: .topLeading) {
            MetalBrainView(
                nodes: model.connectomeSceneNodes,
                edges: model.connectomeSceneEdges,
                selectedID: model.selectedConnectomeSceneID,
                highlightedEdge: model.selectedConnectionEdge,
                layout: .connectome,
                autoRotate: scanning && !reduceMotion,
                flow: flow && !reduceMotion,
                hullOpacity: currentHullOpacity,
                onPick: { pick in
                    switch pick {
                    case let .node(id): model.selectConnectomeSceneNode(id)
                    case let .edge(edge): model.selectConnectomeSceneEdge(edge)
                    case .background: model.selectConnectomeSceneNode(nil)
                    }
                }
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
    }

    private func connectomeTable(_ connectome: ConnectomeEnvelope) -> some View {
        Table(connectome.neurons, selection: $model.selectedAgentID) {
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
                    model.selectedNodeID = nil
                    model.relatedMemories = nil
                }.labelStyle(.iconOnly)
            }
            if model.isDetailLoading {
                ProgressView("Tracing related memory…").controlSize(.small)
            } else if let error = model.detailErrorMessage {
                ContentUnavailableView("Train unavailable", systemImage: "exclamationmark.triangle", description: Text(error))
            } else if let related = model.relatedMemories {
                ScrollView(.horizontal) {
                    HStack(alignment: .top, spacing: 12) {
                        ForEach(["do", "dont", "observation", "note"], id: \.self) { kind in
                            trainGroup(kind: kind, memories: related.related.filter { $0.kind == kind })
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
                    Button {
                        model.selectRelatedMemory(memory)
                    } label: {
                        VStack(alignment: .leading, spacing: 5) {
                            Text(memory.content).font(.callout).lineLimit(3).multilineTextAlignment(.leading)
                            HStack {
                                Text(memory.relation.replacingOccurrences(of: "-", with: " ").capitalized)
                                Spacer()
                                Text(memory.confidence, format: .percent.precision(.fractionLength(0)))
                            }.font(.caption2).foregroundStyle(.secondary)
                        }
                        .padding(9).frame(width: 230, alignment: .leading)
                        .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 9))
                    }.buttonStyle(.plain)
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
            Picker("Brain mode", selection: $model.mode) {
                ForEach(BrainMode.allCases) { Label($0.title, systemImage: $0.systemImage).tag($0) }
            }
            .pickerStyle(.segmented)
            .frame(width: 210)

            Picker("Presentation", selection: $presentation) {
                ForEach(BrainPresentation.allCases) { Label($0.title, systemImage: $0.systemImage).tag($0) }
            }
            .pickerStyle(.segmented)
            .frame(width: 190)

            Button {
                scanning.toggle()
            } label: {
                Label(scanning ? "Pause Scan" : "Resume Scan", systemImage: scanning ? "pause.circle" : "play.circle")
            }
            .disabled(reduceMotion || presentation == .table)

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
                model.selectedNodeID = nil
                model.selectedAgentID = nil
                model.selectedEngramID = nil
                model.selectedConnection = nil
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
                            withAnimation(.snappy(duration: 0.2)) {
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
                                withAnimation(.snappy(duration: 0.2)) {
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
