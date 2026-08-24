import Foundation
import Observation

@MainActor
@Observable
final class BrainViewModel {
    private struct SnapshotScope: Hashable {
        let mode: BrainMode
        let domain: String
        let status: String
    }

    var mode: BrainMode = .memory
    var graph: BrainGraphEnvelope?
    var connectome: ConnectomeEnvelope?
    var connectomeWasTruncated = false
    var selectedNodeID: String? {
        didSet {
            guard selectedNodeID != oldValue else { return }
            relatedMemoryFocus = nil
            if relatedMemories?.id != selectedNodeID { relatedMemories = nil }
        }
    }
    var selectedAgentID: String?
    var selectedEngramID: String?
    var selectedConnectionID: DirectedSynapseID?
    var relatedMemoryFocus: RelatedMemoryFocus?
    var selectedDomain = "" {
        didSet { if selectedDomain != oldValue { invalidateMemoryPayload() } }
    }
    var status = "all" {
        didSet { if status != oldValue { invalidateMemoryPayload() } }
    }
    var engrams: AgentEngramEnvelope?
    var relatedMemories: RelatedMemoryEnvelope?
    var isLoading = false
    var isDetailLoading = false
    var errorMessage: String?
    var detailErrorMessage: String?
    var updatesAvailable: Bool {
        get { pendingUpdateScopes.contains(currentScope) }
        set {
            if newValue { pendingUpdateScopes.insert(currentScope) }
            else { pendingUpdateScopes.remove(currentScope) }
        }
    }
    var eventStreamState: CerebrumEventStreamState = .connecting

    private let api: any SAGEAPI
    private var requestGeneration = 0
    private var detailGeneration = 0
    private var coalescedRefresh: Task<Void, Never>?
    private var trafficByAgent: [String: AgentTraffic] = [:]
    private var completedScopes = Set<SnapshotScope>()
    private var staleScopes = Set<SnapshotScope>()
    private var lastUpdatedByScope: [SnapshotScope: Date] = [:]
    private var pendingUpdateScopes = Set<SnapshotScope>()
    private var loadedScopeByMode: [BrainMode: SnapshotScope] = [:]

    init(api: any SAGEAPI) {
        self.api = api
        #if DEBUG
        if ProcessInfo.processInfo.environment["SAGE_NATIVE_PREVIEW_BRAIN_MODE"] == "connectome" {
            mode = .connectome
        }
        selectedAgentID = ProcessInfo.processInfo.environment["SAGE_NATIVE_PREVIEW_AGENT"]
        selectedNodeID = ProcessInfo.processInfo.environment["SAGE_NATIVE_PREVIEW_MEMORY_ID"]
        #endif
    }

    private var currentScope: SnapshotScope {
        mode == .memory
            ? .init(mode: mode, domain: selectedDomain, status: status)
            : .init(mode: mode, domain: "", status: "")
    }

    var hasCompletedRefresh: Bool { completedScopes.contains(currentScope) }

    var isStale: Bool {
        get { staleScopes.contains(currentScope) }
        set {
            if newValue { staleScopes.insert(currentScope) }
            else { staleScopes.remove(currentScope) }
        }
    }

    var lastUpdated: Date? {
        get {
            guard loadedScopeByMode[mode] == currentScope else { return nil }
            return lastUpdatedByScope[currentScope]
        }
        set {
            lastUpdatedByScope[currentScope] = newValue
            if newValue != nil { loadedScopeByMode[mode] = currentScope }
        }
    }

    var dataStatus: CerebrumDataStatus {
        let snapshot: CerebrumSnapshotState
        if let lastUpdated {
            if isStale {
                snapshot = .refreshFailed(updatedAt: lastUpdated)
            } else if mode == .memory, graph?.projection?.partial == true {
                snapshot = .partial(updatedAt: lastUpdated, detail: graph?.projection?.message)
            } else {
                snapshot = .available(updatedAt: lastUpdated)
            }
        } else {
            snapshot = isLoading || !hasCompletedRefresh ? .loading : .unavailable
        }
        return .init(
            snapshot: snapshot,
            events: eventStreamState,
            isRefreshing: isLoading,
            hasPendingUpdate: updatesAvailable
        )
    }

    var selectedNode: BrainNode? { graph?.nodes.first { $0.id == selectedNodeID } }
    var selectedRelatedMemory: RelatedMemory? {
        guard let relatedMemoryFocus,
              relatedMemoryFocus.anchorMemoryID == selectedNodeID,
              relatedMemories?.id == relatedMemoryFocus.anchorMemoryID
        else { return nil }
        return relatedMemories?.related.first { $0.id == relatedMemoryFocus.relatedMemoryID }
    }
    var sceneFocusedMemoryID: String? {
        guard let relatedMemoryFocus,
              graph?.nodes.contains(where: { $0.id == relatedMemoryFocus.relatedMemoryID }) == true
        else { return selectedNodeID }
        return relatedMemoryFocus.relatedMemoryID
    }
    var selectedNeuron: ConnectomeNeuron? { connectome?.neurons.first { $0.agentID == selectedAgentID } }
    var hasVisibleInspector: Bool { mode == .memory ? selectedNode != nil : selectedNeuron != nil }
    var selectedEngram: AgentEngram? { engrams?.engrams.first { $0.id == selectedEngramID } }
    var selectedConnection: ConnectomeSynapse? {
        get { connectome?.synapses.first { $0.id == selectedConnectionID } }
        set { selectedConnectionID = newValue?.id }
    }
    var selectedConnectionEdge: BrainEdge? {
        selectedConnection.map {
            .init(
                source: Self.agentSceneID($0.fromAgent),
                target: Self.agentSceneID($0.toAgent),
                type: "synapse",
                lastFired: $0.lastFiredDate
            )
        }
    }
    var selectedConnectomeSceneID: String? {
        if let selectedEngramID { return Self.engramSceneID(selectedEngramID) }
        return selectedAgentID.map(Self.agentSceneID)
    }
    var domains: [String] {
        let keys = graph?.domainCounts?.keys ?? Dictionary(uniqueKeysWithValues: (graph?.nodes ?? []).map { ($0.domain, 0) }).keys
        return keys.sorted()
    }

    func incomingTraffic(for agentID: String) -> Int64 {
        trafficByAgent[agentID]?.incoming ?? 0
    }

    func outgoingTraffic(for agentID: String) -> Int64 {
        trafficByAgent[agentID]?.outgoing ?? 0
    }

    func peerCount(for agentID: String) -> Int {
        trafficByAgent[agentID]?.peers.count ?? 0
    }

    func lastActivity(for agentID: String) -> Date? {
        trafficByAgent[agentID]?.lastActivity
    }

    func totalTraffic(for agentID: String) -> Int64 {
        Self.saturatingAdd(incomingTraffic(for: agentID), outgoingTraffic(for: agentID))
    }

    var connectomeSceneNodes: [BrainNode] {
        let neurons = connectome?.neurons ?? []
        let maximum = max(neurons.map { totalTraffic(for: $0.agentID) }.max() ?? 1, 1)
        var nodes = neurons.map { neuron in
            let traffic = totalTraffic(for: neuron.agentID)
            return BrainNode(
                id: Self.agentSceneID(neuron.agentID),
                content: neuron.name,
                domain: neuron.domain ?? "unassigned",
                confidence: 0.45 + 0.55 * Double(traffic) / Double(maximum),
                status: "active",
                memoryType: neuron.role,
                createdAt: mostRecentActivity(for: neuron.agentID) ?? .now,
                agent: neuron.agentID,
                agentLabel: neuron.name,
                agentIsRoot: false,
                tags: nil,
                corroborationCount: peerCount(for: neuron.agentID)
            )
        }
        if let selectedAgentID, engrams?.agentID == selectedAgentID {
            nodes.append(contentsOf: (engrams?.engrams ?? []).prefix(Self.maximumOverlayEngrams).map { engram in
                BrainNode(
                    id: Self.engramSceneID(engram.id), content: engram.content, domain: engram.domain,
                    confidence: engram.confidence, status: engram.status, memoryType: "__engram__",
                    createdAt: engram.createdAt, agent: selectedAgentID, agentLabel: nil,
                    agentIsRoot: false, tags: engram.tags, corroborationCount: engram.corroborationCount
                )
            })
        }
        return nodes
    }

    var connectomeSceneEdges: [BrainEdge] {
        var edges = (connectome?.synapses ?? []).map {
            BrainEdge(
                source: Self.agentSceneID($0.fromAgent), target: Self.agentSceneID($0.toAgent),
                type: "synapse", weight: Double($0.count), lastFired: $0.lastFiredDate
            )
        }
        guard let selectedAgentID, engrams?.agentID == selectedAgentID else {
            return BrainEdgeLOD.select(
                edges, visibleNodeIDs: Set(connectomeSceneNodes.map(\.id)),
                highlighted: selectedConnectionEdge, selectedAgentSceneID: nil,
                policy: .init(maximumEdges: Self.maximumSceneEdges)
            )
        }
        let visibleAgents = Set((connectome?.neurons ?? []).map(\.agentID))
        for engram in engrams?.engrams ?? [] {
            let engramID = Self.engramSceneID(engram.id)
            edges.append(.init(source: Self.agentSceneID(selectedAgentID), target: engramID, type: "engram"))
            for corroborator in engram.corroborators ?? [] where visibleAgents.contains(corroborator) {
                edges.append(.init(source: engramID, target: Self.agentSceneID(corroborator), type: "corroborates"))
            }
        }
        return BrainEdgeLOD.select(
            edges, visibleNodeIDs: Set(connectomeSceneNodes.map(\.id)),
            highlighted: selectedConnectionEdge,
            selectedAgentSceneID: Self.agentSceneID(selectedAgentID),
            policy: .init(maximumEdges: Self.maximumSceneEdges)
        )
    }

    var selectedAgentConnections: [ConnectomeSynapse] {
        guard let selectedAgentID else { return [] }
        return (connectome?.synapses ?? [])
            .filter { $0.fromAgent == selectedAgentID || $0.toAgent == selectedAgentID }
            .sorted {
                if $0.count != $1.count { return $0.count > $1.count }
                if $0.fromAgent != $1.fromAgent { return $0.fromAgent < $1.fromAgent }
                return $0.toAgent < $1.toAgent
            }
    }

    func agentName(_ agentID: String) -> String {
        connectome?.neurons.first { $0.agentID == agentID }?.name ?? agentID
    }

    func visibleCorroborators(_ agentIDs: [String]) -> [String] {
        let visible = Set((connectome?.neurons ?? []).map(\.agentID))
        return agentIDs.filter(visible.contains)
    }

    func selectConnectomeSceneNode(_ sceneID: String?) {
        guard let sceneID else {
            if selectedEngramID != nil || selectedConnection != nil {
                selectedEngramID = nil
                selectedConnection = nil
            } else {
                selectedAgentID = nil
            }
            return
        }
        if let agentID = Self.rawSceneID(sceneID, prefix: "agent:") {
            selectedAgentID = agentID
            selectedEngramID = nil
            selectedConnection = nil
        } else if let engramID = Self.rawSceneID(sceneID, prefix: "engram:"),
                  engrams?.engrams.contains(where: { $0.id == engramID }) == true {
            selectedEngramID = engramID
            selectedConnection = nil
        }
    }

    func selectConnectomeAgent(_ agentID: String?) {
        if agentID != selectedAgentID { selectedAgentID = agentID }
        selectedEngramID = nil
        selectedConnectionID = nil
    }

    func selectConnectomeSceneEdge(_ edge: BrainRenderEdgeID) {
        guard edge.type == "synapse",
              let fromAgent = Self.rawSceneID(edge.source, prefix: "agent:"),
              let toAgent = Self.rawSceneID(edge.target, prefix: "agent:"),
              connectome?.synapses.contains(where: { $0.fromAgent == fromAgent && $0.toAgent == toAgent }) == true
        else { return }
        if selectedAgentID != fromAgent && selectedAgentID != toAgent { selectedAgentID = fromAgent }
        selectedEngramID = nil
        let id = DirectedSynapseID(fromAgent: fromAgent, toAgent: toAgent)
        selectedConnectionID = selectedConnectionID == id ? nil : id
    }

    func refresh() async {
        requestGeneration += 1
        let generation = requestGeneration
        let requestedMode = mode
        let requestedScope = currentScope
        var didComplete = false
        isLoading = true
        errorMessage = nil
        defer {
            if generation == requestGeneration {
                isLoading = false
                if didComplete { completedScopes.insert(requestedScope) }
            }
        }
        do {
            switch requestedMode {
            case .memory:
                let response = try await api.brainGraph(.init(limit: 1_500, status: status, domain: selectedDomain))
                guard generation == requestGeneration, currentScope == requestedScope,
                      !Task.isCancelled else { return }
                graph = response
                if let selectedNodeID, !response.nodes.contains(where: { $0.id == selectedNodeID }) {
                    self.selectedNodeID = nil
                    relatedMemories = nil
                }
            case .connectome:
                let response = try await api.connectome()
                guard generation == requestGeneration, currentScope == requestedScope,
                      !Task.isCancelled else { return }
                let bounded = boundedConnectome(response)
                connectome = bounded
                rebuildTrafficIndex(bounded)
                if let selectedAgentID, !bounded.neurons.contains(where: { $0.agentID == selectedAgentID }) {
                    self.selectedAgentID = nil
                    selectedEngramID = nil
                    selectedConnection = nil
                    engrams = nil
                }
                if let selectedConnectionID, !bounded.synapses.contains(where: { $0.id == selectedConnectionID }) {
                    self.selectedConnectionID = nil
                }
            }
            isStale = false
            updatesAvailable = false
            if let displacedScope = loadedScopeByMode[requestedMode], displacedScope != requestedScope {
                completedScopes.remove(displacedScope)
                staleScopes.remove(displacedScope)
                lastUpdatedByScope[displacedScope] = nil
                pendingUpdateScopes.remove(displacedScope)
            }
            loadedScopeByMode[requestedMode] = requestedScope
            lastUpdatedByScope[requestedScope] = .now
            didComplete = true
        } catch is CancellationError {
            return
        } catch {
            guard generation == requestGeneration, currentScope == requestedScope,
                  !Task.isCancelled else { return }
            errorMessage = error.localizedDescription
            if lastUpdatedByScope[requestedScope] != nil { staleScopes.insert(requestedScope) }
            didComplete = true
        }
    }

    func refreshIncludingPinnedDetail() async {
        let requestedMode = mode
        await refresh()
        guard mode == requestedMode, errorMessage == nil else { return }
        if requestedMode == .memory, selectedNodeID != nil { await loadRelatedForSelection() }
        if requestedMode == .connectome, selectedAgentID != nil { await loadEngramsForSelection() }
    }

    func loadRelatedForSelection() async {
        detailGeneration += 1
        let generation = detailGeneration
        guard mode == .memory, let selectedNodeID else {
            relatedMemories = nil
            isDetailLoading = false
            detailErrorMessage = nil
            return
        }
        if relatedMemories?.id != selectedNodeID {
            relatedMemories = nil
            relatedMemoryFocus = nil
        }
        isDetailLoading = true
        detailErrorMessage = nil
        defer { if generation == detailGeneration { isDetailLoading = false } }
        do {
            let response = try await api.relatedMemories(memoryID: selectedNodeID, limit: 50)
            guard generation == detailGeneration, self.selectedNodeID == selectedNodeID, mode == .memory else { return }
            relatedMemories = response
            if let relatedMemoryFocus,
               relatedMemoryFocus.anchorMemoryID != selectedNodeID ||
               response.id != relatedMemoryFocus.anchorMemoryID ||
               !response.related.contains(where: { $0.id == relatedMemoryFocus.relatedMemoryID }) {
                self.relatedMemoryFocus = nil
            }
        } catch is CancellationError {
            return
        } catch {
            guard generation == detailGeneration else { return }
            detailErrorMessage = error.localizedDescription
        }
    }

    func loadEngramsForSelection() async {
        detailGeneration += 1
        let generation = detailGeneration
        guard mode == .connectome, let selectedAgentID else {
            engrams = nil
            isDetailLoading = false
            detailErrorMessage = nil
            return
        }
        if engrams?.agentID != selectedAgentID { engrams = nil }
        isDetailLoading = true
        detailErrorMessage = nil
        defer { if generation == detailGeneration { isDetailLoading = false } }
        do {
            let response = try await api.agentEngrams(agentID: selectedAgentID)
            guard generation == detailGeneration, self.selectedAgentID == selectedAgentID, mode == .connectome else { return }
            engrams = response
            if let selectedEngramID, !response.engrams.contains(where: { $0.id == selectedEngramID }) {
                self.selectedEngramID = nil
            }
        } catch is CancellationError {
            return
        } catch {
            guard generation == detailGeneration else { return }
            detailErrorMessage = error.localizedDescription
        }
    }

    func selectRelatedMemory(_ memory: RelatedMemory) {
        guard mode == .memory,
              let selectedNodeID,
              relatedMemories?.id == selectedNodeID,
              relatedMemories?.related.contains(where: { $0.id == memory.id }) == true
        else { return }
        let next = RelatedMemoryFocus(anchorMemoryID: selectedNodeID, relatedMemoryID: memory.id)
        relatedMemoryFocus = relatedMemoryFocus == next ? nil : next
    }

    func clearRelatedMemoryFocus() { relatedMemoryFocus = nil }

    func runLiveUpdates() async {
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await self.consumeEvents() }
            group.addTask { await self.pollScheduledRefresh() }
            _ = await group.next()
            group.cancelAll()
        }
    }

    private func consumeEvents() async {
        eventStreamState = .connecting
        let stream = await api.events()
        do {
            for try await element in stream {
                if Task.isCancelled { break }
                handleEventStreamElement(element)
            }
        } catch is CancellationError {
            return
        } catch SAGEAPIError.unauthorized {
            eventStreamState = .stopped
            purgeSensitiveSnapshots()
            return
        } catch {
            if !Task.isCancelled { eventStreamState = .reconnecting }
        }
        if !Task.isCancelled { eventStreamState = .reconnecting }
    }

    func handleEventStreamElement(_ element: DashboardEventStreamElement) {
        switch element {
        case let .state(state):
            eventStreamState = state
        case let .event(event):
            handleLiveEvent(event)
        }
    }

    private func pollScheduledRefresh() async {
        while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(30))
            if Task.isCancelled { break }
            await refreshIncludingPinnedDetail()
        }
    }

    func handleLiveEvent(_ event: DashboardEvent) {
        if event.name == "access" {
            purgeSensitiveSnapshots()
            scheduleRefresh(after: .zero)
        } else if mode == .connectome, event.name == "connectome" || event.name == "agent" {
            if selectedAgentID == nil { scheduleRefresh(after: .milliseconds(900)) }
            else { updatesAvailable = true }
        } else if Self.memoryInvalidations.contains(event.name) {
            if mode == .memory {
                if selectedNodeID == nil { scheduleRefresh(after: .seconds(3)) }
                else { updatesAvailable = true }
            } else if selectedAgentID != nil {
                updatesAvailable = true
            }
        }
    }

    private func scheduleRefresh(after delay: Duration) {
        coalescedRefresh?.cancel()
        coalescedRefresh = Task { [weak self] in
            do {
                try await Task.sleep(for: delay)
                guard !Task.isCancelled else { return }
                await self?.refresh()
            } catch { return }
        }
    }

    private func purgeSensitiveSnapshots() {
        requestGeneration += 1
        detailGeneration += 1
        coalescedRefresh?.cancel()
        graph = nil
        connectome = nil
        connectomeWasTruncated = false
        trafficByAgent = [:]
        engrams = nil
        relatedMemories = nil
        relatedMemoryFocus = nil
        selectedNodeID = nil
        selectedAgentID = nil
        selectedEngramID = nil
        selectedConnectionID = nil
        updatesAvailable = false
        staleScopes.removeAll()
        completedScopes.removeAll()
        lastUpdatedByScope.removeAll()
        pendingUpdateScopes.removeAll()
        loadedScopeByMode.removeAll()
        isLoading = false
        isDetailLoading = false
        errorMessage = nil
        detailErrorMessage = nil
    }

    private func invalidateMemoryPayload() {
        requestGeneration += 1
        coalescedRefresh?.cancel()
        graph = nil
        selectedNodeID = nil
        relatedMemories = nil
        relatedMemoryFocus = nil
        loadedScopeByMode[.memory] = nil
        completedScopes = Set(completedScopes.filter { $0.mode != .memory })
        staleScopes = Set(staleScopes.filter { $0.mode != .memory })
        lastUpdatedByScope = lastUpdatedByScope.filter { $0.key.mode != .memory }
        pendingUpdateScopes = Set(pendingUpdateScopes.filter { $0.mode != .memory })
        isLoading = false
        errorMessage = nil
    }

    private func mostRecentActivity(for agentID: String) -> Date? {
        trafficByAgent[agentID]?.lastActivity
    }

    private func boundedConnectome(_ response: ConnectomeEnvelope) -> ConnectomeEnvelope {
        var seen = Set<String>()
        let neurons = response.neurons
            .filter { !$0.agentID.isEmpty }
            .sorted {
                if $0.agentID != $1.agentID { return $0.agentID < $1.agentID }
                if $0.name != $1.name { return $0.name < $1.name }
                if $0.role != $1.role { return $0.role < $1.role }
                return ($0.domain ?? "") < ($1.domain ?? "")
            }
            .filter { seen.insert($0.agentID).inserted }
            .prefix(Self.maximumCanonicalNeurons)
        let IDs = Set(neurons.map(\.agentID))
        var canonicalSynapses: [DirectedSynapseID: ConnectomeSynapse] = [:]
        for edge in response.synapses where IDs.contains(edge.fromAgent) && IDs.contains(edge.toAgent) {
            guard let existing = canonicalSynapses[edge.id] else {
                canonicalSynapses[edge.id] = edge
                continue
            }
            let latest = [existing, edge].max {
                let lhs = $0.lastFiredDate ?? .distantPast
                let rhs = $1.lastFiredDate ?? .distantPast
                if lhs != rhs { return lhs < rhs }
                return $0.lastFired > $1.lastFired
            } ?? existing
            canonicalSynapses[edge.id] = .init(
                fromAgent: edge.fromAgent, toAgent: edge.toAgent,
                count: max(existing.count, edge.count), lastFired: latest.lastFired
            )
        }
        let synapses = canonicalSynapses.values
            .sorted {
                if $0.count != $1.count { return $0.count > $1.count }
                if ($0.lastFiredDate ?? .distantPast) != ($1.lastFiredDate ?? .distantPast) {
                    return ($0.lastFiredDate ?? .distantPast) > ($1.lastFiredDate ?? .distantPast)
                }
                if $0.fromAgent != $1.fromAgent { return $0.fromAgent < $1.fromAgent }
                if $0.toAgent != $1.toAgent { return $0.toAgent < $1.toAgent }
                return $0.lastFired < $1.lastFired
            }
            .prefix(Self.maximumCanonicalSynapses)
        connectomeWasTruncated = neurons.count != response.neurons.count ||
            canonicalSynapses.count != response.synapses.count || synapses.count != canonicalSynapses.count
        return .init(neurons: Array(neurons), synapses: Array(synapses))
    }

    private func rebuildTrafficIndex(_ snapshot: ConnectomeEnvelope) {
        var index: [String: AgentTraffic] = [:]
        for edge in snapshot.synapses {
            var source = index[edge.fromAgent, default: .init()]
            source.outgoing = Self.saturatingAdd(source.outgoing, edge.count)
            if edge.toAgent != edge.fromAgent { source.peers.insert(edge.toAgent) }
            source.lastActivity = max(source.lastActivity ?? .distantPast, edge.lastFiredDate ?? .distantPast)
            index[edge.fromAgent] = source

            var target = index[edge.toAgent, default: .init()]
            target.incoming = Self.saturatingAdd(target.incoming, edge.count)
            if edge.fromAgent != edge.toAgent { target.peers.insert(edge.fromAgent) }
            target.lastActivity = max(target.lastActivity ?? .distantPast, edge.lastFiredDate ?? .distantPast)
            index[edge.toAgent] = target
        }
        trafficByAgent = index
    }

    private static func saturatingAdd(_ left: Int64, _ right: Int64) -> Int64 {
        let (value, overflow) = left.addingReportingOverflow(right)
        return overflow ? Int64.max : value
    }

    private static func agentSceneID(_ id: String) -> String { "agent:\(id)" }
    private static func engramSceneID(_ id: String) -> String { "engram:\(id)" }
    private static func rawSceneID(_ id: String, prefix: String) -> String? {
        guard id.hasPrefix(prefix) else { return nil }
        return String(id.dropFirst(prefix.count))
    }

    private static let memoryInvalidations: Set<String> = [
        "remember", "forget", "reinstate", "cocommit", "import", "update", "consensus",
    ]

    private static let maximumCanonicalNeurons = 3_840
    private static let maximumCanonicalSynapses = 15_360
    private static let maximumOverlayEngrams = 256
    private static let maximumSceneEdges = 16_384
}

private struct AgentTraffic {
    var incoming: Int64 = 0
    var outgoing: Int64 = 0
    var peers = Set<String>()
    var lastActivity: Date?
}
