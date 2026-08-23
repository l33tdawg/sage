import Foundation
import Testing
@testable import SAGECerebrumNative

@Test func everyPrimaryCerebrumRouteHasANativeDestination() {
    #expect(AppRoute.allCases.map(\.rawValue) == [
        "overview", "brain", "search", "tasks", "import",
        "network", "access", "federation", "settings",
    ])
    #expect(Set(AppRoute.allCases.map(\.cerebrumHash)).count == 9)
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
    await search.handleLiveEvent(.init(name: "access", data: "", receivedAt: .now))
    #expect(search.memories.isEmpty)
    #expect(search.selection.isEmpty)
    #expect(search.inspectedMemoryID == nil)
    #expect(search.inspectedTags.isEmpty)
    #expect(search.authorLabels.isEmpty)

    let brain = BrainViewModel(api: api)
    brain.graph = .init(nodes: [], edges: [], total: 1, domainCounts: [:], domainLast: [:], continuationRequired: false, projection: nil)
    brain.connectome = .init(neurons: [], synapses: [])
    brain.selectedNodeID = "m1"
    brain.selectedAgentID = "a1"
    brain.selectedEngramID = "m1"
    brain.selectedConnection = .init(fromAgent: "a1", toAgent: "a2", count: 1, lastFired: "2026-08-23T00:00:00Z")
    brain.isDetailLoading = true
    brain.detailErrorMessage = "old authorization"
    brain.handleLiveEvent(.init(name: "access", data: "", receivedAt: .now))
    #expect(brain.graph == nil)
    #expect(brain.connectome == nil)
    #expect(brain.selectedNodeID == nil)
    #expect(brain.selectedAgentID == nil)
    #expect(brain.selectedEngramID == nil)
    #expect(brain.selectedConnection == nil)
    #expect(!brain.isDetailLoading)
    #expect(brain.detailErrorMessage == nil)
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

private actor MutationTestAPI: SAGEAPI {
    var forgetResults: [Result<MemoryMutationResponse, SAGEAPIError>]
    var forgetCalls: [String] = []
    var concurrentForgetCalls = 0
    var maximumConcurrentForgetCalls = 0
    var brainConnectome: ConnectomeEnvelope
    let brainEngrams: AgentEngramEnvelope?

    init(
        forgetResults: [Result<MemoryMutationResponse, SAGEAPIError>],
        connectome: ConnectomeEnvelope = .init(neurons: [], synapses: []),
        engrams: AgentEngramEnvelope? = nil
    ) {
        self.forgetResults = forgetResults
        self.brainConnectome = connectome
        self.brainEngrams = engrams
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
        .init(memories: [], total: 0, limit: 100, offset: 0, nextCursor: nil, continuationRequired: nil, authorLabels: nil, projection: nil)
    }
    func tags() async throws -> TagEnvelope { .init(tags: [], partial: false) }
    func memoryTags(id: String) async throws -> MemoryTagsEnvelope { .init(memoryID: id, tags: []) }
    func setMemoryTags(id: String, tags: [String]) async throws -> MemoryTagsEnvelope { .init(memoryID: id, tags: tags) }
    func addTag(_ tag: String, to ids: [String]) async throws -> BulkMemoryUpdateResponse { .init(status: "updated", updated: ids.count, total: ids.count) }
    func brainGraph(_ query: BrainGraphQuery) async throws -> BrainGraphEnvelope {
        .init(nodes: [], edges: [], total: 0, domainCounts: [:], domainLast: [:], continuationRequired: false, projection: nil)
    }
    func connectome() async throws -> ConnectomeEnvelope { brainConnectome }
    func setConnectome(_ value: ConnectomeEnvelope) { brainConnectome = value }
    func agentEngrams(agentID: String) async throws -> AgentEngramEnvelope {
        if let brainEngrams, brainEngrams.agentID == agentID { return brainEngrams }
        return .init(agentID: agentID, engrams: [], continuationRequired: false, projection: nil)
    }
    func relatedMemories(memoryID: String, limit: Int) async throws -> RelatedMemoryEnvelope {
        .init(id: memoryID, domain: "", content: "", related: [])
    }
    func forgetMemory(id: String) async throws -> MemoryMutationResponse {
        forgetCalls.append(id)
        concurrentForgetCalls += 1
        maximumConcurrentForgetCalls = max(maximumConcurrentForgetCalls, concurrentForgetCalls)
        defer { concurrentForgetCalls -= 1 }
        try? await Task.sleep(for: .milliseconds(2))
        return try forgetResults.removeFirst().get()
    }
    func events() async -> AsyncThrowingStream<DashboardEvent, Error> {
        AsyncThrowingStream { $0.finish() }
    }
}
