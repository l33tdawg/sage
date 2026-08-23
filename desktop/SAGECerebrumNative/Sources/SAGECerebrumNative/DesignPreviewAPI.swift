#if DEBUG
import Foundation

actor DesignPreviewAPI: SAGEAPI {
    func authStatus() async throws -> AuthStatus { .init(authRequired: false, authenticated: true) }
    func login(passphrase: String) async throws -> LoginResult { .init(ok: true, error: nil) }
    func lock() async throws {}

    func health() async throws -> DashboardHealth {
        try JSONDecoder.sageDashboard().decode(DashboardHealth.self, from: Data(#"""
        {"sage":"running","version":"12.0.0-beta.1","encrypted":true,"vault_locked":false,
         "uptime":"18h 42m","chain":{"block_height":"284921","catching_up":false,
         "chain_id":"sage-personal-native","app_version":"27","app_hash":"e7b31a4f9c8d",
         "mempool_txs":"0","peers":4,"idle":false,"stuck":false},
         "embedder":{"provider":"ollama","model":"nomic-embed-text","dimension":768,
         "ready":true,"semantic":true,"online":true,"reranker":{"enabled":true,"model":"bge-reranker"}}}
        """#.utf8))
    }

    func stats() async throws -> DashboardStats {
        try JSONDecoder.sageDashboard().decode(DashboardStats.self, from: Data(#"""
        {"total_memories":12847,"by_domain":{"personal":5210,"work":4892,"research":2745},
         "by_status":{"committed":12847},"by_agent":{"codex":4821,"claude":3902,"human":4124},
         "db_size_bytes":428972032,"last_activity":"2026-08-23T10:42:00Z"}
        """#.utf8))
    }

    func agents() async throws -> AgentOverviewEnvelope {
        .init(agents: [
            .init(agentID: "codex", name: "Codex", registeredName: "codex", role: "agent", status: "active", provider: "openai", memoryCount: 4821, lastSeen: .now),
            .init(agentID: "claude", name: "Claude", registeredName: "claude", role: "agent", status: "active", provider: "anthropic", memoryCount: 3902, lastSeen: .now),
            .init(agentID: "local", name: "Local", registeredName: "local", role: "agent", status: "active", provider: "ollama", memoryCount: 4124, lastSeen: .now),
        ])
    }

    func validators() async throws -> ValidatorOverview {
        .init(count: 3, totalVotingPower: "30", validators: [], error: nil)
    }

    func federation() async throws -> FederationOverview {
        .init(localChainID: "sage-personal-native", localNetworkName: "SAGE Home", connections: [
            .init(remoteChainID: "sage-lantern", peerName: "Lantern", status: "active", expired: false, sharingPaused: false),
            .init(remoteChainID: "sage-lab", peerName: "Research Lab", status: "active", expired: false, sharingPaused: false),
        ])
    }

    func memories(_ query: MemoryListQuery) async throws -> MemoryListEnvelope {
        let all = Self.previewMemories
        let needle = query.text.lowercased()
        let filtered = all.filter { memory in
            (needle.isEmpty || memory.content.lowercased().contains(needle) || memory.domainTag.lowercased().contains(needle)) &&
            (query.domain.isEmpty || memory.domainTag == query.domain) &&
            (query.status.isEmpty || query.status == "active" ? memory.status != "deprecated" : memory.status == query.status) &&
            (query.agent.isEmpty || memory.submittingAgent == query.agent)
        }
        return .init(memories: filtered, total: filtered.count, limit: 100, offset: 0, nextCursor: nil, continuationRequired: nil, authorLabels: ["root-v23": "CEREBRUM Root"], projection: nil)
    }

    func tags() async throws -> TagEnvelope {
        .init(tags: [
            .init(tag: "architecture", count: 7),
            .init(tag: "native", count: 4),
            .init(tag: "research", count: 12),
            .init(tag: "release", count: 3),
        ], partial: false)
    }

    func memoryTags(id: String) async throws -> MemoryTagsEnvelope {
        .init(memoryID: id, tags: id.contains("native") ? ["native", "v12", "macos"] : ["architecture"])
    }

    func setMemoryTags(id: String, tags: [String]) async throws -> MemoryTagsEnvelope {
        .init(memoryID: id, tags: tags)
    }

    func addTag(_ tag: String, to ids: [String]) async throws -> BulkMemoryUpdateResponse {
        .init(status: "updated", updated: ids.count, total: ids.count)
    }

    func forgetMemory(id: String) async throws -> MemoryMutationResponse {
        .init(status: "deprecated", message: nil)
    }

    func brainGraph(_ query: BrainGraphQuery) async throws -> BrainGraphEnvelope {
        let decoder = JSONDecoder.sageDashboard()
        let seed = try decoder.decode(BrainGraphEnvelope.self, from: Data(Self.previewGraphJSON.utf8))
        let graph = Self.expandedBrainPreview(from: seed)
        guard !query.domain.isEmpty else { return graph }
        let nodes = graph.nodes.filter { $0.domain == query.domain }
        let ids = Set(nodes.map(\.id))
        return .init(nodes: nodes, edges: graph.edges.filter { ids.contains($0.source) && ids.contains($0.target) }, total: nodes.count, domainCounts: [query.domain: nodes.count], domainLast: nil, continuationRequired: false, projection: nil)
    }

    private static func expandedBrainPreview(from seed: BrainGraphEnvelope) -> BrainGraphEnvelope {
        let domains = ["architecture", "native", "release", "research", "security", "federation", "agents", "consensus"]
        let domainCounts = [
            "architecture": 3_000, "native": 2_400, "release": 1_450, "research": 1_900,
            "security": 1_300, "federation": 1_100, "agents": 900, "consensus": 797,
        ]
        let anchor = Date.now
        var nodes = seed.nodes
        for index in nodes.count ..< 520 {
            let template = seed.nodes[index % seed.nodes.count]
            let confidence = 0.56 + Double((index * 37) % 42) / 100
            nodes.append(.init(
                id: "preview-memory-\(index)",
                content: "Preview memory \(index) · \(domains[index % domains.count])",
                domain: domains[index % domains.count], confidence: min(confidence, 0.98),
                status: index % 47 == 0 ? "deprecated" : index % 31 == 0 ? "challenged" : "committed",
                memoryType: index % 5 == 0 ? "observation" : template.memoryType,
                createdAt: anchor.addingTimeInterval(-Double(index * 61_200)),
                agent: template.agent, agentLabel: template.agentLabel, agentIsRoot: template.agentIsRoot,
                tags: template.tags, corroborationCount: index % 9
            ))
        }
        var edges = seed.edges
        for index in 1 ..< nodes.count {
            let type = ["related", "supports", "precedes", "refines"][index % 4]
            edges.append(.init(source: nodes[index - 1].id, target: nodes[index].id, type: type))
            if index > 11, index % 3 == 0 {
                edges.append(.init(source: nodes[index - 11].id, target: nodes[index].id, type: "related"))
            }
        }
        return .init(
            nodes: nodes, edges: edges, total: 12_847, domainCounts: domainCounts, domainLast: nil,
            continuationRequired: true, projection: nil
        )
    }

    func connectome() async throws -> ConnectomeEnvelope {
        try JSONDecoder.sageDashboard().decode(ConnectomeEnvelope.self, from: Data(#"""
        {"neurons":[
          {"agent_id":"codex","name":"Codex","role":"member","domain":"architecture"},
          {"agent_id":"claude","name":"Claude","role":"member","domain":"research"},
          {"agent_id":"local","name":"Local Intelligence","role":"manager","domain":"native"}
        ],"synapses":[
          {"from_agent":"codex","to_agent":"claude","count":148,"last_fired":"2026-08-23T12:00:00.123456789Z"},
          {"from_agent":"claude","to_agent":"codex","count":92,"last_fired":"2026-08-23T11:52:00Z"},
          {"from_agent":"codex","to_agent":"local","count":61,"last_fired":"2026-08-23T10:20:00Z"},
          {"from_agent":"local","to_agent":"codex","count":47,"last_fired":"2026-08-22T22:10:00Z"}
        ]}
        """#.utf8))
    }

    func agentEngrams(agentID: String) async throws -> AgentEngramEnvelope {
        let all = try JSONDecoder.sageDashboard().decode(AgentEngramEnvelope.self, from: Data(#"""
        {"agent_id":"codex","engrams":[
          {"id":"g1","content":"Native CEREBRUM architecture","domain":"architecture","confidence":0.97,"status":"committed","memory_type":"fact","created_at":"2026-08-23T10:42:00Z","corroboration_count":4,"tags":["native","v12"],"corroborators":["claude","local"]},
          {"id":"g3","content":"Professional Apple-style interface","domain":"native","confidence":0.94,"status":"committed","memory_type":"fact","created_at":"2026-08-22T14:20:00Z","corroboration_count":2,"tags":["design"],"corroborators":["claude"]}
        ],"continuation_required":false}
        """#.utf8))
        return .init(agentID: agentID, engrams: all.engrams, continuationRequired: false, projection: nil)
    }

    func relatedMemories(memoryID: String, limit: Int) async throws -> RelatedMemoryEnvelope {
        try JSONDecoder.sageDashboard().decode(RelatedMemoryEnvelope.self, from: Data(#"""
        {"id":"g1","domain":"architecture","content":"Native CEREBRUM architecture","related":[
          {"id":"g2","content":"SSE events invalidate native projections before authoritative refetch.","domain":"architecture","confidence":0.91,"corroboration_count":3,"status":"committed","created_at":"2026-08-20T08:10:00Z","memory_type":"observation","kind":"do","relation":"same-topic","score":4.8},
          {"id":"g3","content":"Keep the Metal and accessible table selection model synchronized.","domain":"native","confidence":0.94,"corroboration_count":2,"status":"committed","created_at":"2026-08-22T14:20:00Z","memory_type":"fact","kind":"observation","relation":"similar","score":3.7},
          {"id":"g5","content":"Do not sign before native lifecycle ownership is complete.","domain":"release","confidence":0.84,"corroboration_count":1,"status":"committed","created_at":"2026-08-18T07:00:00Z","memory_type":"observation","kind":"dont","relation":"chain","score":6.0}
        ]}
        """#.utf8))
    }

    func events() async -> AsyncThrowingStream<DashboardEvent, Error> {
        AsyncThrowingStream { continuation in
            continuation.yield(.init(name: "consensus", data: "{}", receivedAt: .now))
        }
    }

    private static let previewMemories: [MemorySummary] = {
        let decoder = JSONDecoder.sageDashboard()
        return try! decoder.decode([MemorySummary].self, from: Data(#"""
        [
          {"memory_id":"mem-native-001","submitting_agent":"root-v23","content":"CEREBRUM v12 ships as a fully native macOS application using SwiftUI, AppKit, and Metal.","content_hash":"4f82c9aa","memory_type":"fact","domain_tag":"architecture","provider":"openai","confidence_score":0.97,"status":"committed","created_at":"2026-08-23T10:42:00Z","committed_at":"2026-08-23T10:42:06Z","corroboration_count":4},
          {"memory_id":"mem-native-002","submitting_agent":"codex","content":"The native design system uses semantic SF typography, restrained materials, and the SAGE cyan-violet identity.","content_hash":"ba19d881","memory_type":"observation","domain_tag":"native","provider":"openai","confidence_score":0.92,"status":"committed","created_at":"2026-08-23T09:15:00Z","corroboration_count":3},
          {"memory_id":"mem-sse-003","submitting_agent":"claude","content":"Dashboard SSE events are invalidation signals; every visible value is refreshed from the authoritative API.","content_hash":"7a312fe0","memory_type":"fact","domain_tag":"architecture","provider":"anthropic","confidence_score":0.89,"status":"committed","created_at":"2026-08-22T16:20:00Z","corroboration_count":2},
          {"memory_id":"mem-release-004","submitting_agent":"codex","content":"Signing and notarization remain deferred until native product surfaces reach functional parity.","content_hash":"92cc013b","memory_type":"observation","domain_tag":"release","provider":"openai","confidence_score":0.84,"status":"proposed","created_at":"2026-08-21T11:05:00Z","corroboration_count":1}
        ]
        """#.utf8))
    }()

    private static let previewGraphJSON = #"""
    {"nodes":[
      {"id":"g1","content":"Native CEREBRUM architecture","domain":"architecture","confidence":0.97,"status":"committed","memory_type":"fact","created_at":"2026-08-23T10:42:00Z","agent":"root-v23","agent_label":"CEREBRUM Root","agent_is_root":true,"tags":["native","v12"],"corroboration_count":4},
      {"id":"g2","content":"SSE invalidation and authoritative refetch","domain":"architecture","confidence":0.91,"status":"committed","memory_type":"observation","created_at":"2026-08-20T08:10:00Z","agent":"codex","tags":["sse"],"corroboration_count":3},
      {"id":"g3","content":"Professional Apple-style interface","domain":"native","confidence":0.94,"status":"committed","memory_type":"fact","created_at":"2026-08-22T14:20:00Z","agent":"codex","tags":["design"],"corroboration_count":2},
      {"id":"g4","content":"Metal MRI renderer","domain":"native","confidence":0.88,"status":"proposed","memory_type":"observation","created_at":"2026-08-23T11:30:00Z","agent":"codex","tags":["metal"],"corroboration_count":1},
      {"id":"g5","content":"Beta release remains unsigned","domain":"release","confidence":0.84,"status":"committed","memory_type":"observation","created_at":"2026-08-18T07:00:00Z","agent":"claude","tags":["beta"],"corroboration_count":1},
      {"id":"g6","content":"Sovereign local memory graph","domain":"research","confidence":0.90,"status":"committed","memory_type":"inference","created_at":"2026-07-30T12:00:00Z","agent":"local","tags":["graph"],"corroboration_count":2}
    ],"edges":[
      {"source":"g1","target":"g2","type":"domain"},{"source":"g3","target":"g4","type":"domain"},{"source":"g1","target":"g3","type":"supports"},{"source":"g4","target":"g6","type":"related"},{"source":"g5","target":"g1","type":"precedes"}
    ],"total":12847,"domain_counts":{"architecture":4102,"native":3690,"release":2120,"research":2935},"domain_last":{"native":"2026-08-23T11:30:00Z"},"continuation_required":false}
    """#
}
#endif
