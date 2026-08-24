import Foundation

struct BrainGraphEnvelope: Decodable, Equatable, Sendable {
    let nodes: [BrainNode]
    let edges: [BrainEdge]
    let total: Int?
    let domainCounts: [String: Int]?
    let domainLast: [String: String]?
    let continuationRequired: Bool?
    let projection: MemoryProjection?

    enum CodingKeys: String, CodingKey {
        case nodes, edges, total, projection
        case domainCounts = "domain_counts"
        case domainLast = "domain_last"
        case continuationRequired = "continuation_required"
    }
}

struct BrainNode: Decodable, Equatable, Identifiable, Hashable, Sendable {
    let id: String
    let content: String
    let domain: String
    let confidence: Double
    let status: String
    let memoryType: String
    let createdAt: Date
    let agent: String
    let agentLabel: String?
    let agentIsRoot: Bool?
    let tags: [String]?
    let corroborationCount: Int

    enum CodingKeys: String, CodingKey {
        case id, content, domain, confidence, status, agent, tags
        case memoryType = "memory_type"
        case createdAt = "created_at"
        case agentLabel = "agent_label"
        case agentIsRoot = "agent_is_root"
        case corroborationCount = "corroboration_count"
    }
}

struct BrainEdge: Decodable, Equatable, Hashable, Sendable {
    let source: String
    let target: String
    let type: String
    let weight: Double?
    let lastFired: Date?

    init(source: String, target: String, type: String, weight: Double? = nil, lastFired: Date? = nil) {
        self.source = source
        self.target = target
        self.type = type
        self.weight = weight
        self.lastFired = lastFired
    }

    private enum CodingKeys: String, CodingKey {
        case source, target, type, weight
        case lastFired = "last_fired"
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        source = try values.decode(String.self, forKey: .source)
        target = try values.decode(String.self, forKey: .target)
        type = try values.decode(String.self, forKey: .type)
        weight = try values.decodeIfPresent(Double.self, forKey: .weight)
        lastFired = try values.decodeIfPresent(String.self, forKey: .lastFired).flatMap(RFC3339Timestamp.parse)
    }
}

struct BrainGraphQuery: Equatable, Sendable {
    var limit = 1_500
    var status = "all"
    var domain = ""

    var queryItems: [URLQueryItem] {
        var items = [URLQueryItem(name: "limit", value: String(limit))]
        if !status.isEmpty { items.append(.init(name: "status", value: status)) }
        if !domain.isEmpty { items.append(.init(name: "domain", value: domain)) }
        return items
    }
}

enum BrainPresentation: String, CaseIterable, Identifiable, Sendable {
    case mri
    case table
    var id: String { rawValue }
    var title: String { self == .mri ? "MRI" : "Accessible Table" }
    var systemImage: String { self == .mri ? "brain" : "tablecells" }
}

enum BrainMode: String, CaseIterable, Identifiable, Sendable {
    case memory
    case connectome
    var id: String { rawValue }
    var title: String { self == .memory ? "Memory" : "Connectome" }
    var systemImage: String { self == .memory ? "brain" : "point.3.connected.trianglepath.dotted" }
}

struct ConnectomeEnvelope: Decodable, Equatable, Sendable {
    let neurons: [ConnectomeNeuron]
    let synapses: [ConnectomeSynapse]
}

struct ConnectomeNeuron: Decodable, Equatable, Hashable, Identifiable, Sendable {
    let agentID: String
    let name: String
    let role: String
    let domain: String?
    var id: String { agentID }

    enum CodingKeys: String, CodingKey {
        case name, role, domain
        case agentID = "agent_id"
    }
}

struct DirectedSynapseID: Equatable, Hashable, Sendable {
    let fromAgent: String
    let toAgent: String
}

struct ConnectomeSynapse: Decodable, Equatable, Hashable, Identifiable, Sendable {
    let fromAgent: String
    let toAgent: String
    let count: Int64
    let lastFired: String

    enum CodingKeys: String, CodingKey {
        case count
        case fromAgent = "from_agent"
        case toAgent = "to_agent"
        case lastFired = "last_fired"
    }

    var lastFiredDate: Date? { RFC3339Timestamp.parse(lastFired) }
    var id: DirectedSynapseID { .init(fromAgent: fromAgent, toAgent: toAgent) }
}

struct AgentEngramEnvelope: Decodable, Equatable, Sendable {
    let agentID: String
    let engrams: [AgentEngram]
    let continuationRequired: Bool?
    let projection: MemoryProjection?

    enum CodingKeys: String, CodingKey {
        case engrams, projection
        case agentID = "agent_id"
        case continuationRequired = "continuation_required"
    }
}

struct AgentEngram: Decodable, Equatable, Hashable, Identifiable, Sendable {
    let id: String
    let content: String
    let domain: String
    let confidence: Double
    let status: String
    let memoryType: String
    let createdAt: Date
    let corroborationCount: Int
    let tags: [String]?
    let corroborators: [String]?

    enum CodingKeys: String, CodingKey {
        case id, content, domain, confidence, status, tags, corroborators
        case memoryType = "memory_type"
        case createdAt = "created_at"
        case corroborationCount = "corroboration_count"
    }
}

struct RelatedMemoryEnvelope: Decodable, Equatable, Sendable {
    let id: String
    let domain: String
    let content: String
    let related: [RelatedMemory]
}

struct RelatedMemoryFocus: Equatable, Hashable, Sendable {
    let anchorMemoryID: String
    let relatedMemoryID: String
}

struct RelatedMemory: Decodable, Equatable, Hashable, Identifiable, Sendable {
    let id: String
    let content: String
    let domain: String
    let confidence: Double
    let corroborationCount: Int
    let status: String
    let createdAt: Date
    let memoryType: String
    let kind: String
    let relation: String
    let score: Double

    enum CodingKeys: String, CodingKey {
        case id, content, domain, confidence, status, kind, relation, score
        case corroborationCount = "corroboration_count"
        case createdAt = "created_at"
        case memoryType = "memory_type"
    }
}

private enum RFC3339Timestamp {
    static func parse(_ value: String) -> Date? {
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = fractional.date(from: value) { return date }
        return ISO8601DateFormatter().date(from: value)
    }
}
