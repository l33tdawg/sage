import Foundation

struct AuthStatus: Decodable, Equatable, Sendable {
    let authRequired: Bool
    let authenticated: Bool

    enum CodingKeys: String, CodingKey {
        case authRequired = "auth_required"
        case authenticated
    }
}

struct LoginResult: Decodable, Equatable, Sendable {
    let ok: Bool
    let error: String?
}

struct AgentOverviewEnvelope: Decodable, Equatable, Sendable {
    let agents: [AgentOverview]
}

struct AgentOverview: Decodable, Equatable, Identifiable, Sendable {
    let agentID: String
    let name: String?
    let registeredName: String?
    let role: String?
    let status: String?
    let provider: String?
    let memoryCount: Int?
    let lastSeen: Date?

    var id: String { agentID }

    enum CodingKeys: String, CodingKey {
        case agentID = "agent_id"
        case name
        case registeredName = "registered_name"
        case role, status, provider
        case memoryCount = "memory_count"
        case lastSeen = "last_seen"
    }
}

struct ValidatorOverview: Decodable, Equatable, Sendable {
    let count: Int
    let totalVotingPower: String
    let validators: [ValidatorEntry]
    let error: String?

    enum CodingKeys: String, CodingKey {
        case count, validators, error
        case totalVotingPower = "total_voting_power"
    }
}

struct ValidatorEntry: Decodable, Equatable, Identifiable, Sendable {
    let address: String
    let agentID: String
    let votingPower: String
    let proposerPriority: String

    var id: String { address }

    enum CodingKeys: String, CodingKey {
        case address
        case agentID = "agent_id"
        case votingPower = "voting_power"
        case proposerPriority = "proposer_priority"
    }
}

struct FederationOverview: Decodable, Equatable, Sendable {
    let localChainID: String?
    let localNetworkName: String?
    let connections: [FederationConnection]
    let isEnabled: Bool

    static let disabled = FederationOverview(
        localChainID: nil,
        localNetworkName: nil,
        connections: [],
        isEnabled: false
    )

    enum CodingKeys: String, CodingKey {
        case localChainID = "local_chain_id"
        case localNetworkName = "local_network_name"
        case connections
    }

    init(localChainID: String?, localNetworkName: String?, connections: [FederationConnection], isEnabled: Bool = true) {
        self.localChainID = localChainID
        self.localNetworkName = localNetworkName
        self.connections = connections
        self.isEnabled = isEnabled
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        localChainID = try values.decodeIfPresent(String.self, forKey: .localChainID)
        localNetworkName = try values.decodeIfPresent(String.self, forKey: .localNetworkName)
        connections = try values.decodeIfPresent([FederationConnection].self, forKey: .connections) ?? []
        isEnabled = true
    }
}

struct FederationConnection: Decodable, Equatable, Identifiable, Sendable {
    let remoteChainID: String
    let peerName: String?
    let status: String
    let expired: Bool
    let sharingPaused: Bool

    var id: String { remoteChainID }

    enum CodingKeys: String, CodingKey {
        case remoteChainID = "remote_chain_id"
        case peerName = "peer_name"
        case status, expired
        case sharingPaused = "sharing_paused"
    }
}

struct DashboardStats: Decodable, Equatable, Sendable {
    let totalMemories: Int
    let byDomain: [String: Int]
    let byStatus: [String: Int]
    let byAgent: [String: Int]?
    let databaseSizeBytes: Int64
    let lastActivity: Date?

    enum CodingKeys: String, CodingKey {
        case totalMemories = "total_memories"
        case byDomain = "by_domain"
        case byStatus = "by_status"
        case byAgent = "by_agent"
        case databaseSizeBytes = "db_size_bytes"
        case lastActivity = "last_activity"
    }
}

struct DashboardHealth: Decodable, Equatable, Sendable {
    let sage: String
    let version: String
    let encrypted: Bool
    let vaultLocked: Bool
    let uptime: String
    let chain: ChainHealth?
    let embedder: EmbedderHealth?

    enum CodingKeys: String, CodingKey {
        case sage, version, encrypted, uptime, chain, embedder
        case vaultLocked = "vault_locked"
    }
}

struct ChainHealth: Decodable, Equatable, Sendable {
    let blockHeight: String?
    let blockTime: Date?
    let catchingUp: Bool?
    let chainID: String?
    let moniker: String?
    let appVersion: String?
    let appHash: String?
    let mempoolTransactions: String?
    let peers: Int?
    let idle: Bool?
    let stuck: Bool?

    enum CodingKeys: String, CodingKey {
        case blockHeight = "block_height"
        case blockTime = "block_time"
        case catchingUp = "catching_up"
        case chainID = "chain_id"
        case moniker
        case appVersion = "app_version"
        case appHash = "app_hash"
        case mempoolTransactions = "mempool_txs"
        case peers, idle, stuck
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        blockHeight = try values.decodeFlexibleStringIfPresent(forKey: .blockHeight)
        blockTime = try values.decodeIfPresent(Date.self, forKey: .blockTime)
        catchingUp = try values.decodeIfPresent(Bool.self, forKey: .catchingUp)
        chainID = try values.decodeIfPresent(String.self, forKey: .chainID)
        moniker = try values.decodeIfPresent(String.self, forKey: .moniker)
        appVersion = try values.decodeFlexibleStringIfPresent(forKey: .appVersion)
        appHash = try values.decodeIfPresent(String.self, forKey: .appHash)
        mempoolTransactions = try values.decodeFlexibleStringIfPresent(forKey: .mempoolTransactions)
        peers = try values.decodeFlexibleIntIfPresent(forKey: .peers)
        idle = try values.decodeIfPresent(Bool.self, forKey: .idle)
        stuck = try values.decodeIfPresent(Bool.self, forKey: .stuck)
    }
}

struct EmbedderHealth: Decodable, Equatable, Sendable {
    let provider: String
    let model: String?
    let dimension: Int
    let ready: Bool
    let semantic: Bool
    let online: Bool
    let reranker: RerankerHealth?
}

struct RerankerHealth: Decodable, Equatable, Sendable {
    let enabled: Bool
    let model: String?
}

private extension KeyedDecodingContainer {
    func decodeFlexibleStringIfPresent(forKey key: Key) throws -> String? {
        if let string = try? decode(String.self, forKey: key) { return string }
        if let integer = try? decode(Int.self, forKey: key) { return String(integer) }
        return nil
    }

    func decodeFlexibleIntIfPresent(forKey key: Key) throws -> Int? {
        if let integer = try? decode(Int.self, forKey: key) { return integer }
        if let string = try? decode(String.self, forKey: key) { return Int(string) }
        return nil
    }
}

extension JSONDecoder {
    static func sageDashboard() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }
}
