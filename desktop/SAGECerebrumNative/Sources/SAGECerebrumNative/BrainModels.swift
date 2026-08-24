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

enum BrainMetalBootstrapFailure: Error, Equatable, Sendable {
    case rendererInitialization
}

enum BrainMetalCapability: Equatable, Sendable {
    case probing
    case available
    case unavailable(BrainMetalBootstrapFailure)
}

extension BrainMetalCapability {
    var isUnavailable: Bool {
        if case .unavailable = self { return true }
        return false
    }
}

struct BrainPresentationDecision: Equatable, Sendable {
    let effectivePresentation: BrainPresentation
    let mriEnabled: Bool
}

enum BrainPresentationPolicy {
    static func resolve(
        requested: BrainPresentation,
        capability: BrainMetalCapability
    ) -> BrainPresentationDecision {
        if case .unavailable = capability {
            return .init(effectivePresentation: .table, mriEnabled: false)
        }
        return .init(effectivePresentation: requested, mriEnabled: true)
    }
}

struct BrainResponsiveWidth: Equatable, Sendable {
    static let regularBoundary: CGFloat = 620
    static let expandedBoundary: CGFloat = 840

    let points: CGFloat

    init(points: CGFloat) {
        self.points = points.isFinite ? max(0, points) : 0
    }

    var tier: BrainResponsiveTier {
        if points < Self.regularBoundary { return .compact }
        if points < Self.expandedBoundary { return .regular }
        return .expanded
    }
}

enum BrainResponsiveTier: Equatable, Sendable {
    case compact
    case regular
    case expanded
}

struct BrainResponsiveLayoutPlan: Equatable, Sendable {
    let width: BrainResponsiveWidth
    let tier: BrainResponsiveTier
    let pagePadding: CGFloat
    let showsInlineNavigator: Bool
    let navigatorWidth: CGFloat
    let usesCompactToolbar: Bool
    let surfaceMinimumHeight: CGFloat
    let trainMinimumHeight: CGFloat
    let trainIdealHeight: CGFloat
    let trainMaximumHeight: CGFloat
    let inspectorMinimumWidth: CGFloat
    let inspectorIdealWidth: CGFloat
    let inspectorMaximumWidth: CGFloat
}

enum BrainResponsiveLayoutPolicy {
    static func resolve(size: CGSize, trainVisible: Bool) -> BrainResponsiveLayoutPlan {
        let width = BrainResponsiveWidth(points: size.width)
        let height = size.height.isFinite ? max(0, size.height) : 0

        let dimensions: TierDimensions = switch width.tier {
        case .compact:
            .init(
                pagePadding: 16,
                navigatorWidth: 0,
                surfaceMinimumHeight: trainVisible ? 200 : 280,
                trainMinimumHeight: 110,
                trainIdealHeight: 150,
                inspectorMinimumWidth: 300,
                inspectorIdealWidth: 340,
                inspectorMaximumWidth: 440
            )
        case .regular:
            .init(
                pagePadding: 22,
                navigatorWidth: 0,
                surfaceMinimumHeight: trainVisible ? 220 : 300,
                trainMinimumHeight: 130,
                trainIdealHeight: 190,
                inspectorMinimumWidth: 300,
                inspectorIdealWidth: 340,
                inspectorMaximumWidth: 440
            )
        case .expanded:
            .init(
                pagePadding: 28,
                navigatorWidth: 230,
                surfaceMinimumHeight: trainVisible ? 240 : 320,
                trainMinimumHeight: 150,
                trainIdealHeight: 220,
                inspectorMinimumWidth: 300,
                inspectorIdealWidth: 340,
                inspectorMaximumWidth: 440
            )
        }

        let contentHeight = max(0, height - (dimensions.pagePadding * 2))
        let splitAllowance: CGFloat = trainVisible && contentHeight > 0 ? 1 : 0
        let verticalBudget = max(0, contentHeight - splitAllowance)

        let trainMinimumHeight: CGFloat
        let trainIdealHeight: CGFloat
        let trainMaximumHeight: CGFloat
        let surfaceMinimumHeight: CGFloat
        if trainVisible {
            trainMinimumHeight = min(dimensions.trainMinimumHeight, verticalBudget * 0.35)
            surfaceMinimumHeight = min(
                dimensions.surfaceMinimumHeight,
                max(0, verticalBudget - trainMinimumHeight)
            )
            trainMaximumHeight = max(
                trainMinimumHeight,
                min(verticalBudget * 0.48, verticalBudget - surfaceMinimumHeight)
            )
            trainIdealHeight = min(
                trainMaximumHeight,
                max(trainMinimumHeight, dimensions.trainIdealHeight)
            )
        } else {
            trainMinimumHeight = 0
            trainIdealHeight = 0
            trainMaximumHeight = 0
            surfaceMinimumHeight = min(dimensions.surfaceMinimumHeight, verticalBudget)
        }

        return .init(
            width: width,
            tier: width.tier,
            pagePadding: dimensions.pagePadding,
            showsInlineNavigator: width.tier == .expanded,
            navigatorWidth: dimensions.navigatorWidth,
            usesCompactToolbar: width.tier != .expanded,
            surfaceMinimumHeight: surfaceMinimumHeight,
            trainMinimumHeight: trainMinimumHeight,
            trainIdealHeight: trainIdealHeight,
            trainMaximumHeight: trainMaximumHeight,
            inspectorMinimumWidth: dimensions.inspectorMinimumWidth,
            inspectorIdealWidth: dimensions.inspectorIdealWidth,
            inspectorMaximumWidth: dimensions.inspectorMaximumWidth
        )
    }

    private struct TierDimensions {
        let pagePadding: CGFloat
        let navigatorWidth: CGFloat
        let surfaceMinimumHeight: CGFloat
        let trainMinimumHeight: CGFloat
        let trainIdealHeight: CGFloat
        let inspectorMinimumWidth: CGFloat
        let inspectorIdealWidth: CGFloat
        let inspectorMaximumWidth: CGFloat
    }
}

enum BrainMetalFocusDestination: Equatable, Sendable {
    case surface
    case table
    case retryButton
}

enum BrainMetalAnnouncement: Equatable, Sendable {
    case unavailable
    case stillUnavailable
    case restored
}

enum BrainMountedSurface: Equatable, Hashable, Sendable {
    case inlineNavigator
    case compactNavigatorTrigger
    case metalFallbackNotice
    case metalRetryButton
    case memoryMRI
    case memoryTable
    case connectomeMRI
    case connectomeTable
}

struct BrainMetalRecoveryState: Equatable, Sendable {
    var presentation: BrainPresentation = .mri
    var capability: BrainMetalCapability = .probing
    var retryInFlight = false
    var attemptID: UInt64 = 0

    var effectivePresentation: BrainPresentation {
        BrainPresentationPolicy.resolve(
            requested: presentation,
            capability: capability
        ).effectivePresentation
    }
}

enum BrainMetalRecoveryEvent: Equatable, Sendable {
    case presentationSelected(BrainPresentation)
    case modeChanged
    case rendererReported(
        attemptID: UInt64,
        capability: BrainMetalCapability,
        keyboardSurfaceOwned: Bool,
        accessibilitySurfaceOwned: Bool
    )
    case retryRequested
    case retryCancelled
    case retryCompleted(attemptID: UInt64, succeeded: Bool)
}

struct BrainMetalRecoveryEffects: Equatable, Sendable {
    var keyboardFocus: BrainMetalFocusDestination?
    var accessibilityFocus: BrainMetalFocusDestination?
    var announcement: BrainMetalAnnouncement?
    var beginRetryAttempt: UInt64?
    var discardPreparedRenderer = false
    var acceptPreparedRenderer = false

    static let none = Self()
}

struct BrainMetalRecoveryTransition: Equatable, Sendable {
    let state: BrainMetalRecoveryState
    let effects: BrainMetalRecoveryEffects
}

enum BrainMetalRecoveryReducer {
    static func reduce(
        _ current: BrainMetalRecoveryState,
        event: BrainMetalRecoveryEvent
    ) -> BrainMetalRecoveryTransition {
        var state = current
        var effects = BrainMetalRecoveryEffects.none

        switch event {
        case let .presentationSelected(presentation):
            if presentation == .mri, state.capability.isUnavailable {
                return .init(state: current, effects: .none)
            }
            guard presentation != state.presentation else {
                return .init(state: current, effects: .none)
            }
            state.presentation = presentation
            state.attemptID &+= 1
            state.retryInFlight = false
            effects.discardPreparedRenderer = true
            if presentation == .mri {
                state.capability = .probing
                effects.keyboardFocus = .surface
                effects.accessibilityFocus = .surface
            } else {
                effects.keyboardFocus = .table
                effects.accessibilityFocus = .table
            }

        case .modeChanged:
            state.attemptID &+= 1
            state.retryInFlight = false
            effects.discardPreparedRenderer = true
            let destination: BrainMetalFocusDestination = state.effectivePresentation == .table ? .table : .surface
            effects.keyboardFocus = destination
            effects.accessibilityFocus = destination

        case let .rendererReported(attemptID, capability, keyboardOwned, accessibilityOwned):
            guard attemptID == state.attemptID, capability != state.capability else {
                return .init(state: current, effects: .none)
            }
            let wasRetrying = state.retryInFlight
            state.capability = capability
            switch capability {
            case .probing:
                break
            case .available:
                state.retryInFlight = false
                if wasRetrying {
                    effects.announcement = .restored
                    effects.keyboardFocus = .surface
                    effects.accessibilityFocus = .surface
                }
            case .unavailable:
                state.presentation = .table
                state.retryInFlight = false
                effects.announcement = wasRetrying ? .stillUnavailable : .unavailable
                if wasRetrying {
                    effects.keyboardFocus = .retryButton
                    effects.accessibilityFocus = .retryButton
                } else {
                    effects.keyboardFocus = keyboardOwned ? .table : nil
                    effects.accessibilityFocus = accessibilityOwned ? .table : nil
                }
            }

        case .retryRequested:
            guard state.capability.isUnavailable, !state.retryInFlight else {
                return .init(state: current, effects: .none)
            }
            state.retryInFlight = true
            state.attemptID &+= 1
            effects.beginRetryAttempt = state.attemptID

        case .retryCancelled:
            guard state.retryInFlight else {
                return .init(state: current, effects: .none)
            }
            state.attemptID &+= 1
            state.retryInFlight = false
            effects.discardPreparedRenderer = true

        case let .retryCompleted(attemptID, succeeded):
            guard attemptID == state.attemptID, state.retryInFlight else {
                return .init(state: current, effects: .none)
            }
            if succeeded {
                state.capability = .probing
                state.presentation = .mri
                effects.acceptPreparedRenderer = true
            } else {
                state.retryInFlight = false
                state.capability = .unavailable(.rendererInitialization)
                state.presentation = .table
                effects.announcement = .stillUnavailable
                effects.keyboardFocus = .retryButton
                effects.accessibilityFocus = .retryButton
            }
        }

        return .init(state: state, effects: effects)
    }
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
