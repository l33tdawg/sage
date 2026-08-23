import Foundation

struct BrainEdgeLODPolicy: Equatable, Sendable {
    var maximumEdges = 2_048
    var incomingPerVisibleNode = 1
    var outgoingPerVisibleNode = 1
    var selectedIncoming = 24
    var selectedOutgoing = 24
}

enum BrainEdgeLOD {
    static func select(
        _ edges: [BrainEdge],
        visibleNodeIDs: Set<String>,
        highlighted: BrainEdge?,
        selectedAgentSceneID: String?,
        policy: BrainEdgeLODPolicy = .init()
    ) -> [BrainEdge] {
        guard policy.maximumEdges > 0, !visibleNodeIDs.isEmpty else { return [] }

        var canonical: [Identity: BrainEdge] = [:]
        for edge in edges where
            !edge.source.isEmpty && !edge.target.isEmpty && !edge.type.isEmpty &&
            visibleNodeIDs.contains(edge.source) && visibleNodeIDs.contains(edge.target) {
            let identity = Identity(edge)
            guard let existing = canonical[identity] else {
                canonical[identity] = normalized(edge)
                continue
            }
            canonical[identity] = BrainEdge(
                source: edge.source,
                target: edge.target,
                type: edge.type,
                weight: max(rankedWeight(existing), rankedWeight(edge)),
                lastFired: newest(existing.lastFired, edge.lastFired)
            )
        }

        let ranked = canonical.values.sorted(by: ranksBefore)
        guard !ranked.isEmpty else { return [] }

        var incoming: [String: [BrainEdge]] = [:]
        var outgoing: [String: [BrainEdge]] = [:]
        for edge in ranked {
            incoming[edge.target, default: []].append(edge)
            outgoing[edge.source, default: []].append(edge)
        }

        var result: [BrainEdge] = []
        var included = Set<Identity>()
        func append(_ edge: BrainEdge) {
            guard result.count < policy.maximumEdges, included.insert(Identity(edge)).inserted else { return }
            result.append(edge)
        }
        func appendTier(_ candidates: [BrainEdge]) {
            for edge in candidates.sorted(by: ranksBefore) { append(edge) }
        }

        if let highlighted, let canonicalHighlight = canonical[Identity(highlighted)] {
            append(canonicalHighlight)
        }

        if let selectedAgentSceneID, visibleNodeIDs.contains(selectedAgentSceneID) {
            appendTier(Array((incoming[selectedAgentSceneID] ?? []).prefix(max(0, policy.selectedIncoming))) +
                Array((outgoing[selectedAgentSceneID] ?? []).prefix(max(0, policy.selectedOutgoing))))
        }

        var topology: [Identity: BrainEdge] = [:]
        for nodeID in visibleNodeIDs.sorted() {
            for edge in (incoming[nodeID] ?? []).prefix(max(0, policy.incomingPerVisibleNode)) {
                topology[Identity(edge)] = edge
            }
            for edge in (outgoing[nodeID] ?? []).prefix(max(0, policy.outgoingPerVisibleNode)) {
                topology[Identity(edge)] = edge
            }
        }
        appendTier(Array(topology.values))
        for edge in ranked { append(edge) }
        return result
    }

    private struct Identity: Hashable {
        let source: String
        let target: String
        let type: String

        init(_ edge: BrainEdge) {
            source = edge.source
            target = edge.target
            type = edge.type
        }
    }

    private static func normalized(_ edge: BrainEdge) -> BrainEdge {
        BrainEdge(
            source: edge.source,
            target: edge.target,
            type: edge.type,
            weight: rankedWeight(edge),
            lastFired: edge.lastFired
        )
    }

    private static func newest(_ lhs: Date?, _ rhs: Date?) -> Date? {
        switch (lhs, rhs) {
        case let (lhs?, rhs?): max(lhs, rhs)
        case let (lhs?, nil): lhs
        case let (nil, rhs?): rhs
        case (nil, nil): nil
        }
    }

    private static func rankedWeight(_ edge: BrainEdge) -> Double {
        guard let weight = edge.weight, weight.isFinite, weight > 0 else { return 0 }
        return weight
    }

    private static func ranksBefore(_ lhs: BrainEdge, _ rhs: BrainEdge) -> Bool {
        if rankedWeight(lhs) != rankedWeight(rhs) { return rankedWeight(lhs) > rankedWeight(rhs) }
        if (lhs.lastFired ?? .distantPast) != (rhs.lastFired ?? .distantPast) {
            return (lhs.lastFired ?? .distantPast) > (rhs.lastFired ?? .distantPast)
        }
        if lhs.source != rhs.source { return lhs.source < rhs.source }
        if lhs.target != rhs.target { return lhs.target < rhs.target }
        return lhs.type < rhs.type
    }
}
