import Foundation

enum MemorySort: String, CaseIterable, Identifiable, Sendable {
    case newest
    case oldest
    case confidence

    var id: String { rawValue }
    var title: String {
        switch self {
        case .newest: "Newest first"
        case .oldest: "Oldest first"
        case .confidence: "Highest confidence"
        }
    }
}

enum MemoryDatePreset: String, CaseIterable, Identifiable, Sendable {
    case anytime
    case hour
    case day
    case week
    case month
    case custom

    var id: String { rawValue }
    var title: String {
        switch self {
        case .anytime: "Any time"
        case .hour: "Past hour"
        case .day: "Past 24 hours"
        case .week: "Past 7 days"
        case .month: "Past 30 days"
        case .custom: "Custom range"
        }
    }

    func lowerBound(now: Date = .now) -> Date? {
        let seconds: TimeInterval? = switch self {
        case .anytime: nil
        case .hour: 3_600
        case .day: 86_400
        case .week: 604_800
        case .month: 2_592_000
        case .custom: nil
        }
        return seconds.map { now.addingTimeInterval(-$0) }
    }
}

struct MemoryListQuery: Equatable, Sendable {
    var text = ""
    var domain = ""
    var status = "active"
    var tag = ""
    var agent = ""
    var from: Date?
    var to: Date?
    var sort: MemorySort = .newest
    var limit = 100
    var cursor: String?

    var queryItems: [URLQueryItem] {
        var items = [
            URLQueryItem(name: "limit", value: String(text.isEmpty ? limit : 200)),
            URLQueryItem(name: "sort", value: sort.rawValue),
        ]
        func append(_ name: String, _ value: String) {
            guard !value.isEmpty else { return }
            items.append(URLQueryItem(name: name, value: value))
        }
        append("q", text.trimmingCharacters(in: .whitespacesAndNewlines))
        append("domain", domain)
        append("status", status)
        append("tag", tag)
        append("agent", agent)
        if let from {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            append("from", formatter.string(from: from))
        }
        if let to {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withInternetDateTime]
            append("to", formatter.string(from: to))
        }
        if let cursor { append("cursor", cursor) }
        return items
    }

}

struct MemoryListEnvelope: Decodable, Equatable, Sendable {
    let memories: [MemorySummary]
    let total: Int
    let limit: Int
    let offset: Int
    let nextCursor: String?
    let continuationRequired: Bool?
    let authorLabels: [String: String]?
    let projection: MemoryProjection?

    enum CodingKeys: String, CodingKey {
        case memories, total, limit, offset, projection
        case nextCursor = "next_cursor"
        case continuationRequired = "continuation_required"
        case authorLabels = "author_labels"
    }
}

struct MemoryProjection: Decodable, Equatable, Sendable {
    let complete: Bool?
    let partial: Bool?
    let verifiedOnly: Bool?
    let state: String?
    let hiddenCount: Int?
    let message: String?

    enum CodingKeys: String, CodingKey {
        case complete, partial, state, message
        case verifiedOnly = "verified_only"
        case hiddenCount = "hidden_count"
    }
}

struct MemorySummary: Decodable, Equatable, Identifiable, Hashable, Sendable {
    let memoryID: String
    let submittingAgent: String
    let content: String
    let contentHash: String?
    let memoryType: String
    let domainTag: String
    let provider: String?
    let confidenceScore: Double
    let status: String
    let parentHash: String?
    let taskStatus: String?
    let createdAt: Date
    let committedAt: Date?
    let deprecatedAt: Date?
    let corroborationCount: Int?

    var id: String { memoryID }

    enum CodingKeys: String, CodingKey {
        case content, provider, status
        case memoryID = "memory_id"
        case submittingAgent = "submitting_agent"
        case contentHash = "content_hash"
        case memoryType = "memory_type"
        case domainTag = "domain_tag"
        case confidenceScore = "confidence_score"
        case parentHash = "parent_hash"
        case taskStatus = "task_status"
        case createdAt = "created_at"
        case committedAt = "committed_at"
        case deprecatedAt = "deprecated_at"
        case corroborationCount = "corroboration_count"
    }
}

struct TagEnvelope: Decodable, Equatable, Sendable {
    let tags: [TagCount]
    let partial: Bool?
}

struct TagCount: Decodable, Equatable, Identifiable, Sendable {
    let tag: String
    let count: Int
    var id: String { tag }
}

struct MemoryTagsEnvelope: Decodable, Equatable, Sendable {
    let memoryID: String
    let tags: [String]

    enum CodingKeys: String, CodingKey {
        case memoryID = "memory_id"
        case tags
    }
}

struct MemoryMutationResponse: Decodable, Equatable, Sendable {
    let status: String
    let message: String?
}

struct BulkMemoryUpdateResponse: Decodable, Equatable, Sendable {
    let status: String
    let updated: Int
    let total: Int
}
