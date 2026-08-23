import Foundation

struct DashboardEvent: Equatable, Sendable {
    let name: String
    let data: String
    let receivedAt: Date
}

enum DashboardEventError: LocalizedError, Sendable {
    case invalidResponse
    case server(status: Int)

    var errorDescription: String? {
        switch self {
        case .invalidResponse: "SAGE returned an invalid event stream."
        case let .server(status): "SAGE event stream returned HTTP \(status)."
        }
    }
}

struct SSEEventAccumulator: Sendable {
    private(set) var eventName = "message"
    private(set) var dataLines: [String] = []

    mutating func consume(_ line: String, receivedAt: Date = .now) -> DashboardEvent? {
        if line.isEmpty {
            defer {
                eventName = "message"
                dataLines.removeAll(keepingCapacity: true)
            }
            guard !dataLines.isEmpty else { return nil }
            return DashboardEvent(name: eventName, data: dataLines.joined(separator: "\n"), receivedAt: receivedAt)
        }
        if line.hasPrefix(":") { return nil }
        if line.hasPrefix("event:") {
            eventName = String(line.dropFirst(6)).trimmingCharacters(in: .whitespaces)
        } else if line.hasPrefix("data:") {
            dataLines.append(String(line.dropFirst(5)).trimmingCharacters(in: .whitespaces))
        }
        return nil
    }
}
