import Foundation

struct DashboardEvent: Equatable, Sendable {
    let name: String
    let data: String
    let receivedAt: Date
}

enum CerebrumEventStreamState: CaseIterable, Equatable, Sendable {
    case connecting
    case connected
    case reconnecting
    case stopped
}

enum DashboardEventStreamElement: Equatable, Sendable {
    case state(CerebrumEventStreamState)
    case event(DashboardEvent)
}

enum DashboardEventError: LocalizedError, Sendable {
    case invalidResponse
    case server(status: Int)
    case lineTooLarge

    var errorDescription: String? {
        switch self {
        case .invalidResponse: "SAGE returned an invalid event stream."
        case let .server(status): "SAGE event stream returned HTTP \(status)."
        case .lineTooLarge: "SAGE returned an oversized event-stream line."
        }
    }
}

// AsyncBytes.lines omits empty lines. SSE needs every blank line to dispatch
// a frame, so delimit the bytes directly (LF, CRLF and CR are all legal SSE).
struct SSELineAccumulator: Sendable {
    private var bytes: [UInt8] = []
    private var afterCarriageReturn = false
    private var firstLine = true
    private static let maximumLineBytes = 1_048_576

    mutating func consume(_ byte: UInt8) throws -> String? {
        if afterCarriageReturn {
            afterCarriageReturn = false
            if byte == 10 { return nil }
        }
        if byte == 10 || byte == 13 {
            var line = String(decoding: bytes, as: UTF8.self)
            if firstLine, line.hasPrefix("\u{FEFF}") { line.removeFirst() }
            firstLine = false
            bytes.removeAll(keepingCapacity: true)
            afterCarriageReturn = byte == 13
            return line
        }
        guard bytes.count < Self.maximumLineBytes else { throw DashboardEventError.lineTooLarge }
        bytes.append(byte)
        return nil
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
