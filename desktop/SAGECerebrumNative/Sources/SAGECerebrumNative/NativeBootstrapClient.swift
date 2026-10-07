import CryptoKit
import Foundation

// Transport admission only. This credential never authenticates the vault or
// confers an operator role, and it is never persisted in a shared cookie store.
struct NativeSessionCredential: Sendable {
    let token: String
    let expiresAt: ContinuousClock.Instant
    let connection: ShellControlConnection
}

enum NativeBootstrapError: LocalizedError, Equatable, Sendable {
    case invalidBinding
    case invalidResponse
    case refused(status: Int, message: String)

    var errorDescription: String? {
        switch self {
        case .invalidBinding: "The native session does not match the validated SAGE daemon."
        case .invalidResponse: "SAGE returned an invalid native session response."
        case let .refused(_, message): "SAGE refused native session admission: \(message)"
        }
    }
}

struct NativeBootstrapPeerResponder: Sendable {
    let reply: @Sendable (Data) throws -> Data
}

enum NativeBootstrapClient {
    private static let maximumResponseBytes = 16 * 1024

    static func bootstrap(
        connection: ShellControlConnection,
        sageHome: URL = ShellControlClient.defaultSAGEHome()
    ) async throws -> NativeSessionCredential {
        try await bootstrap(connection: connection, exchange: { request, respond in
            try await ShellControlClient.exchange(request: request, sageHome: sageHome, challengeResponse: respond.reply)
        }, redeem: { origin, payload in
            try await redeem(origin: origin, payload: payload)
        })
    }

    // Internal dependency seam exercises the actual key generation, closed
    // contracts and binding checks. Production always uses the transports above.
    static func bootstrap(
        connection: ShellControlConnection,
        exchange: @Sendable (Data, NativeBootstrapPeerResponder) async throws -> Data,
        redeem: @Sendable (URL, Data) async throws -> Data
    ) async throws -> NativeSessionCredential {
        try Task.checkCancellation()
        let binding = try NativeBootstrapBinding(connection: connection)
        let privateKey = Curve25519.Signing.PrivateKey()
        let publicKey = encodeBase64URL(privateKey.publicKey.rawRepresentation)
        let issue = NativeBootstrapIssue(binding: binding, publicKey: publicKey)
        let responder = NativeBootstrapPeerResponder { challenge in
            try makePeerProof(challenge, binding: binding, publicKey: publicKey, privateKey: privateKey)
        }
        let response = try await exchange(JSONEncoder().encode(issue), responder)
        try Task.checkCancellation()
        let challenge = try validateIssueResponse(response, binding: binding, publicKey: publicKey)
        let redemption = try makeRedemption(challenge: challenge, privateKey: privateKey)
        let beganRedemption = ContinuousClock.now
        let redeemed = try await redeem(connection.origin, JSONEncoder().encode(redemption))
        try Task.checkCancellation()
        let token = try validateRedemptionResponse(redeemed)
        return NativeSessionCredential(token: token, expiresAt: beganRedemption.advanced(by: .seconds(900)),
                                       connection: connection)
    }

    static func makePeerProof(
        _ data: Data, binding: NativeBootstrapBinding, publicKey: String,
        privateKey: Curve25519.Signing.PrivateKey
    ) throws -> Data {
        guard data.count <= maximumResponseBytes,
              encodeBase64URL(privateKey.publicKey.rawRepresentation) == publicKey else {
            throw NativeBootstrapError.invalidBinding
        }
        let response: NativeBootstrapPeerChallenge
        do { response = try JSONDecoder().decode(NativeBootstrapPeerChallenge.self, from: data) }
        catch { throw NativeBootstrapError.invalidResponse }
        guard response.controlProtocol == 2, response.operation == "native-session.prove",
              decodeBase64URL(response.challenge, byteCount: 32) != nil else {
            throw NativeBootstrapError.invalidResponse
        }
        let transcript = Data(["SAGE-NATIVE-PEER/1", response.challenge, binding.instanceGeneration,
                               binding.uiOrigin, binding.startupProof, publicKey].joined(separator: "\n").utf8)
        let signature = try privateKey.signature(for: transcript)
        return try JSONEncoder().encode(NativeBootstrapPeerProof(signature: encodeBase64URL(signature)))
    }

    static func validateIssueResponse(
        _ data: Data, binding: NativeBootstrapBinding, publicKey: String
    ) throws -> NativeBootstrapChallenge {
        guard data.count <= maximumResponseBytes else { throw NativeBootstrapError.invalidResponse }
        let response: NativeBootstrapChallenge
        do { response = try JSONDecoder().decode(NativeBootstrapChallenge.self, from: data) }
        catch { throw NativeBootstrapError.invalidResponse }
        guard response.controlProtocol == 2, response.operation == "native-session.issue",
              response.instanceGeneration == binding.instanceGeneration,
              response.uiOrigin == binding.uiOrigin, response.startupProof == binding.startupProof,
              response.publicKey == publicKey, response.expiresInSeconds == 30,
              decodeBase64URL(response.ticket, byteCount: 32) != nil,
              decodeBase64URL(response.challenge, byteCount: 32) != nil,
              decodeBase64URL(response.publicKey, byteCount: 32) != nil
        else { throw NativeBootstrapError.invalidBinding }
        return response
    }

    static func validateRedemptionResponse(_ data: Data) throws -> String {
        guard data.count <= maximumResponseBytes else { throw NativeBootstrapError.invalidResponse }
        let response: NativeBootstrapRedemptionResponse
        do { response = try JSONDecoder().decode(NativeBootstrapRedemptionResponse.self, from: data) }
        catch { throw NativeBootstrapError.invalidResponse }
        guard response.expiresInSeconds == 900,
              decodeBase64URL(response.nativeSession, byteCount: 32) != nil
        else { throw NativeBootstrapError.invalidResponse }
        return response.nativeSession
    }

    static func makeRedemption(
        challenge: NativeBootstrapChallenge, privateKey: Curve25519.Signing.PrivateKey
    ) throws -> NativeBootstrapRedemption {
        guard encodeBase64URL(privateKey.publicKey.rawRepresentation) == challenge.publicKey else {
            throw NativeBootstrapError.invalidBinding
        }
        let signature = try privateKey.signature(for: signingPayload(challenge))
        return NativeBootstrapRedemption(challenge: challenge, signature: encodeBase64URL(signature))
    }

    static func signingPayload(_ challenge: NativeBootstrapChallenge) -> Data {
        Data(["SAGE-NATIVE-BOOTSTRAP/1", challenge.ticket, challenge.challenge,
              challenge.instanceGeneration, challenge.uiOrigin, challenge.startupProof,
              challenge.publicKey].joined(separator: "\n").utf8)
    }

    static func encodeBase64URL(_ data: Data) -> String {
        data.base64EncodedString().replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "")
    }

    static func decodeBase64URL(_ value: String, byteCount: Int) -> Data? {
        let allowed = Set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8)
        guard value.utf8.count == (byteCount * 8 + 5) / 6,
              value.utf8.allSatisfy(allowed.contains) else { return nil }
        var base64 = value.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        base64 += String(repeating: "=", count: (4 - base64.count % 4) % 4)
        guard let data = Data(base64Encoded: base64), data.count == byteCount,
              encodeBase64URL(data) == value else { return nil }
        return data
    }

    private static func redeem(origin: URL, payload: Data) async throws -> Data {
        guard SAGEAPIClient.isSafeLoopback(origin) else { throw NativeBootstrapError.invalidBinding }
        try Task.checkCancellation()
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = false
        configuration.httpCookieStorage = nil
        configuration.urlCredentialStorage = nil
        configuration.urlCache = nil
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.timeoutIntervalForRequest = 5
        configuration.timeoutIntervalForResource = 5
        let session = URLSession(configuration: configuration, delegate: NativeBootstrapRedirectDelegate(), delegateQueue: nil)
        defer { session.invalidateAndCancel() }
        var request = URLRequest(url: origin.appending(path: "/v1/dashboard/native/redeem"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.httpBody = payload
        let (bytes, response) = try await session.bytes(for: request)
        guard let response = response as? HTTPURLResponse else { throw NativeBootstrapError.invalidResponse }
        var data = Data()
        for try await byte in bytes {
            try Task.checkCancellation()
            guard data.count < maximumResponseBytes else { throw NativeBootstrapError.invalidResponse }
            data.append(byte)
        }
        guard response.statusCode == 200 else {
            let detail = (try? JSONDecoder().decode(NativeBootstrapRefusal.self, from: data))?.error
            throw NativeBootstrapError.refused(status: response.statusCode,
                message: detail.map { String($0.prefix(512)) } ?? HTTPURLResponse.localizedString(forStatusCode: response.statusCode))
        }
        return data
    }
}

struct NativeBootstrapBinding: Sendable {
    let instanceGeneration: String
    let uiOrigin: String
    let startupProof: String

    init(connection: ShellControlConnection) throws {
        try ShellControlClient.validate(connection.status)
        guard connection.status.canServeNativeUI, SAGEAPIClient.isSafeLoopback(connection.origin),
              let origin = connection.status.uiOrigin, origin == connection.origin.absoluteString,
              origin.utf8.allSatisfy({ $0 < 128 && $0 > 32 })
        else { throw NativeBootstrapError.invalidBinding }
        instanceGeneration = connection.status.instanceGeneration
        uiOrigin = origin
        startupProof = connection.status.startupProof ?? ""
    }
}

private struct NativeBootstrapPeerChallenge: Decodable {
    let controlProtocol: Int
    let operation: String
    let challenge: String
    enum CodingKeys: String, CodingKey, CaseIterable {
        case controlProtocol = "control_protocol", operation, challenge
    }
    init(from decoder: Decoder) throws {
        try requireNativeBootstrapFields(decoder, keys: CodingKeys.allCases.map(\.rawValue))
        let fields = try decoder.container(keyedBy: CodingKeys.self)
        controlProtocol = try fields.decode(Int.self, forKey: .controlProtocol)
        operation = try fields.decode(String.self, forKey: .operation)
        challenge = try fields.decode(String.self, forKey: .challenge)
    }
}

private struct NativeBootstrapPeerProof: Encodable {
    let controlProtocol = 2
    let operation = "native-session.prove"
    let signature: String
    enum CodingKeys: String, CodingKey {
        case controlProtocol = "control_protocol", operation, signature
    }
}

private struct NativeBootstrapIssue: Encodable {
    let controlProtocol = 2
    let shellProtocol = 1
    let operation = "native-session.issue"
    let instanceGeneration: String
    let uiOrigin: String
    let startupProof: String
    let publicKey: String
    init(binding: NativeBootstrapBinding, publicKey: String) {
        instanceGeneration = binding.instanceGeneration; uiOrigin = binding.uiOrigin
        startupProof = binding.startupProof; self.publicKey = publicKey
    }
    enum CodingKeys: String, CodingKey {
        case controlProtocol = "control_protocol", shellProtocol = "shell_protocol", operation
        case instanceGeneration = "instance_generation", uiOrigin = "ui_origin"
        case startupProof = "startup_proof", publicKey = "public_key"
    }
}

struct NativeBootstrapChallenge: Decodable, Sendable {
    let controlProtocol: Int
    let operation: String
    let instanceGeneration: String
    let uiOrigin: String
    let startupProof: String
    let publicKey: String
    let ticket: String
    let challenge: String
    let expiresInSeconds: Int
    enum CodingKeys: String, CodingKey, CaseIterable {
        case controlProtocol = "control_protocol", operation
        case instanceGeneration = "instance_generation", uiOrigin = "ui_origin"
        case startupProof = "startup_proof", publicKey = "public_key", ticket, challenge
        case expiresInSeconds = "expires_in_seconds"
    }
    init(from decoder: Decoder) throws {
        try requireNativeBootstrapFields(decoder, keys: CodingKeys.allCases.map(\.rawValue))
        let fields = try decoder.container(keyedBy: CodingKeys.self)
        controlProtocol = try fields.decode(Int.self, forKey: .controlProtocol)
        operation = try fields.decode(String.self, forKey: .operation)
        instanceGeneration = try fields.decode(String.self, forKey: .instanceGeneration)
        uiOrigin = try fields.decode(String.self, forKey: .uiOrigin)
        startupProof = try fields.decode(String.self, forKey: .startupProof)
        publicKey = try fields.decode(String.self, forKey: .publicKey)
        ticket = try fields.decode(String.self, forKey: .ticket)
        challenge = try fields.decode(String.self, forKey: .challenge)
        expiresInSeconds = try fields.decode(Int.self, forKey: .expiresInSeconds)
    }
}

struct NativeBootstrapRedemption: Encodable, Sendable {
    let ticket: String
    let challenge: String
    let instanceGeneration: String
    let uiOrigin: String
    let startupProof: String
    let publicKey: String
    let signature: String
    init(challenge: NativeBootstrapChallenge, signature: String) {
        ticket = challenge.ticket; self.challenge = challenge.challenge
        instanceGeneration = challenge.instanceGeneration; uiOrigin = challenge.uiOrigin
        startupProof = challenge.startupProof; publicKey = challenge.publicKey; self.signature = signature
    }
    enum CodingKeys: String, CodingKey {
        case ticket, challenge, instanceGeneration = "instance_generation", uiOrigin = "ui_origin"
        case startupProof = "startup_proof", publicKey = "public_key", signature
    }
}

private struct NativeBootstrapRedemptionResponse: Decodable {
    let nativeSession: String
    let expiresInSeconds: Int
    enum CodingKeys: String, CodingKey, CaseIterable {
        case nativeSession = "native_session", expiresInSeconds = "expires_in_seconds"
    }
    init(from decoder: Decoder) throws {
        try requireNativeBootstrapFields(decoder, keys: CodingKeys.allCases.map(\.rawValue))
        let fields = try decoder.container(keyedBy: CodingKeys.self)
        nativeSession = try fields.decode(String.self, forKey: .nativeSession)
        expiresInSeconds = try fields.decode(Int.self, forKey: .expiresInSeconds)
    }
}

private func requireNativeBootstrapFields(_ decoder: Decoder, keys: [String]) throws {
    struct Field: CodingKey {
        let stringValue: String
        let intValue: Int? = nil
        init?(stringValue: String) { self.stringValue = stringValue }
        init?(intValue: Int) { return nil }
    }
    let fields = try decoder.container(keyedBy: Field.self)
    guard Set(fields.allKeys.map(\.stringValue)) == Set(keys) else { throw NativeBootstrapError.invalidResponse }
}

private struct NativeBootstrapRefusal: Decodable { let error: String }

private final class NativeBootstrapRedirectDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(_ session: URLSession, task: URLSessionTask,
                    willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest,
                    completionHandler: @escaping (URLRequest?) -> Void) {
        completionHandler(nil)
    }
}
