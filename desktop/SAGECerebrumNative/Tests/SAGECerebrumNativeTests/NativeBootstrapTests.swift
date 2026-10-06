import CryptoKit
import Foundation
import Testing
@testable import SAGECerebrumNative

@Suite struct NativeBootstrapQualification {
    @Test func peerProofRequiresFreshChallengeAndBindsTheConnection() throws {
        let key = Curve25519.Signing.PrivateKey()
        let publicKey = NativeBootstrapClient.encodeBase64URL(key.publicKey.rawRepresentation)
        let binding = try NativeBootstrapBinding(connection: nativeBootstrapConnection())
        let challenge = nativeToken(7)
        let data = try JSONSerialization.data(withJSONObject: ["control_protocol": 2, "operation": "native-session.prove", "challenge": challenge])
        let proof = try NativeBootstrapClient.makePeerProof(data, binding: binding, publicKey: publicKey, privateKey: key)
        let fields = try #require(JSONSerialization.jsonObject(with: proof) as? [String: Any])
        #expect(Set(fields.keys) == Set(["control_protocol", "operation", "signature"]))
        let signatureText = try #require(fields["signature"] as? String)
        let signature = try #require(NativeBootstrapClient.decodeBase64URL(signatureText, byteCount: 64))
        let parts = ["SAGE-NATIVE-PEER/1", challenge, binding.instanceGeneration, binding.uiOrigin, binding.startupProof, publicKey]
        #expect(key.publicKey.isValidSignature(signature, for: Data(parts.joined(separator: "\n").utf8)))
        for index in parts.indices {
            var changed = parts
            changed[index] += "x"
            #expect(!key.publicKey.isValidSignature(signature, for: Data(changed.joined(separator: "\n").utf8)))
        }
        var fresh = parts
        fresh[1] = nativeToken(8)
        #expect(!key.publicKey.isValidSignature(signature, for: Data(fresh.joined(separator: "\n").utf8)))
    }

    @Test func peerChallengeRejectsUnexpectedOrIncompleteContracts() throws {
        let key = Curve25519.Signing.PrivateKey()
        let publicKey = NativeBootstrapClient.encodeBase64URL(key.publicKey.rawRepresentation)
        let binding = try NativeBootstrapBinding(connection: nativeBootstrapConnection())
        for fields: [String: Any] in [
            ["control_protocol": 2, "operation": "native-session.prove"],
            ["control_protocol": 1, "operation": "native-session.prove", "challenge": nativeToken(7)],
            ["control_protocol": 2, "operation": "native-session.issue", "challenge": nativeToken(7)],
            ["control_protocol": 2, "operation": "native-session.prove", "challenge": nativeToken(7), "ticket": nativeToken(8)],
            ["control_protocol": 2, "operation": "native-session.prove", "challenge": nativeToken(7) + "="],
        ] {
            #expect(throws: NativeBootstrapError.self) {
                try NativeBootstrapClient.makePeerProof(JSONSerialization.data(withJSONObject: fields), binding: binding, publicKey: publicKey, privateKey: key)
            }
        }
    }

    @Test func signatureBindsEveryFieldAndUsesExactDomainSeparatedBytes() throws {
        let key = try Curve25519.Signing.PrivateKey(rawRepresentation: Data(0..<32))
        let connection = nativeBootstrapConnection(proof: String(repeating: "a", count: 64))
        let binding = try NativeBootstrapBinding(connection: connection)
        let publicKey = NativeBootstrapClient.encodeBase64URL(key.publicKey.rawRepresentation)
        let data = try nativeChallengeData(connection: connection, publicKey: publicKey)
        let challenge = try NativeBootstrapClient.validateIssueResponse(data, binding: binding, publicKey: publicKey)
        let redemption = try NativeBootstrapClient.makeRedemption(challenge: challenge, privateKey: key)
        let payload = NativeBootstrapClient.signingPayload(challenge)
        let expected = ["SAGE-NATIVE-BOOTSTRAP/1", nativeToken(1), nativeToken(2),
                        connection.status.instanceGeneration, connection.origin.absoluteString,
                        String(repeating: "a", count: 64), publicKey].joined(separator: "\n")
        #expect(payload == Data(expected.utf8))
        #expect(payload.last != 10)
        let signature = try #require(NativeBootstrapClient.decodeBase64URL(redemption.signature, byteCount: 64))
        #expect(key.publicKey.isValidSignature(signature, for: payload))
        for index in 0..<7 {
            var changed = expected.components(separatedBy: "\n")
            changed[index] += "x"
            #expect(!key.publicKey.isValidSignature(signature, for: Data(changed.joined(separator: "\n").utf8)))
        }
        #expect(throws: NativeBootstrapError.self) {
            try NativeBootstrapClient.makeRedemption(challenge: challenge, privateKey: Curve25519.Signing.PrivateKey())
        }
    }

    @Test(arguments: ["control_protocol", "operation", "instance_generation", "ui_origin", "startup_proof",
                      "public_key", "ticket", "challenge", "expires_in_seconds"])
    func challengeRejectsChangedOrMissingFields(_ field: String) throws {
        let connection = nativeBootstrapConnection()
        let binding = try NativeBootstrapBinding(connection: connection)
        let publicKey = nativeToken(3)
        let data = try nativeChallengeData(connection: connection, publicKey: publicKey)
        var fields = try #require(JSONSerialization.jsonObject(with: data) as? [String: Any])
        let original = fields.removeValue(forKey: field)
        #expect(throws: NativeBootstrapError.self) {
            try NativeBootstrapClient.validateIssueResponse(JSONSerialization.data(withJSONObject: fields), binding: binding, publicKey: publicKey)
        }
        fields[field] = NSNull()
        #expect(throws: NativeBootstrapError.self) {
            try NativeBootstrapClient.validateIssueResponse(JSONSerialization.data(withJSONObject: fields), binding: binding, publicKey: publicKey)
        }
        fields[field] = original is Int ? 3 : "different"
        #expect(throws: NativeBootstrapError.self) {
            try NativeBootstrapClient.validateIssueResponse(JSONSerialization.data(withJSONObject: fields), binding: binding, publicKey: publicKey)
        }
    }

    @Test func challengeRejectsUnknownFieldsAndOversizeFrames() throws {
        let connection = nativeBootstrapConnection()
        let binding = try NativeBootstrapBinding(connection: connection)
        let publicKey = nativeToken(3)
        var fields = try #require(JSONSerialization.jsonObject(with: nativeChallengeData(connection: connection, publicKey: publicKey)) as? [String: Any])
        fields["root"] = true
        #expect(throws: NativeBootstrapError.self) {
            try NativeBootstrapClient.validateIssueResponse(JSONSerialization.data(withJSONObject: fields), binding: binding, publicKey: publicKey)
        }
        #expect(throws: NativeBootstrapError.self) {
            try NativeBootstrapClient.validateIssueResponse(Data(repeating: 32, count: 16_385), binding: binding, publicKey: publicKey)
        }
    }

    @Test(arguments: ["", String(repeating: "A", count: 42), String(repeating: "A", count: 44),
                      String(repeating: "A", count: 43) + "=", String(repeating: "A", count: 42) + "B",
                      String(repeating: "A", count: 42) + "/", String(repeating: "A", count: 42) + "\n"])
    func base64RejectsNoncanonicalRepresentations(_ value: String) {
        #expect(NativeBootstrapClient.decodeBase64URL(value, byteCount: 32) == nil)
    }

    @Test func redemptionResponseIsClosedAndDoesNotAcceptAuthenticationClaims() throws {
        let token = nativeToken(9)
        #expect(try NativeBootstrapClient.validateRedemptionResponse(nativeCredentialData(token: token)) == token)
        for fields: [String: Any] in [
            ["native_session": token],
            ["native_session": token, "expires_in_seconds": 901],
            ["native_session": token, "expires_in_seconds": 0],
            ["native_session": token, "expires_in_seconds": 900, "authenticated": true],
            ["native_session": token, "expires_in_seconds": 900, "root": true],
            ["native_session": NSNull(), "expires_in_seconds": 900],
            ["native_session": nativeToken(9) + "=", "expires_in_seconds": 900],
        ] {
            #expect(throws: NativeBootstrapError.self) {
                try NativeBootstrapClient.validateRedemptionResponse(JSONSerialization.data(withJSONObject: fields))
            }
        }
    }

    @Test func bindingUsesExactValidatedOriginAndRequiredEmptyStartupProof() throws {
        let connection = nativeBootstrapConnection()
        let binding = try NativeBootstrapBinding(connection: connection)
        #expect(binding.startupProof == "")
        #expect(binding.uiOrigin == connection.status.uiOrigin)
        let changed = ShellControlConnection(status: connection.status, origin: URL(string: "http://127.0.0.1:49153")!)
        #expect(throws: NativeBootstrapError.self) { try NativeBootstrapBinding(connection: changed) }
    }

    @Test func bootstrapCreatesFreshKeysAndOnlyReturnsBoundTransportAdmission() async throws {
        let connection = nativeBootstrapConnection()
        let calls = NativeBootstrapCalls()
        for _ in 0..<2 {
            let start = ContinuousClock.now
            let credential = try await NativeBootstrapClient.bootstrap(connection: connection, exchange: { data, _ in
                await calls.issue(data)
                let fields = try #require(JSONSerialization.jsonObject(with: data) as? [String: Any])
                #expect(Set(fields.keys) == Set(["control_protocol", "shell_protocol", "operation", "instance_generation", "ui_origin", "startup_proof", "public_key"]))
                #expect(fields["control_protocol"] as? Int == 2)
                #expect(fields["shell_protocol"] as? Int == 1)
                #expect(fields["operation"] as? String == "native-session.issue")
                #expect(fields["startup_proof"] as? String == "")
                return try nativeChallengeData(connection: connection, publicKey: #require(fields["public_key"] as? String))
            }, redeem: { origin, data in
                await calls.redeem()
                #expect(origin == connection.origin)
                let fields = try #require(JSONSerialization.jsonObject(with: data) as? [String: String])
                #expect(Set(fields.keys) == Set(["ticket", "challenge", "instance_generation", "ui_origin", "startup_proof", "public_key", "signature"]))
                let publicKeyText = try #require(fields["public_key"])
                let signatureText = try #require(fields["signature"])
                let keyBytes = try #require(NativeBootstrapClient.decodeBase64URL(publicKeyText, byteCount: 32))
                let key = try Curve25519.Signing.PublicKey(rawRepresentation: keyBytes)
                let signature = try #require(NativeBootstrapClient.decodeBase64URL(signatureText, byteCount: 64))
                let signed = ["SAGE-NATIVE-BOOTSTRAP/1", fields["ticket"]!, fields["challenge"]!, fields["instance_generation"]!, fields["ui_origin"]!, fields["startup_proof"]!, fields["public_key"]!].joined(separator: "\n")
                #expect(key.isValidSignature(signature, for: Data(signed.utf8)))
                return try nativeCredentialData(token: nativeToken(9))
            })
            #expect(credential.connection == connection)
            #expect(credential.token == nativeToken(9))
            #expect(credential.expiresAt >= start.advanced(by: .seconds(900)))
            #expect(credential.expiresAt <= ContinuousClock.now.advanced(by: .seconds(900)))
        }
        let issued = await calls.issued
        #expect(issued.count == 2)
        let keys = try issued.map { try #require((JSONSerialization.jsonObject(with: $0) as? [String: Any])?["public_key"] as? String) }
        #expect(keys[0] != keys[1])
        #expect(await calls.redemptions == 2)
    }

    @Test func rejectedIssueNeverReachesRedemptionOrFallback() async throws {
        let calls = NativeBootstrapCalls()
        do {
            _ = try await NativeBootstrapClient.bootstrap(connection: nativeBootstrapConnection(), exchange: { data, _ in
                await calls.issue(data)
                throw NativeBootstrapError.refused(status: 403, message: "untrusted app identity")
            }, redeem: { _, _ in
                await calls.redeem()
                return try nativeCredentialData(token: nativeToken(9))
            })
            Issue.record("Refused bootstrap returned credentials")
        } catch let error as NativeBootstrapError {
            #expect(error == .refused(status: 403, message: "untrusted app identity"))
        }
        #expect(await calls.issued.count == 1)
        #expect(await calls.redemptions == 0)
    }

    @Test func cancellationAfterIssueCannotRedeemOneUseTicket() async throws {
        let connection = nativeBootstrapConnection()
        let calls = NativeBootstrapCalls()
        let task = Task {
            do {
                _ = try await NativeBootstrapClient.bootstrap(connection: connection, exchange: { data, _ in
                    await calls.issue(data)
                    let fields = try #require(JSONSerialization.jsonObject(with: data) as? [String: Any])
                    withUnsafeCurrentTask { $0?.cancel() }
                    return try nativeChallengeData(connection: connection, publicKey: #require(fields["public_key"] as? String))
                }, redeem: { _, _ in
                    await calls.redeem()
                    return try nativeCredentialData(token: nativeToken(9))
                })
                Issue.record("Cancelled bootstrap returned credentials")
            } catch is CancellationError { }
        }
        try await task.value
        #expect(await calls.issued.count == 1)
        #expect(await calls.redemptions == 0)
    }

    @Test func malformedIssueNeverReachesHTTP() async throws {
        let calls = NativeBootstrapCalls()
        do {
            _ = try await NativeBootstrapClient.bootstrap(connection: nativeBootstrapConnection(), exchange: { data, _ in
                await calls.issue(data)
                return Data(#"{"control_protocol":1,"operation":"status"}"#.utf8)
            }, redeem: { _, _ in
                await calls.redeem()
                return try nativeCredentialData(token: nativeToken(9))
            })
            Issue.record("SSCP/1 fallback was accepted as SSCP/2 admission")
        } catch is NativeBootstrapError { }
        #expect(await calls.issued.count == 1)
        #expect(await calls.redemptions == 0)
    }
}

private actor NativeBootstrapCalls {
    private(set) var issued: [Data] = []
    private(set) var redemptions = 0
    func issue(_ data: Data) { issued.append(data) }
    func redeem() { redemptions += 1 }
}

private func nativeBootstrapConnection(proof: String? = nil) -> ShellControlConnection {
    let origin = URL(string: "http://127.0.0.1:49152")!
    return .init(status: .init(controlProtocol: 1, daemonVersion: "12.0.0-beta.1", apiSchema: 1,
                              minimumShellProtocol: 1, maximumShellProtocol: 1,
                              instanceGeneration: nativeToken(0), state: .ready,
                              uiOrigin: origin.absoluteString, startupProof: proof), origin: origin)
}

private func nativeToken(_ byte: UInt8) -> String {
    NativeBootstrapClient.encodeBase64URL(Data(repeating: byte, count: 32))
}

private func nativeChallengeData(connection: ShellControlConnection, publicKey: String) throws -> Data {
    try JSONSerialization.data(withJSONObject: [
        "control_protocol": 2, "operation": "native-session.issue",
        "instance_generation": connection.status.instanceGeneration,
        "ui_origin": connection.origin.absoluteString, "startup_proof": connection.status.startupProof ?? "",
        "public_key": publicKey, "ticket": nativeToken(1), "challenge": nativeToken(2), "expires_in_seconds": 30,
    ])
}

private func nativeCredentialData(token: String) throws -> Data {
    try JSONSerialization.data(withJSONObject: ["native_session": token, "expires_in_seconds": 900])
}


@Suite(.serialized) struct NativeCredentialLifecycleQualification {
    @Test func credentialExpiringWhileLockedRetiresOnceBeforeLoginOrOtherHTTP() async throws {
        let fixture = nativeClientFixture(mode: .successfulLogin)
        let clock = NativeCredentialTestClock()
        let callbacks = NativeUnauthorizedCalls()
        let connection = nativeBootstrapConnection()
        let credential = NativeSessionCredential(token: nativeToken(9), expiresAt: clock.now().advanced(by: .seconds(900)), connection: connection)
        let api = SAGEAPIClient(baseURL: connection.origin, session: fixture, nativeCredential: credential,
                                now: { clock.now() }, onUnauthorized: { await callbacks.record() })
        defer { fixture.invalidateAndCancel() }
        clock.advance(by: .seconds(901))
        do {
            _ = try await api.login(passphrase: "correct but transport expired")
            Issue.record("Expired credential reached login")
        } catch let error as SAGEAPIError { #expect(error == .unauthorized) }
        for _ in 0..<2 {
            do { _ = try await api.authStatus(); Issue.record("Retired client remained usable") }
            catch is CancellationError { }
        }
        #expect(await callbacks.count == 1)
        #expect(NativeCredentialURLProtocol.state.requestCount == 0)
    }

    @Test func expiryIsCheckedByTypedPathsLockAndEventSubscription() async throws {
        for operation in ["typed-path", "lock", "events"] {
            let fixture = nativeClientFixture(mode: .successfulLogin)
            let clock = NativeCredentialTestClock()
            let callbacks = NativeUnauthorizedCalls()
            let connection = nativeBootstrapConnection()
            let credential = NativeSessionCredential(token: nativeToken(9), expiresAt: clock.now().advanced(by: .seconds(900)), connection: connection)
            let api = SAGEAPIClient(baseURL: connection.origin, session: fixture, nativeCredential: credential,
                                    now: { clock.now() }, onUnauthorized: { await callbacks.record() })
            clock.advance(by: .seconds(901))
            do {
                switch operation {
                case "typed-path": _ = try await api.memoryTags(id: "test")
                case "lock": try await api.lock()
                default: for try await _ in await api.events() { }
                }
                Issue.record("Expired credential accepted by \(operation)")
            } catch let error as SAGEAPIError { #expect(error == .unauthorized) }
            #expect(await callbacks.count == 1)
            #expect(NativeCredentialURLProtocol.state.requestCount == 0)
            fixture.invalidateAndCancel()
        }
    }

    @Test func serverRevokedAdmissionAtLoginTriggersFreshBootstrapPath() async throws {
        let fixture = nativeClientFixture(mode: .revokedAdmission)
        let callbacks = NativeUnauthorizedCalls()
        let connection = nativeBootstrapConnection()
        let credential = NativeSessionCredential(token: nativeToken(9), expiresAt: ContinuousClock.now.advanced(by: .seconds(900)), connection: connection)
        let api = SAGEAPIClient(baseURL: connection.origin, session: fixture, nativeCredential: credential,
                                onUnauthorized: { await callbacks.record() })
        defer { fixture.invalidateAndCancel() }
        do { _ = try await api.login(passphrase: "correct"); Issue.record("Revoked admission accepted") }
        catch let error as SAGEAPIError { #expect(error == .unauthorized) }
        do { _ = try await api.login(passphrase: "retry"); Issue.record("Revoked client reused") }
        catch is CancellationError { }
        #expect(await callbacks.count == 1)
        #expect(NativeCredentialURLProtocol.state.requestCount == 1)
    }

    @Test func wrongPassphraseRemainsVisibleAndTransportCanRetry() async throws {
        let fixture = nativeClientFixture(mode: .wrongPassword)
        let callbacks = NativeUnauthorizedCalls()
        let connection = nativeBootstrapConnection()
        let credential = NativeSessionCredential(token: nativeToken(9), expiresAt: ContinuousClock.now.advanced(by: .seconds(900)), connection: connection)
        let api = SAGEAPIClient(baseURL: connection.origin, session: fixture, nativeCredential: credential,
                                onUnauthorized: { await callbacks.record() })
        defer { fixture.invalidateAndCancel() }
        do { _ = try await api.login(passphrase: "wrong"); Issue.record("Wrong passphrase accepted") }
        catch let error as SAGEAPIError { #expect(error == .server(status: 401, message: "wrong passphrase")) }
        #expect(await callbacks.count == 0)
        NativeCredentialURLProtocol.state.mode = .successfulLogin
        #expect(try await api.login(passphrase: "correct").ok)
        #expect(await callbacks.count == 0)
        #expect(NativeCredentialURLProtocol.state.requestCount == 2)
    }
}

private actor NativeUnauthorizedCalls {
    private(set) var count = 0
    func record() { count += 1 }
}

private final class NativeCredentialTestClock: @unchecked Sendable {
    private let lock = NSLock()
    private var value = ContinuousClock.now
    func now() -> ContinuousClock.Instant { lock.withLock { value } }
    func advance(by duration: Duration) { lock.withLock { value = value.advanced(by: duration) } }
}

private enum NativeCredentialResponseMode { case revokedAdmission, wrongPassword, successfulLogin }

private final class NativeCredentialProtocolState: @unchecked Sendable {
    private let lock = NSLock()
    private var storedMode: NativeCredentialResponseMode = .successfulLogin
    private var requests = 0
    var mode: NativeCredentialResponseMode {
        get { lock.withLock { storedMode } }
        set { lock.withLock { storedMode = newValue } }
    }
    var requestCount: Int { lock.withLock { requests } }
    func reset(mode: NativeCredentialResponseMode) { lock.withLock { requests = 0; storedMode = mode } }
    func response() -> (Int, Data) {
        lock.withLock {
            requests += 1
            switch storedMode {
            case .revokedAdmission: return (401, Data(#"{"error":"unauthorized","login_required":true}"#.utf8))
            case .wrongPassword: return (401, Data(#"{"ok":false,"error":"wrong passphrase"}"#.utf8))
            case .successfulLogin: return (200, Data(#"{"ok":true}"#.utf8))
            }
        }
    }
}

private final class NativeCredentialURLProtocol: URLProtocol, @unchecked Sendable {
    static let state = NativeCredentialProtocolState()
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        let (status, data) = Self.state.response()
        let response = HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: "HTTP/1.1", headerFields: ["Content-Type": "application/json"])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: data)
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() { }
}

private func nativeClientFixture(mode: NativeCredentialResponseMode) -> URLSession {
    NativeCredentialURLProtocol.state.reset(mode: mode)
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [NativeCredentialURLProtocol.self]
    configuration.httpCookieStorage = nil
    configuration.urlCredentialStorage = nil
    configuration.urlCache = nil
    return URLSession(configuration: configuration)
}
