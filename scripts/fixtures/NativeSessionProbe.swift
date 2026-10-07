// Test-only release entry point. All session, menu, discovery and HTTP types
// below are the production source files, without mocks or DEBUG overrides.
import CryptoKit
import Foundation

@main
@MainActor
struct NativeSessionProbe {
    static func main() async throws {
        guard CommandLine.arguments.count == 2 else { throw ProbeFailure.invalidInput }
        let home = URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
        let context = NativeSessionProbeContext(home: home)
        let session = AppSession(discover: {
            try await ShellControlClient.discoverConnection(sageHome: home, timeout: .seconds(1))
        }, makeClient: { connection, unauthorized in
            try await context.makeClient(connection: connection, unauthorized: unauthorized)
        })
        var retainedClient: (any SAGEAPI)?
        var retainedCredential: NativeSessionCredential?
        defer { session.stopMonitoring() }
        while let line = await Task.detached(operation: { readLine() }).value {
            guard let data = line.data(using: .utf8),
                  let command = try JSONSerialization.jsonObject(with: data) as? [String: String],
                  let operation = command["operation"] else { throw ProbeFailure.invalidInput }
            var result: [String: Any] = ["ok": true]
            switch operation {
            case "unencrypted":
                result.merge(try await context.checkUnencrypted()) { _, new in new }
            case "bootstrap-contract":
                result.merge(try await context.checkBootstrapContract()) { _, new in new }
            case "cookie-isolation":
                result.merge(try await context.checkCookieIsolation()) { _, new in new }
            case "retained-admission":
                guard let credential = retainedCredential else { throw ProbeFailure.missingClient }
                let (status, _) = try await NativeSessionProbeContext.request(
                    origin: credential.connection.origin, path: "/v1/dashboard/auth/check",
                    headers: ["X-SAGE-Native-Session": credential.token])
                result["admission_rejected"] = status == 401
            case "start":
                await session.connect()
                session.startMonitoring()
            case "snapshot": break
            case "route":
                guard let value = command["route"], let route = AppRoute(rawValue: value), route.isImplemented else {
                    throw ProbeFailure.invalidInput
                }
                session.route = route
            case "login":
                guard let passphrase = command["passphrase"] else { throw ProbeFailure.invalidInput }
                session.passphrase = passphrase
                await session.login()
            case "lock": await session.lock()
            case "retain-client":
                guard let api = session.api else { throw ProbeFailure.missingClient }
                retainedClient = api
                retainedCredential = context.credential
            case "read", "retained-read":
                let api = operation == "read" ? session.api : retainedClient
                if let api {
                    do {
                        let health = try await api.health()
                        let memories = try await api.memories(MemoryListQuery())
                        result["read_succeeded"] = true
                        result["encrypted"] = health.encrypted
                        result["vault_locked"] = health.vaultLocked
                        result["app_version"] = health.chain?.appVersion ?? ""
                        result["fixture_memory_found"] = memories.memories.contains {
                            $0.id == command["memory_id"] && $0.content == command["content"]
                        }
                    } catch {
                        // Never print response bodies, credentials, or cookies.
                        result["read_succeeded"] = false
                    }
                } else { result["read_succeeded"] = false }
            case "finish":
                session.stopMonitoring()
                result["finished"] = true
            default: throw ProbeFailure.invalidInput
            }
            result["phase"] = switch session.phase {
            case .connecting: "connecting"
            case .ready: "ready"
            case .locked: "locked"
            case .failed: "failed"
            }
            result["has_api"] = session.api != nil
            result["ready_commands"] = session.acceptsReadyCommands
            result["route"] = session.route.rawValue
            result["epoch"] = session.sessionEpoch
            result["generation"] = session.connection?.status.instanceGeneration ?? ""
            result["login_error"] = session.loginError != nil
            result["passphrase_empty"] = session.passphrase.isEmpty
            let output = try JSONSerialization.data(withJSONObject: result, options: [.sortedKeys])
            FileHandle.standardOutput.write(output + Data([0x0A]))
            if operation == "finish" { return }
        }
        throw ProbeFailure.unexpectedEOF
    }

    enum ProbeFailure: Error { case invalidInput, missingClient, unexpectedEOF }
}


// The fixture retains a private cookie store only to test theft/replay boundaries.
// It never prints any cookie, ticket, signing key or admission credential.
@MainActor
private final class NativeSessionProbeContext {
    let home: URL
    var credential: NativeSessionCredential?
    var transport: URLSession?
    init(home: URL) { self.home = home }

    func makeClient(connection: ShellControlConnection,
                    unauthorized: @escaping @Sendable () async -> Void) async throws -> any SAGEAPI {
        let admitted = try await NativeBootstrapClient.bootstrap(connection: connection, sageHome: home)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = true
        configuration.httpCookieAcceptPolicy = .always
        configuration.timeoutIntervalForRequest = 15
        let session = URLSession(configuration: configuration)
        credential = admitted
        transport = session
        return SAGEAPIClient(baseURL: connection.origin, session: session,
                             nativeCredential: admitted, onUnauthorized: unauthorized)
    }

    func checkUnencrypted() async throws -> [String: Any] {
        let connection = try await ShellControlClient.discoverConnection(sageHome: home)
        let admitted = try await NativeBootstrapClient.bootstrap(connection: connection, sageHome: home)
        let (status, _) = try await Self.request(origin: connection.origin, path: "/v1/dashboard/auth/check",
                                               headers: ["X-SAGE-Native-Session": admitted.token])
        let (revoked, _) = try await Self.request(origin: connection.origin, path: "/v1/dashboard/native/revoke",
                                                method: "POST", headers: ["X-SAGE-Native-Session": admitted.token])
        guard status == 403, revoked == 200 else { throw NativeSessionProbe.ProbeFailure.invalidInput }
        return ["unencrypted_refused": true]
    }

    func checkBootstrapContract() async throws -> [String: Any] {
        let connection = try await ShellControlClient.discoverConnection(sageHome: home)
        let binding = try NativeBootstrapBinding(connection: connection)
        let privateKey = Curve25519.Signing.PrivateKey()
        let publicKey = NativeBootstrapClient.encodeBase64URL(privateKey.publicKey.rawRepresentation)
        let request: [String: Any] = ["control_protocol": 2, "shell_protocol": 1,
            "operation": "native-session.issue", "instance_generation": binding.instanceGeneration,
            "ui_origin": binding.uiOrigin, "startup_proof": binding.startupProof, "public_key": publicKey]
        let response = try await ShellControlClient.exchange(request: JSONSerialization.data(withJSONObject: request), sageHome: home,
            challengeResponse: { challenge in
                try NativeBootstrapClient.makePeerProof(challenge, binding: binding, publicKey: publicKey, privateKey: privateKey)
            })
        let challenge = try NativeBootstrapClient.validateIssueResponse(response, binding: binding, publicKey: publicKey)
        let proof = try NativeBootstrapClient.makeRedemption(challenge: challenge, privateKey: privateKey)
        let payload = try JSONEncoder().encode(proof)
        guard let fields = try JSONSerialization.jsonObject(with: payload) as? [String: String] else {
            throw NativeSessionProbe.ProbeFailure.invalidInput
        }
        var wrongSignature = fields
        wrongSignature["signature"] = NativeBootstrapClient.encodeBase64URL(Data(repeating: 0, count: 64))
        guard try await redeem(connection.origin, fields: wrongSignature).0 == 401 else {
            throw NativeSessionProbe.ProbeFailure.invalidInput
        }
        let wrongKey = Curve25519.Signing.PrivateKey()
        var wrongPublicKey = fields
        wrongPublicKey["public_key"] = NativeBootstrapClient.encodeBase64URL(wrongKey.publicKey.rawRepresentation)
        let wrongTranscript = ["SAGE-NATIVE-BOOTSTRAP/1", fields["ticket"]!, fields["challenge"]!,
            fields["instance_generation"]!, fields["ui_origin"]!, fields["startup_proof"]!, wrongPublicKey["public_key"]!].joined(separator: "\n")
        wrongPublicKey["signature"] = NativeBootstrapClient.encodeBase64URL(try wrongKey.signature(for: Data(wrongTranscript.utf8)))
        guard try await redeem(connection.origin, fields: wrongPublicKey).0 == 401 else {
            throw NativeSessionProbe.ProbeFailure.invalidInput
        }
        for (name, changed) in ["instance_generation": NativeBootstrapClient.encodeBase64URL(Data(repeating: 7, count: 32)),
                                "ui_origin": "http://127.0.0.1:1", "startup_proof": String(repeating: "a", count: 64),
                                "challenge": NativeBootstrapClient.encodeBase64URL(Data(repeating: 8, count: 32))] {
            var altered = fields
            altered[name] = changed
            guard try await redeem(connection.origin, fields: altered).0 == 401 else {
                throw NativeSessionProbe.ProbeFailure.invalidInput
            }
        }
        async let first = Self.request(origin: connection.origin, path: "/v1/dashboard/native/redeem", method: "POST", body: payload)
        async let second = Self.request(origin: connection.origin, path: "/v1/dashboard/native/redeem", method: "POST", body: payload)
        let (a, b) = try await (first, second)
        guard [a.0, b.0].sorted() == [200, 401] else { throw NativeSessionProbe.ProbeFailure.invalidInput }
        let token = try NativeBootstrapClient.validateRedemptionResponse(a.0 == 200 ? a.1 : b.1)
        guard try await redeem(connection.origin, fields: fields).0 == 401 else {
            throw NativeSessionProbe.ProbeFailure.invalidInput
        }
        let (protected, _) = try await Self.request(origin: connection.origin, path: "/v1/dashboard/memory/list",
                                                  headers: ["X-SAGE-Native-Session": token])
        guard protected == 401 else { throw NativeSessionProbe.ProbeFailure.invalidInput }
        let (revoked, _) = try await Self.request(origin: connection.origin, path: "/v1/dashboard/native/revoke",
                                                method: "POST", headers: ["X-SAGE-Native-Session": token])
        guard revoked == 200 else { throw NativeSessionProbe.ProbeFailure.invalidInput }
        return ["signature_and_key_refused": true, "altered_binding_refused": true,
                "signed_redemption_accepted": true, "concurrent_single_winner": true,
                "replay_refused": true, "admission_without_vault_refused": true]
    }

    private func redeem(_ origin: URL, fields: [String: String]) async throws -> (Int, Data) {
        try await Self.request(origin: origin, path: "/v1/dashboard/native/redeem", method: "POST",
                               body: JSONSerialization.data(withJSONObject: fields))
    }

    func checkCookieIsolation() async throws -> [String: Any] {
        guard let credential, let transport,
              let cookies = transport.configuration.httpCookieStorage?.cookies(for: credential.connection.origin),
              !cookies.isEmpty else { throw NativeSessionProbe.ProbeFailure.missingClient }
        let cookieHeaders = HTTPCookie.requestHeaderFields(with: cookies)
        var copied = cookieHeaders
        copied["Origin"] = credential.connection.origin.absoluteString
        copied["Sec-Fetch-Site"] = "same-origin"
        let (withoutAdmission, _) = try await Self.request(origin: credential.connection.origin,
            path: "/v1/dashboard/memory/list", headers: copied)
        var fabricated = copied
        fabricated["X-SAGE-Native-Session"] = credential.token
        let (withBrowserMetadata, _) = try await Self.request(origin: credential.connection.origin,
            path: "/v1/dashboard/memory/list", headers: fabricated)
        var legitimate = cookieHeaders
        legitimate["X-SAGE-Native-Session"] = credential.token
        let (withAdmission, _) = try await Self.request(origin: credential.connection.origin,
            path: "/v1/dashboard/memory/list", headers: legitimate)
        guard withoutAdmission == 401, withBrowserMetadata == 401, withAdmission == 200 else {
            throw NativeSessionProbe.ProbeFailure.invalidInput
        }
        return ["copied_cookie_refused": true, "browser_metadata_refused": true,
                "native_cookie_and_admission_accepted": true]
    }

    nonisolated static func request(origin: URL, path: String, method: String = "GET",
                                    headers: [String: String] = [:], body: Data? = nil) async throws -> (Int, Data) {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = false
        configuration.httpCookieStorage = nil
        configuration.urlCredentialStorage = nil
        configuration.timeoutIntervalForRequest = 5
        configuration.timeoutIntervalForResource = 5
        let session = URLSession(configuration: configuration)
        defer { session.invalidateAndCancel() }
        var request = URLRequest(url: origin.appending(path: path))
        request.httpMethod = method
        for (name, value) in headers { request.setValue(value, forHTTPHeaderField: name) }
        if let body {
            request.httpBody = body
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        let (data, response) = try await session.data(for: request)
        guard let response = response as? HTTPURLResponse else { throw NativeSessionProbe.ProbeFailure.invalidInput }
        return (response.statusCode, data)
    }
}
