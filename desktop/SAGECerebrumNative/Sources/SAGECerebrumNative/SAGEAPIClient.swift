import Foundation

enum APIEndpoint: String, Sendable {
    case authCheck = "/v1/dashboard/auth/check"
    case authLogin = "/v1/dashboard/auth/login"
    case nativeRevoke = "/v1/dashboard/native/revoke"
    case authLock = "/v1/dashboard/auth/lock"
    case health = "/v1/dashboard/health"
    case stats = "/v1/dashboard/stats"
    case agents = "/v1/dashboard/network/agents"
    case validators = "/v1/dashboard/chain/validators"
    case federation = "/v1/dashboard/federation/connections"
    case federationSetting = "/v1/dashboard/settings/federation"
    case events = "/v1/dashboard/events"
    case memories = "/v1/dashboard/memory/list"
    case tags = "/v1/dashboard/tags"
    case graph = "/v1/dashboard/memory/graph"
    case synapses = "/v1/dashboard/network/synapses"
    case engrams = "/v1/dashboard/memory/engrams"
}

enum SAGEAPIError: LocalizedError, Equatable, Sendable {
    case invalidResponse
    case unauthorized
    case server(status: Int, message: String)

    var errorDescription: String? {
        switch self {
        case .invalidResponse: "SAGE returned an invalid response."
        case .unauthorized: "CEREBRUM is locked."
        case let .server(_, message): message
        }
    }
}

protocol SAGEAPI: Sendable {
    func invalidate() async
    func authStatus() async throws -> AuthStatus
    func login(passphrase: String) async throws -> LoginResult
    func lock() async throws
    func health() async throws -> DashboardHealth
    func stats() async throws -> DashboardStats
    func agents() async throws -> AgentOverviewEnvelope
    func validators() async throws -> ValidatorOverview
    func federation() async throws -> FederationOverview
    func memories(_ query: MemoryListQuery) async throws -> MemoryListEnvelope
    func tags() async throws -> TagEnvelope
    func memoryTags(id: String) async throws -> MemoryTagsEnvelope
    func setMemoryTags(id: String, tags: [String]) async throws -> MemoryTagsEnvelope
    func addTag(_ tag: String, to ids: [String]) async throws -> BulkMemoryUpdateResponse
    func forgetMemory(id: String) async throws -> MemoryMutationResponse
    func brainGraph(_ query: BrainGraphQuery) async throws -> BrainGraphEnvelope
    func connectome() async throws -> ConnectomeEnvelope
    func agentEngrams(agentID: String) async throws -> AgentEngramEnvelope
    func relatedMemories(memoryID: String, limit: Int) async throws -> RelatedMemoryEnvelope
    func events() async -> AsyncThrowingStream<DashboardEventStreamElement, Error>
}

extension SAGEAPI {
    // Stateless previews and fixture APIs do not own credentials or transports.
    func invalidate() async {}
}

actor SAGEAPIClient: SAGEAPI {
    let baseURL: URL
    private let session: URLSession
    private let decoder = JSONDecoder.sageDashboard()
    private let encoder = JSONEncoder()
    private let onUnauthorized: @Sendable () async -> Void
    private let now: @Sendable () -> ContinuousClock.Instant
    private var invalidated = false
    private var nativeCredential: NativeSessionCredential?
    private var revocationSession: URLSession?
    private var eventReaders: [UUID: Task<Void, Error>] = [:]

    init(
        baseURL: URL,
        session: URLSession? = nil,
        nativeCredential: NativeSessionCredential? = nil,
        now: @escaping @Sendable () -> ContinuousClock.Instant = { ContinuousClock.now },
        onUnauthorized: @escaping @Sendable () async -> Void = {}
    ) {
        self.baseURL = baseURL
        self.now = now
        self.nativeCredential = nativeCredential
        #if !SAGE_LEGACY_TRANSPORT_QUALIFICATION
        if nativeCredential == nil { self.invalidated = true }
        #endif
        if let credential = nativeCredential,
           (credential.connection.origin != baseURL || credential.expiresAt <= now()) {
            self.invalidated = true
            self.nativeCredential = nil
        }
        self.onUnauthorized = onUnauthorized
        if let session {
            self.session = session
        } else {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.httpShouldSetCookies = true
            configuration.httpCookieAcceptPolicy = .always
            configuration.timeoutIntervalForRequest = 15
            self.session = URLSession(
                configuration: configuration,
                delegate: LoopbackRedirectDelegate(origin: baseURL),
                delegateQueue: nil
            )
        }
    }

    func invalidate() async {
        let revoke = !invalidated && nativeCredential.map({ $0.expiresAt > now() }) == true
            ? makeRequest(.nativeRevoke, method: "POST") : nil
        invalidateTransport()
        if let revoke {
            // Local retirement is immediate. Best-effort daemon revocation owns
            // a separate bounded, cookie-free connection and cannot block it.
            Task {
                let configuration = URLSessionConfiguration.ephemeral
                configuration.httpShouldSetCookies = false
                configuration.httpCookieStorage = nil
                configuration.timeoutIntervalForRequest = 2
                configuration.timeoutIntervalForResource = 2
                let revoker = URLSession(configuration: configuration, delegate: LoopbackRedirectDelegate(origin: baseURL), delegateQueue: nil)
                defer { revoker.invalidateAndCancel() }
                _ = try? await revoker.data(for: revoke)
            }
        }
    }

    private func invalidateTransport() {
        revocationSession?.invalidateAndCancel()
        revocationSession = nil
        invalidated = true
        nativeCredential = nil
        let readers = eventReaders.values
        eventReaders.removeAll()
        for reader in readers { reader.cancel() }
        session.invalidateAndCancel()
        session.configuration.httpCookieStorage?.removeCookies(since: .distantPast)
    }

    static func isSafeLoopback(_ url: URL) -> Bool {
        guard url.scheme == "http" || url.scheme == "https",
              url.user == nil,
              url.password == nil,
              let host = url.host,
              url.port != nil,
              (url.path.isEmpty || url.path == "/"),
              url.query == nil,
              url.fragment == nil
        else { return false }
        return host == "127.0.0.1" || host == "::1" || host.lowercased() == "localhost"
    }

    func authStatus() async throws -> AuthStatus {
        try await send(.authCheck)
    }

    func login(passphrase: String) async throws -> LoginResult {
        try await send(.authLogin, method: "POST", body: ["passphrase": passphrase])
    }

    func lock() async throws {
        try await requireActiveCredential()
        var request = makeRequest(.authLock, method: "POST")
        if let cookies = session.configuration.httpCookieStorage?.cookies(for: baseURL) {
            for (name, value) in HTTPCookie.requestHeaderFields(with: cookies) {
                request.setValue(value, forHTTPHeaderField: name)
            }
        }
        // The old client becomes unusable before the revocation request can
        // suspend. Its event streams and protected requests stop immediately.
        invalidateTransport()
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = false
        configuration.httpCookieStorage = nil
        configuration.timeoutIntervalForRequest = 15
        let revoker = URLSession(configuration: configuration,
                                 delegate: LoopbackRedirectDelegate(origin: baseURL), delegateQueue: nil)
        revocationSession = revoker
        defer {
            revoker.invalidateAndCancel()
            revocationSession = nil
        }
        let (_, response) = try await revoker.data(for: request)
        guard let response = response as? HTTPURLResponse else { throw SAGEAPIError.invalidResponse }
        guard 200 ..< 300 ~= response.statusCode else {
            throw SAGEAPIError.server(status: response.statusCode,
                                     message: HTTPURLResponse.localizedString(forStatusCode: response.statusCode))
        }
    }

    func health() async throws -> DashboardHealth {
        try await send(.health)
    }

    func stats() async throws -> DashboardStats {
        try await send(.stats)
    }

    func agents() async throws -> AgentOverviewEnvelope {
        try await send(.agents)
    }

    func validators() async throws -> ValidatorOverview {
        try await send(.validators)
    }

    func federation() async throws -> FederationOverview {
        do {
            // Connections remain readable while networking is off. Only the
            // settings endpoint reports the transport's actual enabled state.
            let setting: FederationSetting = try await send(.federationSetting)
            guard setting.enabled else { return .disabled }
            return try await send(.federation)
        } catch let SAGEAPIError.server(status, _) where status == 501 {
            return .disabled
        }
    }

    func memories(_ query: MemoryListQuery) async throws -> MemoryListEnvelope {
        try await send(.memories, queryItems: query.queryItems)
    }

    func tags() async throws -> TagEnvelope {
        try await send(.tags)
    }

    func memoryTags(id: String) async throws -> MemoryTagsEnvelope {
        try await send(path: "/v1/dashboard/memory/\(pathSegment(id))/tags")
    }

    func setMemoryTags(id: String, tags: [String]) async throws -> MemoryTagsEnvelope {
        try await send(path: "/v1/dashboard/memory/\(pathSegment(id))/tags", method: "PUT", body: ["tags": tags])
    }

    func addTag(_ tag: String, to ids: [String]) async throws -> BulkMemoryUpdateResponse {
        try await send(path: "/v1/dashboard/memory/bulk", method: "POST", body: ["ids": ids, "add_tags": [tag]])
    }

    func forgetMemory(id: String) async throws -> MemoryMutationResponse {
        try await send(path: "/v1/dashboard/memory/\(pathSegment(id))", method: "DELETE", body: Optional<[String: String]>.none)
    }

    func brainGraph(_ query: BrainGraphQuery) async throws -> BrainGraphEnvelope {
        try await send(.graph, queryItems: query.queryItems)
    }

    func connectome() async throws -> ConnectomeEnvelope {
        try await send(.synapses)
    }

    func agentEngrams(agentID: String) async throws -> AgentEngramEnvelope {
        try await send(.engrams, queryItems: [.init(name: "agent", value: agentID)])
    }

    func relatedMemories(memoryID: String, limit: Int = 50) async throws -> RelatedMemoryEnvelope {
        try await send(
            path: "/v1/dashboard/memory/\(pathSegment(memoryID))/related",
            queryItems: [.init(name: "k", value: String(min(max(limit, 1), 120)))]
        )
    }

    func events() async -> AsyncThrowingStream<DashboardEventStreamElement, Error> {
        do { try await requireActiveCredential() }
        catch { return AsyncThrowingStream { $0.finish(throwing: error) } }
        let readerID = UUID()
        let request = makeRequest(.events)
        let session = self.session
        return AsyncThrowingStream { continuation in
            let reader = Task {
                defer { self.eventReaders.removeValue(forKey: readerID) }
                var reconnectDelay = Duration.seconds(1)
                continuation.yield(.state(.connecting))
                while !Task.isCancelled && !self.invalidated {
                    do {
                        try await self.requireActiveCredential()
                        let (bytes, response) = try await session.bytes(for: request)
                        guard !Task.isCancelled, !self.invalidated else { break }
                        guard let response = response as? HTTPURLResponse else {
                            throw DashboardEventError.invalidResponse
                        }
                        guard 200 ..< 300 ~= response.statusCode else {
                            if response.statusCode == 401 {
                                await self.retireUnauthorized()
                                continuation.finish(throwing: SAGEAPIError.unauthorized)
                                return
                            }
                            throw DashboardEventError.server(status: response.statusCode)
                        }
                        continuation.yield(.state(.connected))
                        var accumulator = SSEEventAccumulator()
                        var lines = SSELineAccumulator()
                        var observedActivity = false
                        for try await byte in bytes {
                            if Task.isCancelled || self.invalidated { break }
                            guard let line = try lines.consume(byte) else { continue }
                            if !observedActivity, !line.isEmpty {
                                observedActivity = true
                                reconnectDelay = .seconds(1)
                            }
                            if let event = accumulator.consume(line) {
                                continuation.yield(.event(event))
                            }
                        }
                        if !Task.isCancelled && !self.invalidated {
                            continuation.yield(.state(.reconnecting))
                            do {
                                try await Task.sleep(for: reconnectDelay)
                            } catch is CancellationError {
                                break
                            }
                            if !observedActivity {
                                reconnectDelay = min(reconnectDelay * 2, .seconds(15))
                            }
                        }
                    } catch is CancellationError {
                        break
                    } catch {
                        if Task.isCancelled || self.invalidated { break }
                        continuation.yield(.state(.reconnecting))
                        do {
                            try await Task.sleep(for: reconnectDelay)
                        } catch is CancellationError {
                            break
                        }
                        reconnectDelay = min(reconnectDelay * 2, .seconds(15))
                    }
                }
                continuation.finish()
            }
            eventReaders[readerID] = reader
            continuation.onTermination = { _ in reader.cancel() }
        }
    }

    private func send<Response: Decodable>(
        _ endpoint: APIEndpoint,
        method: String = "GET",
        body: [String: String]? = nil,
        queryItems: [URLQueryItem] = []
    ) async throws -> Response {
        try await requireActiveCredential()
        var request = makeRequest(endpoint, method: method, queryItems: queryItems)
        if let body {
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try encoder.encode(body)
        }
        guard !invalidated else { throw CancellationError() }
        let (data, response) = try await session.data(for: request)
        guard !invalidated else { throw CancellationError() }
        guard let response = response as? HTTPURLResponse else {
            throw SAGEAPIError.invalidResponse
        }
        if response.statusCode == 401 {
            let refusal = try? decoder.decode(ErrorPayload.self, from: data)
            // Wrong passphrases are retryable. A native-admission refusal also
            // uses 401, but its canonical login_required flag means this client
            // must retire so AppSession can bootstrap a fresh transport.
            if endpoint != .authLogin || refusal?.loginRequired == true {
                await retireUnauthorized()
                throw SAGEAPIError.unauthorized
            }
        }
        guard 200 ..< 300 ~= response.statusCode else {
            let payload = (try? decoder.decode(ErrorPayload.self, from: data))?.error
            throw SAGEAPIError.server(
                status: response.statusCode,
                message: payload ?? HTTPURLResponse.localizedString(forStatusCode: response.statusCode)
            )
        }
        return try decoder.decode(Response.self, from: data)
    }

    private func send<Response: Decodable, Body: Encodable>(
        path: String,
        method: String = "GET",
        body: Body? = Optional<[String: String]>.none,
        queryItems: [URLQueryItem] = []
    ) async throws -> Response {
        try await requireActiveCredential()
        var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false)!
        components.percentEncodedPath = path
        if !queryItems.isEmpty { components.queryItems = queryItems }
        var request = URLRequest(url: components.url!)
        request.httpMethod = method
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if let body {
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try encoder.encode(body)
        }
        applyTransportIdentity(to: &request)
        guard !invalidated else { throw CancellationError() }
        let (data, response) = try await session.data(for: request)
        guard !invalidated else { throw CancellationError() }
        guard let response = response as? HTTPURLResponse else { throw SAGEAPIError.invalidResponse }
        if response.statusCode == 401 {
            await retireUnauthorized()
            throw SAGEAPIError.unauthorized
        }
        guard 200 ..< 300 ~= response.statusCode else {
            let payload = (try? decoder.decode(ErrorPayload.self, from: data))?.error
            throw SAGEAPIError.server(status: response.statusCode, message: payload ?? HTTPURLResponse.localizedString(forStatusCode: response.statusCode))
        }
        return try decoder.decode(Response.self, from: data)
    }

    private func requireActiveCredential() async throws {
        guard !invalidated else { throw CancellationError() }
        if let credential = nativeCredential, credential.expiresAt <= now() {
            await retireUnauthorized()
            throw SAGEAPIError.unauthorized
        }
    }

    private func retireUnauthorized() async {
        guard !invalidated else { return }
        // Retire before awaiting the callback: concurrent requests must neither
        // reuse the expired credential nor notify the session twice. The daemon
        // already rejects this expired/revoked admission, so no logout is needed.
        invalidateTransport()
        await onUnauthorized()
    }

    private func pathSegment(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-._~"))
        return value.addingPercentEncoding(withAllowedCharacters: allowed) ?? value
    }

    private func applyTransportIdentity(to request: inout URLRequest) {
        if let credential = nativeCredential {
            request.setValue(credential.token, forHTTPHeaderField: "X-SAGE-Native-Session")
        }
        #if SAGE_LEGACY_TRANSPORT_QUALIFICATION
        if nativeCredential == nil {
            // Only the separate legacy transport fixture compiles this bridge.
            request.setValue(baseURL.absoluteString.trimmingCharacters(in: CharacterSet(charactersIn: "/")), forHTTPHeaderField: "Origin")
            request.setValue("same-origin", forHTTPHeaderField: "Sec-Fetch-Site")
        }
        #endif
    }

    private func makeRequest(
        _ endpoint: APIEndpoint,
        method: String = "GET",
        queryItems: [URLQueryItem] = []
    ) -> URLRequest {
        let endpointURL = baseURL.appending(path: endpoint.rawValue)
        var components = URLComponents(url: endpointURL, resolvingAgainstBaseURL: false)!
        if !queryItems.isEmpty { components.queryItems = queryItems }
        var request = URLRequest(url: components.url!)
        request.httpMethod = method
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        applyTransportIdentity(to: &request)
        return request
    }
}

private final class LoopbackRedirectDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    private let origin: URL

    init(origin: URL) {
        self.origin = origin
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        guard let candidate = request.url,
              candidate.scheme == origin.scheme,
              candidate.host?.lowercased() == origin.host?.lowercased(),
              candidate.port == origin.port
        else {
            completionHandler(nil)
            return
        }
        completionHandler(request)
    }
}

private struct ErrorPayload: Decodable {
    let error: String?
    let loginRequired: Bool?
    enum CodingKeys: String, CodingKey {
        case error
        case loginRequired = "login_required"
    }
}

private struct FederationSetting: Decodable {
    let enabled: Bool
}
