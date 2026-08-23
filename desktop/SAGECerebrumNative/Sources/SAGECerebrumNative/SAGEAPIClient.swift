import Foundation

enum APIEndpoint: String, Sendable {
    case authCheck = "/v1/dashboard/auth/check"
    case authLogin = "/v1/dashboard/auth/login"
    case authLock = "/v1/dashboard/auth/lock"
    case health = "/v1/dashboard/health"
    case stats = "/v1/dashboard/stats"
    case agents = "/v1/dashboard/network/agents"
    case validators = "/v1/dashboard/chain/validators"
    case federation = "/v1/dashboard/federation/connections"
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
    func events() async -> AsyncThrowingStream<DashboardEvent, Error>
}

actor SAGEAPIClient: SAGEAPI {
    let baseURL: URL
    private let session: URLSession
    private let decoder = JSONDecoder.sageDashboard()
    private let encoder = JSONEncoder()
    private let onUnauthorized: @Sendable () async -> Void

    init(
        baseURL: URL,
        session: URLSession? = nil,
        onUnauthorized: @escaping @Sendable () async -> Void = {}
    ) {
        self.baseURL = baseURL
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
        let _: LoginResult = try await send(.authLock, method: "POST")
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

    func events() async -> AsyncThrowingStream<DashboardEvent, Error> {
        let request = makeRequest(.events)
        let session = self.session
        return AsyncThrowingStream { continuation in
            let reader = Task {
                var reconnectDelay = Duration.seconds(1)
                while !Task.isCancelled {
                    do {
                        let (bytes, response) = try await session.bytes(for: request)
                        guard let response = response as? HTTPURLResponse else {
                            throw DashboardEventError.invalidResponse
                        }
                        guard 200 ..< 300 ~= response.statusCode else {
                            if response.statusCode == 401 {
                                await self.onUnauthorized()
                                continuation.finish(throwing: SAGEAPIError.unauthorized)
                                return
                            }
                            throw DashboardEventError.server(status: response.statusCode)
                        }
                        reconnectDelay = .seconds(1)
                        continuation.yield(DashboardEvent(name: "connected", data: "", receivedAt: .now))
                        var accumulator = SSEEventAccumulator()
                        for try await line in bytes.lines {
                            if Task.isCancelled { break }
                            if let event = accumulator.consume(line) {
                                continuation.yield(event)
                            }
                        }
                    } catch is CancellationError {
                        break
                    } catch {
                        if Task.isCancelled { break }
                        continuation.yield(DashboardEvent(name: "disconnected", data: "", receivedAt: .now))
                        try? await Task.sleep(for: reconnectDelay)
                        reconnectDelay = min(reconnectDelay * 2, .seconds(15))
                    }
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in reader.cancel() }
        }
    }

    private func send<Response: Decodable>(
        _ endpoint: APIEndpoint,
        method: String = "GET",
        body: [String: String]? = nil,
        queryItems: [URLQueryItem] = []
    ) async throws -> Response {
        var request = makeRequest(endpoint, method: method, queryItems: queryItems)
        if let body {
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try encoder.encode(body)
        }
        let (data, response) = try await session.data(for: request)
        guard let response = response as? HTTPURLResponse else {
            throw SAGEAPIError.invalidResponse
        }
        if response.statusCode == 401 {
            await onUnauthorized()
            throw SAGEAPIError.unauthorized
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
        request.setValue(baseURL.absoluteString.trimmingCharacters(in: CharacterSet(charactersIn: "/")), forHTTPHeaderField: "Origin")
        request.setValue("same-origin", forHTTPHeaderField: "Sec-Fetch-Site")
        let (data, response) = try await session.data(for: request)
        guard let response = response as? HTTPURLResponse else { throw SAGEAPIError.invalidResponse }
        if response.statusCode == 401 {
            await onUnauthorized()
            throw SAGEAPIError.unauthorized
        }
        guard 200 ..< 300 ~= response.statusCode else {
            let payload = (try? decoder.decode(ErrorPayload.self, from: data))?.error
            throw SAGEAPIError.server(status: response.statusCode, message: payload ?? HTTPURLResponse.localizedString(forStatusCode: response.statusCode))
        }
        return try decoder.decode(Response.self, from: data)
    }

    private func pathSegment(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-._~"))
        return value.addingPercentEncoding(withAllowedCharacters: allowed) ?? value
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
        request.setValue(
            baseURL.absoluteString.trimmingCharacters(in: CharacterSet(charactersIn: "/")),
            forHTTPHeaderField: "Origin"
        )
        request.setValue("same-origin", forHTTPHeaderField: "Sec-Fetch-Site")
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
}
