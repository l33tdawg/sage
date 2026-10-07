import Foundation
import Testing
@testable import SAGECerebrumNative

@Suite("AppSession lifecycle")
@MainActor
struct AppSessionLifecycleTests {
    @Test func discoveryIsSingleFlightAndFreshIdentityReplacesClient() async throws {
        let gate = LifecycleGate<ShellControlConnection>()
        let discovery = LifecycleDiscovery(gate: gate)
        let factory = LifecycleFactory()
        let session = makeSession(discovery, factory)
        let first = Task { await session.connect() }
        try await eventually { await discovery.calls == 1 }
        let second = Task { await session.connect() }
        await Task.yield()
        #expect(await discovery.calls == 1)
        await gate.release(.success(connection("A")))
        await first.value
        await second.value
        try await eventually { session.phase == .locked }
        #expect(factory.clients.count == 1)
        let epoch = session.sessionEpoch
        await discovery.set(.success(connection("A")))
        await session.connect()
        #expect(session.sessionEpoch == epoch)
        await discovery.set(.success(connection("B")))
        await session.connect()
        try await eventually { session.phase == .locked && factory.clients.count == 2 }
        #expect(session.sessionEpoch > epoch)
        #expect(await factory.clients[0].invalidations == 1)
        #expect(session.connection?.status.instanceGeneration == connection("B").status.instanceGeneration)
        session.stopMonitoring()
    }

    @Test func slowAuthenticationCannotBlockDiscoveryAndCannotResurrectOldGeneration() async throws {
        let blocked = LifecycleGate<AuthStatus>()
        let discovery = LifecycleDiscovery()
        let factory = LifecycleFactory(authGates: [blocked])
        let session = makeSession(discovery, factory)
        await session.connect()
        try await eventually { await factory.clients.first?.authCalls == 1 }
        #expect(session.phase == .connecting)
        await discovery.set(.success(connection("B")))
        await session.connect()
        try await eventually { session.phase == .locked }
        let epoch = session.sessionEpoch
        await blocked.release(.success(.init(authRequired: true, authenticated: true)))
        await Task.yield()
        #expect(session.phase == .locked)
        #expect(session.sessionEpoch == epoch)
        #expect(factory.clients.count == 2)
        session.stopMonitoring()
    }

    @Test func lossClearsProtectedStateAndSameGenerationReconnectRequiresFreshAuthentication() async throws {
        let discovery = LifecycleDiscovery()
        let factory = LifecycleFactory()
        let session = makeSession(discovery, factory)
        await session.connect()
        try await eventually { session.phase == .locked }
        session.passphrase = "correct"
        await session.login()
        #expect(session.phase == .ready)
        session.route = .search
        session.passphrase = "must disappear"
        session.showsKeyboardShortcuts = true
        session.updateSearchInspectorCommandState(hasInspector: true, isPresented: true, commandsBlocked: false)
        session.focusSearch()
        let epoch = session.sessionEpoch
        await discovery.set(.failure(ShellControlError.unavailable("test loss")))
        await session.connect()
        #expect(session.api == nil)
        #expect(session.connection == nil)
        #expect(!session.acceptsReadyCommands)
        #expect(session.passphrase.isEmpty)
        #expect(!session.searchHasInspector && !session.showsKeyboardShortcuts)
        #expect(session.consumedSearchFocusRequestID == session.searchFocusRequestID)
        #expect(session.sessionEpoch > epoch)
        await discovery.set(.success(connection("A")))
        await session.connect()
        try await eventually { session.phase == .locked }
        #expect(session.route == .search)
        #expect(factory.clients.count == 2)
        await factory.unauthorized[0]()
        #expect(session.phase == .locked)
        session.passphrase = "correct"
        await session.login()
        #expect(session.phase == .ready)
        await factory.unauthorized[0]()
        #expect(session.phase == .ready)
        session.stopMonitoring()
    }

    @Test func wrongPasswordIsVisibleAndSuccessfulLoginRequiresCookieAuthentication() async throws {
        let discovery = LifecycleDiscovery()
        let factory = LifecycleFactory()
        let session = makeSession(discovery, factory)
        await session.connect()
        try await eventually { session.phase == .locked }
        session.passphrase = "wrong"
        await session.login()
        #expect(session.phase == .locked)
        #expect(session.loginError == "wrong passphrase")
        #expect(session.loginFailureID == 1)
        #expect(session.passphrase.isEmpty && !session.isLoggingIn)
        await factory.clients[0].setAcceptCookie(false)
        session.passphrase = "correct"
        await session.login()
        #expect(session.phase == .locked)
        #expect(session.loginFailureID == 2)
        await factory.clients[0].setAcceptCookie(true)
        session.passphrase = "correct"
        await session.login()
        #expect(session.phase == .ready)
        #expect(session.loginError == nil)
        session.stopMonitoring()
    }

    @Test func pendingLoginCannotUndoLockOrReplaceNewSession() async throws {
        let login = LifecycleGate<LoginResult>()
        let lock = LifecycleGate<Void>()
        let discovery = LifecycleDiscovery()
        let factory = LifecycleFactory(loginGates: [login], lockGates: [lock])
        let session = makeSession(discovery, factory)
        await session.connect()
        try await eventually { session.phase == .locked }
        session.passphrase = "correct"
        let oldLogin = Task { await session.login() }
        try await eventually { await factory.clients[0].loginCalls == 1 }
        let locking = Task { await session.lock() }
        try await eventually { await factory.clients[0].lockCalls == 1 }
        #expect(session.phase == .locked && session.api == nil)
        #expect(!session.canAttemptLogin)
        #expect(!session.isLoggingIn && !session.acceptsReadyCommands)
        await login.release(.success(.init(ok: true, error: nil)))
        await oldLogin.value
        #expect(session.phase == .locked && session.api == nil)
        #expect(!session.canAttemptLogin)
        await lock.release(.success(()))
        await locking.value
        try await eventually { session.phase == .locked && factory.clients.count == 2 }
        #expect(session.loginError == nil)
        session.stopMonitoring()
    }

    @Test func stoppingDuringLockFencesOldCompletionAndAllowsNewLifecycle() async throws {
        let lock = LifecycleGate<Void>()
        let discovery = LifecycleDiscovery()
        let factory = LifecycleFactory(lockGates: [lock])
        let session = makeSession(discovery, factory)
        await session.connect()
        try await eventually { session.phase == .locked }
        let locking = Task { await session.lock() }
        try await eventually { await factory.clients[0].lockCalls == 1 }
        session.stopMonitoring()
        try await eventually { await factory.clients[0].invalidations > 0 }
        await session.connect()
        try await eventually { session.phase == .locked && factory.clients.count == 2 }
        session.passphrase = "correct"
        await session.login()
        let epoch = session.sessionEpoch
        await lock.release(.success(()))
        await locking.value
        #expect(session.phase == .ready)
        #expect(session.sessionEpoch == epoch)
        #expect(factory.clients.count == 2)
        session.stopMonitoring()
    }

    @Test func monitorStartsOnceAndStopFencesNoncooperativeDiscovery() async throws {
        let gate = LifecycleGate<ShellControlConnection>()
        let discovery = LifecycleDiscovery(gate: gate)
        let factory = LifecycleFactory()
        let session = makeSession(discovery, factory)
        session.startMonitoring()
        session.startMonitoring()
        try await eventually { await discovery.calls == 1 }
        session.stopMonitoring()
        await gate.release(.success(connection("A")))
        for _ in 0..<30 { await Task.yield() }
        #expect(factory.clients.isEmpty)
        #expect(session.api == nil)
        #expect(session.phase == .connecting)
    }

    @Test func originAndStartupProofChangesRetireSameGenerationSession() async throws {
        let discovery = LifecycleDiscovery()
        let factory = LifecycleFactory()
        let session = makeSession(discovery, factory)
        await session.connect()
        try await eventually { session.phase == .locked }
        for found in [
            connection("A", port: 49153),
            connection("A", port: 49153, proof: String(repeating: "a", count: 64)),
        ] {
            let epoch = session.sessionEpoch
            let previous = factory.clients.last!
            await discovery.set(.success(found))
            await session.connect()
            try await eventually { session.phase == .locked }
            #expect(session.sessionEpoch > epoch)
            #expect(await previous.invalidations == 1)
            #expect(session.connection == found)
        }
        #expect(factory.clients.count == 3)
        session.stopMonitoring()
    }

    @Test func existentialInvalidationTerminatesRealClientBeforeRequestsOrStreams() async throws {
        let api: any SAGEAPI = SAGEAPIClient(baseURL: URL(string: "http://127.0.0.1:0")!)
        await api.invalidate()
        do {
            _ = try await api.authStatus()
            Issue.record("A retired client accepted a request")
        } catch is CancellationError { }
        do {
            for try await _ in await api.events() { Issue.record("A retired client emitted a stream item") }
            Issue.record("A retired client accepted a stream")
        } catch is CancellationError { }
    }

    @Test func lifecycleDiscoveryRejectsUnboundedOrNonpositiveTimeouts() async {
        for timeout in [Duration.zero, .seconds(-1), .seconds(3)] {
            do {
                _ = try await ShellControlClient.discoverConnection(environment: [:], timeout: timeout)
                Issue.record("An invalid control timeout was accepted")
            } catch ShellControlError.invalidTimeout { }
            catch { Issue.record("Invalid timeout reached socket discovery: \(error)") }
        }
    }

    private func makeSession(_ discovery: LifecycleDiscovery, _ factory: LifecycleFactory) -> AppSession {
        AppSession(discover: { try await discovery.read() }, makeClient: { _, unauthorized in
            factory.make(unauthorized: unauthorized)
        }, sleep: { try await Task.sleep(for: .seconds(30)) })
    }
}

@MainActor
private func eventually(_ condition: () async -> Bool) async throws {
    let deadline = ContinuousClock.now.advanced(by: .seconds(3))
    while !(await condition()) {
        guard ContinuousClock.now < deadline else {
            Issue.record("Timed out waiting for lifecycle condition")
            throw LifecycleTestError.timeout
        }
        try await Task.sleep(for: .milliseconds(1))
    }
}

private enum LifecycleTestError: Error { case timeout, unused }

private func connection(_ generation: String, port: Int = 49152, proof: String? = nil) -> ShellControlConnection {
    let origin = URL(string: "http://127.0.0.1:\(port)")!
    return .init(status: .init(controlProtocol: 1, daemonVersion: "12.0.0-beta.1", apiSchema: 1,
                              minimumShellProtocol: 1, maximumShellProtocol: 1,
                              instanceGeneration: String(repeating: generation, count: 42) + "A",
                              state: .ready, uiOrigin: origin.absoluteString, startupProof: proof), origin: origin)
}

private actor LifecycleGate<Value: Sendable> {
    private var result: Result<Value, Error>?
    private var waiters: [CheckedContinuation<Value, Error>] = []
    func wait() async throws -> Value {
        if let result { return try result.get() }
        return try await withCheckedThrowingContinuation { waiters.append($0) }
    }
    func release(_ result: Result<Value, Error>) {
        self.result = result
        for waiter in waiters { waiter.resume(with: result) }
        waiters.removeAll()
    }
}

private actor LifecycleDiscovery {
    private var result: Result<ShellControlConnection, Error> = .success(connection("A"))
    private var gate: LifecycleGate<ShellControlConnection>?
    private(set) var calls = 0
    init(gate: LifecycleGate<ShellControlConnection>? = nil) { self.gate = gate }
    func read() async throws -> ShellControlConnection {
        calls += 1
        if let gate { return try await gate.wait() }
        return try result.get()
    }
    func set(_ result: Result<ShellControlConnection, Error>) { self.result = result; gate = nil }
}

@MainActor
private final class LifecycleFactory {
    var clients: [LifecycleAPI] = []
    var unauthorized: [@Sendable () async -> Void] = []
    var authGates: [LifecycleGate<AuthStatus>]
    var loginGates: [LifecycleGate<LoginResult>]
    var lockGates: [LifecycleGate<Void>]
    init(authGates: [LifecycleGate<AuthStatus>] = [], loginGates: [LifecycleGate<LoginResult>] = [],
         lockGates: [LifecycleGate<Void>] = []) {
        self.authGates = authGates; self.loginGates = loginGates; self.lockGates = lockGates
    }
    func make(unauthorized: @escaping @Sendable () async -> Void) -> LifecycleAPI {
        let client = LifecycleAPI(authGate: authGates.isEmpty ? nil : authGates.removeFirst(),
                                  loginGate: loginGates.isEmpty ? nil : loginGates.removeFirst(),
                                  lockGate: lockGates.isEmpty ? nil : lockGates.removeFirst())
        clients.append(client)
        self.unauthorized.append(unauthorized)
        return client
    }
}

private actor LifecycleAPI: SAGEAPI {
    let authGate: LifecycleGate<AuthStatus>?
    let loginGate: LifecycleGate<LoginResult>?
    let lockGate: LifecycleGate<Void>?
    private var authenticated = false
    private var acceptCookie = true
    private(set) var invalidations = 0
    private(set) var authCalls = 0
    private(set) var loginCalls = 0
    private(set) var lockCalls = 0
    init(authGate: LifecycleGate<AuthStatus>?, loginGate: LifecycleGate<LoginResult>?, lockGate: LifecycleGate<Void>?) {
        self.authGate = authGate; self.loginGate = loginGate; self.lockGate = lockGate
    }
    func setAcceptCookie(_ value: Bool) { acceptCookie = value }
    func invalidate() async { invalidations += 1; authenticated = false }
    func authStatus() async throws -> AuthStatus {
        authCalls += 1
        if let authGate { return try await authGate.wait() }
        return .init(authRequired: true, authenticated: authenticated)
    }
    func login(passphrase: String) async throws -> LoginResult {
        loginCalls += 1
        if let loginGate { return try await loginGate.wait() }
        guard passphrase == "correct" else { throw SAGEAPIError.server(status: 401, message: "wrong passphrase") }
        authenticated = acceptCookie
        return .init(ok: true, error: nil)
    }
    func lock() async throws {
        lockCalls += 1
        authenticated = false
        if let lockGate { try await lockGate.wait() }
    }
    func health() async throws -> DashboardHealth { throw LifecycleTestError.unused }
    func stats() async throws -> DashboardStats { throw LifecycleTestError.unused }
    func agents() async throws -> AgentOverviewEnvelope { throw LifecycleTestError.unused }
    func validators() async throws -> ValidatorOverview { throw LifecycleTestError.unused }
    func federation() async throws -> FederationOverview { throw LifecycleTestError.unused }
    func memories(_ query: MemoryListQuery) async throws -> MemoryListEnvelope { throw LifecycleTestError.unused }
    func tags() async throws -> TagEnvelope { throw LifecycleTestError.unused }
    func memoryTags(id: String) async throws -> MemoryTagsEnvelope { throw LifecycleTestError.unused }
    func setMemoryTags(id: String, tags: [String]) async throws -> MemoryTagsEnvelope { throw LifecycleTestError.unused }
    func addTag(_ tag: String, to ids: [String]) async throws -> BulkMemoryUpdateResponse { throw LifecycleTestError.unused }
    func forgetMemory(id: String) async throws -> MemoryMutationResponse { throw LifecycleTestError.unused }
    func brainGraph(_ query: BrainGraphQuery) async throws -> BrainGraphEnvelope { throw LifecycleTestError.unused }
    func connectome() async throws -> ConnectomeEnvelope { throw LifecycleTestError.unused }
    func agentEngrams(agentID: String) async throws -> AgentEngramEnvelope { throw LifecycleTestError.unused }
    func relatedMemories(memoryID: String, limit: Int) async throws -> RelatedMemoryEnvelope { throw LifecycleTestError.unused }
    func events() async -> AsyncThrowingStream<DashboardEventStreamElement, Error> { .init { $0.finish() } }
}
