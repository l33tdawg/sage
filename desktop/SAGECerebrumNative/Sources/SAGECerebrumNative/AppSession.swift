import Foundation
import Observation

@MainActor
@Observable
final class AppSession {
    enum Phase: Equatable {
        case connecting
        case ready
        case locked
        case failed(String)
    }

    var route: AppRoute = .overview {
        didSet {
            if oldValue == .search, route != .search { clearSearchInspectorCommandState() }
            CerebrumNativeMenuCoordinator.shared.refresh()
        }
    }
    var phase: Phase = .connecting
    var passphrase = ""
    var loginError: String?
    var isLoggingIn = false
    var loginFailureID = 0
    var searchFocusRequestID: UInt64 = 0
    var consumedSearchFocusRequestID: UInt64 = 0
    var searchInspectorToggleRequestID: UInt64 = 0
    var consumedSearchInspectorToggleRequestID: UInt64 = 0
    var searchHasInspector = false
    var searchInspectorIsPresented = false
    var searchInspectorCommandsBlocked = false
    var showsKeyboardShortcuts = false
    var api: (any SAGEAPI)?
    private(set) var connection: ShellControlConnection?
    private(set) var sessionEpoch: UInt64 = 0

    typealias Discovery = @Sendable () async throws -> ShellControlConnection
    typealias ClientFactory = @MainActor @Sendable (ShellControlConnection, @escaping @Sendable () async -> Void) async throws -> any SAGEAPI
    @ObservationIgnored private let discover: Discovery
    @ObservationIgnored private let makeClient: ClientFactory
    @ObservationIgnored private let sleep: @Sendable () async throws -> Void
    @ObservationIgnored private var monitorTask: Task<Void, Never>?
    @ObservationIgnored private var connectionTask: Task<Void, Never>?
    @ObservationIgnored private var authTask: Task<Void, Never>?
    @ObservationIgnored private var connectionAttempt: UUID?
    @ObservationIgnored private var clientID: UUID?
    @ObservationIgnored private var authOperation: UInt64 = 0
    @ObservationIgnored private var isLocking = false
    @ObservationIgnored private var lockAttempt: UUID?
    @ObservationIgnored private var lockingClient: (any SAGEAPI)?
    @ObservationIgnored private var isPreview = false

    var acceptsReadyCommands: Bool { phase == .ready && api != nil }
    var canAttemptLogin: Bool { phase == .locked && api != nil && !isLoggingIn && !isLocking }

    func acceptsRouteCommands(for candidate: AppRoute) -> Bool {
        acceptsReadyCommands && api != nil && candidate.isImplemented && candidate == route
    }

    init(
        discover: @escaping Discovery = { try await ShellControlClient.discoverConnection(timeout: .seconds(1)) },
        makeClient: @escaping ClientFactory = { connection, unauthorized in
            let credential = try await NativeBootstrapClient.bootstrap(connection: connection)
            return SAGEAPIClient(baseURL: connection.origin, nativeCredential: credential, onUnauthorized: unauthorized)
        },
        sleep: @escaping @Sendable () async throws -> Void = { try await Task.sleep(for: .seconds(1)) }
    ) {
        self.discover = discover
        self.makeClient = makeClient
        self.sleep = sleep
    }

    convenience init(previewAPI: any SAGEAPI) {
        self.init()
        isPreview = true
        api = previewAPI
        phase = .ready
        #if DEBUG
        if let previewRoute = ProcessInfo.processInfo.environment["SAGE_NATIVE_PREVIEW_ROUTE"],
           let route = AppRoute(rawValue: previewRoute) {
            self.route = route
        }
        #endif
    }

    // Lifecycle discovery uses a one-second deadline plus a one-second poll
    // interval: the two-second loss-detection budget excludes OS scheduling.
    // Raw SSCP keeps its two-second default. HTTP auth cannot block these polls.
    func startMonitoring() {
        guard !isPreview, monitorTask == nil else { return }
        monitorTask = Task { [weak self] in
            while !Task.isCancelled {
                guard let self else { return }
                await self.connect()
                do { try await self.sleep() } catch { return }
            }
        }
    }

    func stopMonitoring() {
        guard !isPreview else { return }
        monitorTask?.cancel()
        monitorTask = nil
        connectionTask?.cancel()
        connectionTask = nil
        connectionAttempt = nil
        let old = discardSession()
        let retired = lockingClient
        lockingClient = nil
        lockAttempt = nil
        isLocking = false
        phase = .connecting
        Task {
            await old?.invalidate()
            await retired?.invalidate()
        }
    }

    // Retry and monitor share a discovery attempt. Authentication continues in a
    // separate fenced task; callers observe phase for completion.
    func connect() async {
        guard !isPreview, !isLocking else { return }
        if let connectionTask { await connectionTask.value; return }
        let attempt = UUID()
        connectionAttempt = attempt
        let task = Task { [weak self] in
            guard let self else { return }
            await self.checkConnection(attempt: attempt)
        }
        connectionTask = task
        await task.value
        if connectionAttempt == attempt {
            connectionTask = nil
            connectionAttempt = nil
        }
    }

    private func checkConnection(attempt: UUID) async {
        do {
            let found = try await discover()
            guard connectionAttempt == attempt, !Task.isCancelled, !isLocking else { return }
            if let connection, api != nil, connection.hasSameIdentity(as: found) {
                self.connection = found
                return
            }
            let old = discardSession()
            phase = .connecting
            await old?.invalidate()
            guard connectionAttempt == attempt, !Task.isCancelled, !isLocking else { return }
            connection = found
            let id = UUID()
            clientID = id
            let newAPI = try await makeClient(found) { [weak self] in
                await self?.handleUnauthorized(client: id)
            }
            guard connectionAttempt == attempt, clientID == id, !Task.isCancelled, !isLocking else {
                await newAPI.invalidate()
                return
            }
            api = newAPI
            let epoch = sessionEpoch
            authTask = Task { [weak self] in
                do {
                    let status = try await newAPI.authStatus()
                    guard let self, self.clientID == id, self.sessionEpoch == epoch,
                          !Task.isCancelled else { return }
                    self.phase = status.authRequired && !status.authenticated ? .locked : .ready
                } catch {
                    guard let self, self.clientID == id, self.sessionEpoch == epoch,
                          !Task.isCancelled else { return }
                    let old = self.discardSession()
                    self.phase = .failed(error.localizedDescription)
                    await old?.invalidate()
                }
            }
        } catch {
            guard connectionAttempt == attempt, !Task.isCancelled, !isLocking else { return }
            let old = discardSession()
            phase = .failed(error.localizedDescription)
            await old?.invalidate()
        }
    }

    func login() async {
        guard let api, let id = clientID, phase == .locked, !isLoggingIn, !isLocking else { return }
        let candidate = passphrase
        passphrase = ""
        let epoch = sessionEpoch
        authOperation &+= 1
        let operation = authOperation
        isLoggingIn = true
        loginError = nil
        defer {
            if sessionEpoch == epoch, authOperation == operation { isLoggingIn = false }
        }
        do {
            let result = try await api.login(passphrase: candidate)
            guard sessionEpoch == epoch, clientID == id, authOperation == operation else { return }
            if result.ok {
                let status = try await api.authStatus()
                guard sessionEpoch == epoch, clientID == id, authOperation == operation else { return }
                if !status.authRequired || status.authenticated {
                    phase = .ready
                } else {
                    loginError = "The session could not be unlocked. Please try again."
                    loginFailureID += 1
                }
            } else {
                loginError = result.error ?? "The vault could not be unlocked."
                loginFailureID += 1
            }
        } catch {
            guard sessionEpoch == epoch, clientID == id, authOperation == operation else { return }
            loginError = error.localizedDescription
            loginFailureID += 1
        }
    }

    func lock() async {
        guard let old = api, !isLocking else { return }
        isLocking = true
        let operation = UUID()
        lockAttempt = operation
        lockingClient = old
        connectionTask?.cancel()
        connectionTask = nil
        connectionAttempt = nil
        _ = discardSession()
        let epoch = sessionEpoch
        phase = .locked
        // Hide protected content immediately. This revokes one dashboard session;
        // it does not relock the shared vault or stop agents.
        do { try await old.lock() } catch { /* Discard local credentials regardless. */ }
        await old.invalidate()
        guard sessionEpoch == epoch, lockAttempt == operation else { return }
        lockingClient = nil
        lockAttempt = nil
        isLocking = false
        await connect()
    }

    private func discardSession() -> (any SAGEAPI)? {
        let old = api
        authTask?.cancel()
        authTask = nil
        api = nil
        connection = nil
        clientID = nil
        sessionEpoch &+= 1
        authOperation &+= 1
        passphrase = ""
        loginError = nil
        isLoggingIn = false
        showsKeyboardShortcuts = false
        consumedSearchFocusRequestID = searchFocusRequestID
        consumedSearchInspectorToggleRequestID = searchInspectorToggleRequestID
        clearSearchInspectorCommandState()
        if !route.isImplemented { route = .overview }
        return old
    }

    func focusSearch() {
        guard acceptsReadyCommands, api != nil, !showsKeyboardShortcuts else { return }
        route = .search
        searchFocusRequestID &+= 1
    }

    func consumeSearchFocusRequest(_ requestID: UInt64) {
        guard requestID == searchFocusRequestID else { return }
        consumedSearchFocusRequestID = requestID
    }

    func updateSearchInspectorCommandState(hasInspector: Bool, isPresented: Bool, commandsBlocked: Bool) {
        searchHasInspector = hasInspector
        searchInspectorIsPresented = hasInspector && isPresented
        searchInspectorCommandsBlocked = commandsBlocked
        CerebrumNativeMenuCoordinator.shared.refresh()
    }

    func clearSearchInspectorCommandState() {
        searchHasInspector = false
        searchInspectorIsPresented = false
        searchInspectorCommandsBlocked = false
        CerebrumNativeMenuCoordinator.shared.refresh()
    }

    func requestSearchInspectorToggle() {
        guard acceptsRouteCommands(for: .search), searchHasInspector, !searchInspectorCommandsBlocked,
              !showsKeyboardShortcuts, searchInspectorToggleRequestID == consumedSearchInspectorToggleRequestID else { return }
        searchInspectorToggleRequestID &+= 1
        CerebrumNativeMenuCoordinator.shared.refresh()
    }

    func consumeSearchInspectorToggleRequest(_ requestID: UInt64) {
        guard requestID == searchInspectorToggleRequestID else { return }
        consumedSearchInspectorToggleRequestID = requestID
        CerebrumNativeMenuCoordinator.shared.refresh()
    }

    private func handleUnauthorized(client: UUID) async {
        guard clientID == client else { return }
        let old = discardSession()
        phase = .locked
        await old?.invalidate()
        // Fresh discovery/authentication happens on the monitor's next poll.
    }
}
