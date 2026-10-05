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
            if oldValue != route { clearBrainCommandRegistration() }
            if oldValue == .search, route != .search { clearSearchInspectorCommandState() }
            CerebrumNativeMenuCoordinator.shared.refresh()
        }
    }
    var phase: Phase = .connecting {
        didSet {
            if phase != .ready { clearBrainCommandRegistration() }
            CerebrumNativeMenuCoordinator.shared.refresh()
        }
    }
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
    var showsKeyboardShortcuts = false {
        didSet { CerebrumNativeMenuCoordinator.shared.refresh() }
    }
    var api: (any SAGEAPI)? {
        didSet { CerebrumNativeMenuCoordinator.shared.refresh() }
    }
    private(set) var brainCommandOwner: UUID?
    private(set) var brainCommandState: BrainCommandState?
    private(set) var brainCommandRequest: BrainCommandRequest?
    private var nextBrainCommandRequestID: UInt64 = 0

    func registerBrainCommands(owner: UUID, state: BrainCommandState) {
        defer { CerebrumNativeMenuCoordinator.shared.refresh() }
        guard acceptsRouteCommands(for: .brain) else { return }
        brainCommandOwner = owner
        brainCommandState = state
        brainCommandRequest = nil
    }

    func updateBrainCommands(owner: UUID, state: BrainCommandState) {
        defer { CerebrumNativeMenuCoordinator.shared.refresh() }
        guard brainCommandOwner == owner, acceptsRouteCommands(for: .brain) else { return }
        brainCommandState = state
    }

    func unregisterBrainCommands(owner: UUID) {
        guard brainCommandOwner == owner else { return }
        clearBrainCommandRegistration()
    }

    private func clearBrainCommandRegistration() {
        defer { CerebrumNativeMenuCoordinator.shared.refresh() }
        brainCommandOwner = nil
        brainCommandState = nil
        brainCommandRequest = nil
    }

    func requestBrainCommand(_ command: CerebrumCommandID, owner: UUID) {
        defer { CerebrumNativeMenuCoordinator.shared.refresh() }
        guard acceptsRouteCommands(for: .brain), !showsKeyboardShortcuts,
              brainCommandOwner == owner, brainCommandState?.allows(command) == true,
              brainCommandRequest == nil else { return }
        nextBrainCommandRequestID &+= 1
        brainCommandRequest = .init(id: nextBrainCommandRequestID, owner: owner, command: command)
    }

    func takeBrainCommand(owner: UUID, state: BrainCommandState) -> CerebrumCommandID? {
        defer { CerebrumNativeMenuCoordinator.shared.refresh() }
        guard let request = brainCommandRequest, request.owner == owner,
              brainCommandOwner == owner else { return nil }
        brainCommandRequest = nil
        guard acceptsRouteCommands(for: .brain), !showsKeyboardShortcuts,
              state.allows(request.command) else { return nil }
        return request.command
    }

    var acceptsReadyCommands: Bool { phase == .ready }

    func acceptsRouteCommands(for candidate: AppRoute) -> Bool {
        acceptsReadyCommands && api != nil && candidate.isImplemented && candidate == route
    }

    init() {}

    init(previewAPI: any SAGEAPI) {
        api = previewAPI
        phase = .ready
        #if DEBUG
        if let previewRoute = ProcessInfo.processInfo.environment["SAGE_NATIVE_PREVIEW_ROUTE"],
           let route = AppRoute(rawValue: previewRoute) {
            self.route = route
        }
        #endif
    }

    func connect() async {
        phase = .connecting
        do {
            let origin = try await ShellControlClient.discoverAPIOrigin()
            let api = SAGEAPIClient(baseURL: origin) { [weak self] in
                await self?.handleUnauthorized()
            }
            self.api = api
            let status = try await api.authStatus()
            phase = status.authRequired && !status.authenticated ? .locked : .ready
        } catch {
            phase = .failed(error.localizedDescription)
        }
    }

    func login() async {
        guard let api, !isLoggingIn else { return }
        let candidate = passphrase
        isLoggingIn = true
        loginError = nil
        defer { isLoggingIn = false }
        do {
            let result = try await api.login(passphrase: candidate)
            if result.ok {
                passphrase = ""
                phase = .ready
            } else {
                loginError = result.error ?? "The vault could not be unlocked."
                loginFailureID += 1
            }
        } catch {
            loginError = error.localizedDescription
            loginFailureID += 1
        }
    }

    func lock() async {
        guard let api else { return }
        do {
            try await api.lock()
            phase = .locked
        } catch {
            phase = .failed(error.localizedDescription)
        }
    }

    var acceptsGlobalCommands: Bool {
        acceptsReadyCommands && api != nil && !showsKeyboardShortcuts &&
            !(route == .brain && brainCommandState?.blocksGlobalCommands == true) &&
            !(route == .search && searchInspectorCommandsBlocked)
    }

    func navigate(to destination: AppRoute) {
        guard acceptsGlobalCommands, destination.isImplemented else { return }
        route = destination
    }

    func focusSearch() {
        guard acceptsGlobalCommands else { return }
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

    private func handleUnauthorized() {
        passphrase = ""
        loginError = nil
        phase = .locked
    }
}
