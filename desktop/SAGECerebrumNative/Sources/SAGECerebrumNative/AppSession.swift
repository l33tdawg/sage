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

    var route: AppRoute = .overview
    var phase: Phase = .connecting
    var passphrase = ""
    var loginError: String?
    var isLoggingIn = false
    var loginFailureID = 0
    var searchFocusRequestID: UInt64 = 0
    var consumedSearchFocusRequestID: UInt64 = 0
    var showsKeyboardShortcuts = false
    var api: (any SAGEAPI)?

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

    func focusSearch() {
        guard acceptsReadyCommands, api != nil, !showsKeyboardShortcuts else { return }
        route = .search
        searchFocusRequestID &+= 1
    }

    func consumeSearchFocusRequest(_ requestID: UInt64) {
        guard requestID == searchFocusRequestID else { return }
        consumedSearchFocusRequestID = requestID
    }

    private func handleUnauthorized() {
        passphrase = ""
        loginError = nil
        phase = .locked
    }
}
