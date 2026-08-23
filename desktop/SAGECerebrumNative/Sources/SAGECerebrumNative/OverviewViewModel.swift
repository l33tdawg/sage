import Foundation
import Observation

@MainActor
@Observable
final class OverviewViewModel {
    var health: DashboardHealth?
    var stats: DashboardStats?
    var agents: AgentOverviewEnvelope?
    var validators: ValidatorOverview?
    var federation: FederationOverview?
    var healthIsStale = false
    var statsAreStale = false
    var agentsAreStale = false
    var validatorsAreStale = false
    var federationIsStale = false
    var isRefreshing = false
    var lastUpdated: Date?
    var lastEventAt: Date?
    var liveEventsConnected = false

    private let api: any SAGEAPI
    private var refreshGeneration = 0

    init(api: any SAGEAPI) {
        self.api = api
    }

    func refresh() async {
        refreshGeneration += 1
        let generation = refreshGeneration
        isRefreshing = true
        defer { if generation == refreshGeneration { isRefreshing = false } }

        async let nextHealth = capture { try await api.health() }
        async let nextStats = capture { try await api.stats() }
        async let nextAgents = capture { try await api.agents() }
        async let nextValidators = capture { try await api.validators() }
        async let nextFederation = capture { try await api.federation() }
        let results = await (nextHealth, nextStats, nextAgents, nextValidators, nextFederation)
        guard generation == refreshGeneration else { return }
        let (healthResult, statsResult, agentsResult, validatorsResult, federationResult) = results

        switch healthResult {
        case let .success(value):
            health = value
            healthIsStale = false
        case .failure:
            healthIsStale = true
        }
        switch statsResult {
        case let .success(value):
            stats = value
            statsAreStale = false
        case .failure:
            statsAreStale = true
        }
        switch agentsResult {
        case let .success(value):
            agents = value
            agentsAreStale = false
        case .failure:
            agentsAreStale = true
        }
        switch validatorsResult {
        case let .success(value):
            validators = value
            validatorsAreStale = false
        case .failure:
            validatorsAreStale = true
        }
        switch federationResult {
        case let .success(value):
            federation = value
            federationIsStale = false
        case .failure:
            federationIsStale = true
        }
        if [healthResult.isSuccess, statsResult.isSuccess, agentsResult.isSuccess,
            validatorsResult.isSuccess, federationResult.isSuccess].contains(true) {
            lastUpdated = .now
        }
    }

    func runLiveUpdates() async {
        await refresh()
        async let events: Void = consumeEvents()
        async let fallback: Void = pollFallback()
        _ = await (events, fallback)
    }

    private func consumeEvents() async {
        let stream = await api.events()
        do {
            for try await event in stream {
                if Task.isCancelled { break }
                await handleLiveEvent(event)
            }
        } catch {
            liveEventsConnected = false
        }
    }

    func handleLiveEvent(_ event: DashboardEvent) async {
        if event.name == "disconnected" {
            liveEventsConnected = false
            return
        }
        liveEventsConnected = true
        lastEventAt = event.receivedAt
        if event.name == "connected" { return }
        if event.name == "access" { purgeSensitiveSnapshots() }
        await refresh()
    }

    private func purgeSensitiveSnapshots() {
        refreshGeneration += 1
        health = nil
        stats = nil
        agents = nil
        validators = nil
        federation = nil
        healthIsStale = false
        statsAreStale = false
        agentsAreStale = false
        validatorsAreStale = false
        federationIsStale = false
        isRefreshing = false
        lastUpdated = nil
    }

    private func pollFallback() async {
        while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(30))
            if Task.isCancelled { break }
            await refresh()
        }
    }
}

private func capture<Value: Sendable>(
    _ operation: @Sendable () async throws -> Value
) async -> Result<Value, Error> {
    do {
        return .success(try await operation())
    } catch {
        return .failure(error)
    }
}

private extension Result {
    var isSuccess: Bool {
        if case .success = self { return true }
        return false
    }
}
