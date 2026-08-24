import Foundation
import Observation

@MainActor
@Observable
final class OverviewViewModel {
    private enum Source: Hashable { case health, stats, agents, validators, federation }

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
    var hasCompletedRefresh = false
    var lastUpdated: Date?
    var lastEventAt: Date?
    var eventStreamState: CerebrumEventStreamState = .connecting

    private let api: any SAGEAPI
    private var refreshGeneration = 0
    private var eventRefresh: Task<Void, Never>?
    private var sourceUpdatedAt: [Source: Date] = [:]

    init(api: any SAGEAPI) {
        self.api = api
    }

    var dataStatus: CerebrumDataStatus {
        let staleSources = [
            healthIsStale, statsAreStale, agentsAreStale,
            validatorsAreStale, federationIsStale,
        ].filter { $0 }.count
        let snapshot: CerebrumSnapshotState
        if let lastUpdated {
            let snapshotUpdatedAt = sourceUpdatedAt.values.min() ?? lastUpdated
            if staleSources == 5 {
                snapshot = .refreshFailed(updatedAt: snapshotUpdatedAt)
            } else if staleSources > 0 {
                snapshot = .partial(
                    updatedAt: snapshotUpdatedAt,
                    detail: "\(staleSources) of 5 sources not refreshed"
                )
            } else {
                snapshot = .available(updatedAt: snapshotUpdatedAt)
            }
        } else {
            snapshot = isRefreshing || !hasCompletedRefresh ? .loading : .unavailable
        }
        return .init(snapshot: snapshot, events: eventStreamState, isRefreshing: isRefreshing)
    }

    func refresh() async {
        refreshGeneration += 1
        let generation = refreshGeneration
        var didComplete = false
        isRefreshing = true
        defer {
            if generation == refreshGeneration {
                isRefreshing = false
                if didComplete { hasCompletedRefresh = true }
            }
        }

        async let nextHealth = capture { try await api.health() }
        async let nextStats = capture { try await api.stats() }
        async let nextAgents = capture { try await api.agents() }
        async let nextValidators = capture { try await api.validators() }
        async let nextFederation = capture { try await api.federation() }
        let results = await (nextHealth, nextStats, nextAgents, nextValidators, nextFederation)
        guard generation == refreshGeneration, !Task.isCancelled else { return }
        didComplete = true
        let (healthResult, statsResult, agentsResult, validatorsResult, federationResult) = results
        let refreshedAt = Date.now

        switch healthResult {
        case let .success(value):
            health = value
            healthIsStale = false
            sourceUpdatedAt[.health] = refreshedAt
        case .failure:
            healthIsStale = true
        }
        switch statsResult {
        case let .success(value):
            stats = value
            statsAreStale = false
            sourceUpdatedAt[.stats] = refreshedAt
        case .failure:
            statsAreStale = true
        }
        switch agentsResult {
        case let .success(value):
            agents = value
            agentsAreStale = false
            sourceUpdatedAt[.agents] = refreshedAt
        case .failure:
            agentsAreStale = true
        }
        switch validatorsResult {
        case let .success(value):
            validators = value
            validatorsAreStale = false
            sourceUpdatedAt[.validators] = refreshedAt
        case .failure:
            validatorsAreStale = true
        }
        switch federationResult {
        case let .success(value):
            federation = value
            federationIsStale = false
            sourceUpdatedAt[.federation] = refreshedAt
        case .failure:
            federationIsStale = true
        }
        if [healthResult.isSuccess, statsResult.isSuccess, agentsResult.isSuccess,
            validatorsResult.isSuccess, federationResult.isSuccess].contains(true) {
            lastUpdated = refreshedAt
        }
    }

    func runLiveUpdates() async {
        defer {
            eventRefresh?.cancel()
            eventRefresh = nil
        }
        await refresh()
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await self.consumeEvents() }
            group.addTask { await self.pollScheduledRefresh() }
            _ = await group.next()
            group.cancelAll()
        }
    }

    private func consumeEvents() async {
        eventStreamState = .connecting
        let stream = await api.events()
        do {
            for try await element in stream {
                if Task.isCancelled { break }
                handleEventStreamElement(element)
            }
        } catch is CancellationError {
            return
        } catch SAGEAPIError.unauthorized {
            eventStreamState = .stopped
            purgeSensitiveSnapshots()
            return
        } catch {
            if !Task.isCancelled { eventStreamState = .reconnecting }
        }
        if !Task.isCancelled { eventStreamState = .reconnecting }
    }

    func handleEventStreamElement(_ element: DashboardEventStreamElement) {
        switch element {
        case let .state(state):
            eventStreamState = state
        case let .event(event):
            handleLiveEvent(event)
        }
    }

    func handleLiveEvent(_ event: DashboardEvent) {
        lastEventAt = event.receivedAt
        if event.name == "access" { purgeSensitiveSnapshots() }
        eventRefresh?.cancel()
        eventRefresh = Task { [weak self] in await self?.refresh() }
    }

    private func purgeSensitiveSnapshots() {
        refreshGeneration += 1
        eventRefresh?.cancel()
        eventRefresh = nil
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
        hasCompletedRefresh = false
        lastUpdated = nil
        lastEventAt = nil
        sourceUpdatedAt.removeAll()
    }

    private func pollScheduledRefresh() async {
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
