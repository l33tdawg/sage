import Foundation
import Observation

enum MemoryOperationTone: Equatable, Sendable {
    case success
    case warning
    case error
}

@MainActor
@Observable
final class SearchViewModel {
    private struct SnapshotScope: Hashable {
        let query: String
        let domain: String
        let status: String
        let tag: String
        let agent: String
        let sort: String
        let datePreset: String
        let customFrom: Date?
        let customTo: Date?
    }

    var query = ""
    var domain = ""
    var status = "active"
    var tag = ""
    var agent = ""
    var sort: MemorySort = .newest
    var datePreset: MemoryDatePreset = .anytime
    var customFrom = Date.now.addingTimeInterval(-604_800)
    var customTo = Date.now

    var memories: [MemorySummary] = []
    var total = 0
    var nextCursor: String?
    var continuationRequired = false
    var authorLabels: [String: String] = [:]
    var projection: MemoryProjection?
    var agents: [AgentOverview] = []
    var domains: [String] = []
    var tags: [TagCount] = []
    var selection = Set<MemorySummary.ID>()
    var inspectedMemoryID: MemorySummary.ID?
    var inspectedTags: [String] = []
    var tagsAreLoading = false
    var tagsError: String?
    var newTag = ""
    var isMutating = false
    var operationMessage: String?
    var operationTone: MemoryOperationTone = .success
    var isLoading = false
    var isLoadingOlder = false
    var errorMessage: String?
    var eventStreamState: CerebrumEventStreamState = .connecting
    var hasCompletedRefresh: Bool {
        get { completedScopes.contains(currentScope) }
        set {
            if newValue { completedScopes.insert(currentScope) }
            else { completedScopes.remove(currentScope) }
        }
    }
    var lastUpdated: Date?
    var isStale: Bool {
        get { staleScopes.contains(currentScope) }
        set {
            if newValue { staleScopes.insert(currentScope) }
            else { staleScopes.remove(currentScope) }
        }
    }
    var updatesAvailable: Bool {
        get { pendingUpdateScopes.contains(currentScope) }
        set {
            if newValue { pendingUpdateScopes.insert(currentScope) }
            else { pendingUpdateScopes.remove(currentScope) }
        }
    }

    private let api: any SAGEAPI
    private var requestGeneration = 0
    private var tagRequestGeneration = 0
    private var metadataGeneration = 0
    private var lastSuccessfulScope: SnapshotScope?
    private var pendingUpdateScopes = Set<SnapshotScope>()
    private var completedScopes = Set<SnapshotScope>()
    private var staleScopes = Set<SnapshotScope>()
    private var eventRefresh: Task<Void, Never>?

    init(api: any SAGEAPI) { self.api = api }

    var dataStatus: CerebrumDataStatus {
        let snapshot: CerebrumSnapshotState
        if lastSuccessfulScope == currentScope, let lastUpdated {
            if isStale {
                snapshot = .refreshFailed(updatedAt: lastUpdated)
            } else if projection?.partial == true {
                snapshot = .partial(updatedAt: lastUpdated, detail: projection?.message)
            } else {
                snapshot = .available(updatedAt: lastUpdated)
            }
        } else {
            snapshot = isLoading || !hasCompletedRefresh ? .loading : .unavailable
        }
        return .init(
            snapshot: snapshot,
            events: eventStreamState,
            isRefreshing: isLoading,
            hasPendingUpdate: updatesAvailable
        )
    }

    var hasSnapshotForCurrentScope: Bool { lastSuccessfulScope == currentScope }

    var inspectedMemory: MemorySummary? {
        guard hasSnapshotForCurrentScope else { return nil }
        return memories.first { $0.id == inspectedMemoryID }
    }

    var activeFilterCount: Int {
        [domain, tag, agent].filter { !$0.isEmpty }.count
            + (status == "active" ? 0 : 1)
            + (datePreset == .anytime ? 0 : 1)
            + (sort == .newest ? 0 : 1)
    }

    func loadMetadata() async {
        metadataGeneration += 1
        let generation = metadataGeneration
        async let agentResult = captureSearch { try await api.agents() }
        async let statsResult = captureSearch { try await api.stats() }
        async let tagResult = captureSearch { try await api.tags() }
        let values = await (agentResult, statsResult, tagResult)
        guard generation == metadataGeneration else { return }
        if case let .success(value) = values.0 { agents = value.agents }
        if case let .success(value) = values.1 { domains = value.byDomain.keys.sorted() }
        if case let .success(value) = values.2 { tags = value.tags.sorted { $0.tag < $1.tag } }
    }

    func refresh() async {
        requestGeneration += 1
        let generation = requestGeneration
        let requestedScope = currentScope
        let requestedQuery = currentQuery()
        var didComplete = false
        isLoading = true
        errorMessage = nil
        defer {
            if generation == requestGeneration {
                isLoading = false
                if didComplete { hasCompletedRefresh = true }
            }
        }
        do {
            let response = try await api.memories(requestedQuery)
            guard generation == requestGeneration, currentScope == requestedScope,
                  !Task.isCancelled else { return }
            apply(response, append: false, scope: requestedScope)
            didComplete = true
        } catch is CancellationError {
            return
        } catch {
            guard generation == requestGeneration, currentScope == requestedScope,
                  !Task.isCancelled else { return }
            isStale = lastSuccessfulScope == requestedScope && lastUpdated != nil
            errorMessage = error.localizedDescription
            didComplete = true
        }
    }

    func loadOlder() async {
        guard let nextCursor, !nextCursor.isEmpty, !isLoadingOlder else { return }
        let generation = requestGeneration
        let requestedScope = currentScope
        isLoadingOlder = true
        defer { isLoadingOlder = false }
        do {
            var query = currentQuery()
            query.cursor = nextCursor
            let response = try await api.memories(query)
            guard generation == requestGeneration, currentScope == requestedScope,
                  self.nextCursor == nextCursor, !Task.isCancelled else { return }
            apply(response, append: true, scope: requestedScope)
        } catch is CancellationError {
            return
        } catch {
            guard generation == requestGeneration, currentScope == requestedScope,
                  !Task.isCancelled else { return }
            errorMessage = error.localizedDescription
        }
    }

    func loadInspectedTags() async {
        tagRequestGeneration += 1
        let generation = tagRequestGeneration
        guard let id = inspectedMemoryID else {
            inspectedTags = []
            tagsError = nil
            tagsAreLoading = false
            return
        }
        inspectedTags = []
        newTag = ""
        tagsAreLoading = true
        tagsError = nil
        defer {
            if generation == tagRequestGeneration, inspectedMemoryID == id {
                tagsAreLoading = false
            }
        }
        do {
            let response = try await api.memoryTags(id: id)
            guard generation == tagRequestGeneration, inspectedMemoryID == id else { return }
            inspectedTags = response.tags
        } catch {
            guard generation == tagRequestGeneration, inspectedMemoryID == id else { return }
            inspectedTags = []
            tagsError = error.localizedDescription
        }
    }

    func addInspectedTag() async {
        let normalized = Self.normalizeTag(newTag)
        guard !normalized.isEmpty, !inspectedTags.contains(normalized) else {
            newTag = ""
            return
        }
        guard inspectedTags.count < 32 else {
            tagsError = "A memory can have at most 32 tags."
            return
        }
        newTag = ""
        await saveInspectedTags(inspectedTags + [normalized])
    }

    func removeInspectedTag(_ tag: String) async {
        await saveInspectedTags(inspectedTags.filter { $0 != tag })
    }

    func addBulkTag(_ rawTag: String) async -> Bool {
        let tag = Self.normalizeTag(rawTag)
        let ids = Array(selection)
        guard !tag.isEmpty, !ids.isEmpty else { return false }
        isMutating = true
        defer { isMutating = false }
        do {
            let response = try await api.addTag(tag, to: ids)
            operationMessage = "Tagged \(response.updated) of \(response.total) selected memories with “\(tag)”."
            operationTone = response.updated < response.total ? .warning : .success
            if response.updated == response.total { selection.removeAll() }
            await refresh()
            return true
        } catch {
            operationMessage = "Couldn’t tag the selected memories: \(error.localizedDescription)"
            operationTone = .error
            return false
        }
    }

    func forget(ids: [String]) async {
        guard !ids.isEmpty else { return }
        isMutating = true
        defer { isMutating = false }
        var deprecatedIDs = Set<String>()
        var settling = 0
        var challenged = 0
        var failed = 0
        var stoppedForUncertainty = false
        var firstError: Error?
        for id in ids {
            do {
                let response = try await api.forgetMemory(id: id)
                switch response.status {
                case "deprecated": deprecatedIDs.insert(id)
                case "challenge_opened": challenged += 1
                case "consensus_submitted": settling += 1
                case "confirmation_pending":
                    settling += 1
                    stoppedForUncertainty = true
                default:
                    settling += 1
                }
                if stoppedForUncertainty { break }
            } catch {
                if firstError == nil { firstError = error }
                failed += 1
                if case let SAGEAPIError.server(status, _) = error, status == 503 {
                    stoppedForUncertainty = true
                    break
                }
            }
        }
        let attempted = deprecatedIDs.count + settling + challenged + failed
        let notAttempted = ids.count - attempted
        if failed == 0, settling == 0, challenged == 0, deprecatedIDs.count == ids.count {
            operationMessage = ids.count == 1 ? "Memory moved to audit-only history." : "\(deprecatedIDs.count) memories moved to audit-only history."
            operationTone = .success
        } else {
            var parts: [String] = []
            if !deprecatedIDs.isEmpty { parts.append("\(deprecatedIDs.count) deprecated") }
            if challenged > 0 { parts.append("\(challenged) awaiting another manager’s confirmation") }
            if settling > 0 { parts.append("\(settling) settling") }
            if failed > 0 { parts.append("\(failed) failed") }
            if notAttempted > 0 { parts.append("\(notAttempted) not attempted") }
            operationMessage = parts.joined(separator: ", ") + ". " +
                (stoppedForUncertainty ? "Further changes paused until signer state reconciles." : (firstError?.localizedDescription ?? "The list has been reconciled."))
            operationTone = failed > 0 ? .error : .warning
        }
        await refresh()
        selection.subtract(deprecatedIDs)
        if let inspectedMemoryID, deprecatedIDs.contains(inspectedMemoryID) { self.inspectedMemoryID = nil }
    }

    private func saveInspectedTags(_ updated: [String]) async {
        guard let id = inspectedMemoryID, !isMutating else { return }
        let previous = inspectedTags
        tagRequestGeneration += 1
        let generation = tagRequestGeneration
        inspectedTags = updated
        tagsError = nil
        isMutating = true
        defer { isMutating = false }
        do {
            _ = try await api.setMemoryTags(id: id, tags: updated)
            let canonical = try await api.memoryTags(id: id)
            guard generation == tagRequestGeneration, inspectedMemoryID == id else { return }
            inspectedTags = canonical.tags
            operationMessage = "Tags saved."
            operationTone = .success
        } catch {
            guard generation == tagRequestGeneration, inspectedMemoryID == id else { return }
            inspectedTags = previous
            tagsError = "Tags weren’t saved. Your previous tags were restored."
            operationMessage = "Couldn’t save tags: \(error.localizedDescription)"
            operationTone = .error
        }
    }

    nonisolated static func normalizeTag(_ input: String) -> String {
        let normalized = input.trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
            .replacingOccurrences(of: "[^a-z0-9_-]+", with: "-", options: .regularExpression)
            .trimmingCharacters(in: CharacterSet(charactersIn: "-_"))
        return String(normalized.prefix(128))
    }

    func resetFilters() {
        domain = ""
        status = "active"
        tag = ""
        agent = ""
        sort = .newest
        datePreset = .anytime
    }

    func runLiveUpdates() async {
        defer {
            eventRefresh?.cancel()
            eventRefresh = nil
        }
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await self.consumeEvents() }
            group.addTask { await self.pollScheduledRefresh() }
            _ = await group.next()
            group.cancelAll()
        }
    }

    private func currentQuery() -> MemoryListQuery {
        MemoryListQuery(
            text: query,
            domain: domain,
            status: status,
            tag: tag,
            agent: agent,
            from: lowerDateBound,
            to: upperDateBound,
            sort: sort,
            limit: 100
        )
    }

    private var currentScope: SnapshotScope {
        .init(
            query: query,
            domain: domain,
            status: status,
            tag: tag,
            agent: agent,
            sort: sort.rawValue,
            datePreset: datePreset.rawValue,
            customFrom: datePreset == .custom ? customFrom : nil,
            customTo: datePreset == .custom ? customTo : nil
        )
    }

    private func apply(_ response: MemoryListEnvelope, append: Bool, scope: SnapshotScope) {
        if append {
            let known = Set(memories.map(\.id))
            memories.append(contentsOf: response.memories.filter { !known.contains($0.id) })
        } else {
            memories = response.memories
        }
        total = response.total
        nextCursor = response.nextCursor
        continuationRequired = response.continuationRequired == true
        authorLabels.merge(response.authorLabels ?? [:]) { _, new in new }
        projection = response.projection
        lastSuccessfulScope = scope
        isStale = false
        updatesAvailable = false
        let visible = Set(memories.map(\.id))
        selection.formIntersection(visible)
        if let inspectedMemoryID, !visible.contains(inspectedMemoryID) { self.inspectedMemoryID = nil }
        lastUpdated = .now
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
            purgeSensitiveState()
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

    private func pollScheduledRefresh() async {
        while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(30))
            if Task.isCancelled { break }
            await refresh()
        }
    }

    func handleLiveEvent(_ event: DashboardEvent) {
        if event.name == "access" {
            purgeSensitiveState()
            eventRefresh = Task { [weak self] in
                guard let self else { return }
                async let content: Void = refresh()
                async let metadata: Void = loadMetadata()
                _ = await (content, metadata)
            }
        } else if selection.isEmpty {
            eventRefresh?.cancel()
            eventRefresh = Task { [weak self] in await self?.refresh() }
        }
        else { updatesAvailable = true }
    }

    private var lowerDateBound: Date? {
        if datePreset == .custom { return Calendar.current.startOfDay(for: customFrom) }
        return datePreset.lowerBound()
    }

    private var upperDateBound: Date? {
        guard datePreset == .custom,
              let nextDay = Calendar.current.date(byAdding: .day, value: 1, to: Calendar.current.startOfDay(for: customTo))
        else { return nil }
        return nextDay.addingTimeInterval(-1)
    }

    private func purgeSensitiveState() {
        requestGeneration += 1
        eventRefresh?.cancel()
        eventRefresh = nil
        tagRequestGeneration += 1
        metadataGeneration += 1
        memories = []
        total = 0
        nextCursor = nil
        continuationRequired = false
        authorLabels = [:]
        projection = nil
        agents = []
        domains = []
        tags = []
        selection.removeAll()
        inspectedMemoryID = nil
        inspectedTags = []
        newTag = ""
        tagsError = nil
        operationMessage = nil
        updatesAvailable = false
        isStale = false
        isLoading = false
        errorMessage = nil
        hasCompletedRefresh = false
        lastUpdated = nil
        lastSuccessfulScope = nil
        pendingUpdateScopes.removeAll()
        completedScopes.removeAll()
        staleScopes.removeAll()
    }
}

private func captureSearch<Value: Sendable>(
    _ operation: @Sendable () async throws -> Value
) async -> Result<Value, Error> {
    do { return .success(try await operation()) }
    catch { return .failure(error) }
}
