// Compiled with the production Foundation transport sources, without DEBUG.
// This executable is test tooling; it is never part of the native application.
import Foundation

struct EventTranscript: Codable, Sendable {
    let states: [String]
    let events: [String]
    let unauthorized_count: Int
    let failure: String?
}

actor ProbeTranscript {
    var states: [String] = []
    var events: [String] = []
    var unauthorizedCount = 0
    var failure: String?
    private(set) var finished = false

    func finish() { finished = true }
    func unauthorized() { unauthorizedCount += 1 }
    func record(_ element: DashboardEventStreamElement) {
        switch element {
        case let .state(state): states.append(String(describing: state))
        case let .event(event): events.append(event.name)
        }
    }
    func fail(_ error: Error) { failure = error.localizedDescription }
    func snapshot() -> EventTranscript {
        EventTranscript(states: states, events: events, unauthorized_count: unauthorizedCount, failure: failure)
    }
}

actor LockProbeState {
    private(set) var completed = false
    private(set) var cancelled = false
    func finish(cancelled: Bool) { self.cancelled = cancelled; completed = true }
}

@main struct NativeTransportProbe {
    static func emit(_ value: [String: Any]) throws {
        let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        print(String(decoding: data, as: UTF8.self))
    }

    static func waitForLockFixture(_ condition: @escaping @Sendable () async -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(3))
        while !(await condition()) {
            guard ContinuousClock.now < deadline else {
                throw SAGEAPIError.server(status: 0, message: "Timed out waiting for delayed-lock fixture.")
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    static func main() async {
        var operation = "arguments"
        do {
            let args = Array(CommandLine.arguments.dropFirst())
            guard let command = args.first else { throw SAGEAPIError.invalidResponse }
            operation = command
            switch command {
            case "decode-health":
                let health = try JSONDecoder.sageDashboard().decode(DashboardHealth.self, from: FileHandle.standardInput.readDataToEndOfFile())
                try emit(["ok": true, "block_time_decoded": health.chain?.blockTime != nil])
            case "validate":
                let status = try JSONDecoder().decode(ShellControlStatus.self, from: FileHandle.standardInput.readDataToEndOfFile())
                try ShellControlClient.validate(status)
                try emit(["ok": true])
            case "discover":
                let home = URL(fileURLWithPath: args[1], isDirectory: true)
                let start = ContinuousClock.now
                let origin = try await ShellControlClient.discoverAPIOrigin(sageHome: home, environment: [:])
                let elapsed = start.duration(to: .now).components
                try emit(["ok": true, "origin": origin.absoluteString,
                          "elapsed_seconds": Double(elapsed.seconds) + Double(elapsed.attoseconds) / 1e18])
            default:
                guard let origin = URL(string: args[1]), SAGEAPIClient.isSafeLoopback(origin) else {
                    throw ShellControlError.unsafeOrigin
                }
                let transcript = ProbeTranscript()
                let client = SAGEAPIClient(baseURL: origin, onUnauthorized: { await transcript.unauthorized() })
                switch command {
                case "memory-tags":
                    let memoryID = args[2]
                    let before = try await client.memoryTags(id: memoryID)
                    let replaced = try await client.setMemoryTags(id: memoryID, tags: ["native-fixture"])
                    let bulk = try await client.addTag("wire-qualified", to: [memoryID])
                    let after = try await client.memoryTags(id: memoryID)
                    let related = try await client.relatedMemories(memoryID: memoryID)
                    try emit(["ok": true, "before": before.tags, "replaced": replaced.tags,
                              "bulk_updated": bulk.updated, "after": after.tags,
                              "related": related.related.count])
                case "federation":
                    let value = try await client.federation()
                    try emit(["ok": true, "enabled": value.isEnabled, "connections": value.connections.count])
                case "health":
                    let health = try await client.health()
                    try emit(["ok": true, "version": health.version])
                case "unauthorized":
                    do {
                        _ = try await client.health()
                        throw SAGEAPIError.invalidResponse
                    } catch SAGEAPIError.unauthorized {
                        try emit(["ok": true, "unauthorized_count": await transcript.unauthorizedCount])
                    }
                case "cookies":
                    let before = try await client.authStatus()
                    let login = try await client.login(passphrase: "fixture-passphrase")
                    let after = try await client.authStatus()
                    let other = try await SAGEAPIClient(baseURL: origin).authStatus()
                    try await client.lock()
                    var oldClientInvalidated = false
                    do { _ = try await client.authStatus() }
                    catch is CancellationError { oldClientInvalidated = true }
                    let locked = try await SAGEAPIClient(baseURL: origin).authStatus()
                    try emit(["ok": true, "before": before.authenticated, "login": login.ok,
                              "after": after.authenticated, "other_session": other.authenticated,
                              "locked": locked.authenticated, "old_client_invalidated": oldClientInvalidated])
                case "delayed-lock":
                    let marker = URL(fileURLWithPath: args[2])
                    let login = try await client.login(passphrase: "fixture-passphrase")
                    guard login.ok else { throw SAGEAPIError.invalidResponse }
                    let reader = Task {
                        do {
                            for try await element in await client.events() {
                                await transcript.record(element)
                            }
                        } catch { await transcript.fail(error) }
                        await transcript.finish()
                    }
                    defer { reader.cancel() }
                    try await waitForLockFixture { await transcript.events.contains("consensus") }
                    let lockState = LockProbeState()
                    let locking = Task {
                        do {
                            try await client.lock()
                            await lockState.finish(cancelled: false)
                        } catch {
                            let cancelled = error is CancellationError || (error as? URLError)?.code == .cancelled
                            await lockState.finish(cancelled: cancelled)
                        }
                    }
                    defer { locking.cancel() }
                    // The HTTP fixture writes this only after it receives the
                    // authenticated POST, then withholds the response entirely.
                    try await waitForLockFixture { FileManager.default.fileExists(atPath: marker.path) }
                    var oldHTTPCancelled = false
                    do { _ = try await client.health() }
                    catch is CancellationError { oldHTTPCancelled = true }
                    try await waitForLockFixture { await transcript.finished }
                    let streamFinished = await transcript.finished
                    let lockPending = !(await lockState.completed)
                    let start = ContinuousClock.now
                    await client.invalidate()
                    try await waitForLockFixture { await lockState.completed }
                    await locking.value
                    await reader.value
                    let elapsed = start.duration(to: .now).components
                    try emit(["ok": true, "old_http_cancelled": oldHTTPCancelled,
                              "stream_finished_while_lock_pending": streamFinished && lockPending,
                              "lock_pending_before_invalidate": lockPending,
                              "revocation_cancelled": await lockState.cancelled,
                              "unauthorized_count": await transcript.unauthorizedCount,
                              "invalidation_seconds": Double(elapsed.seconds) + Double(elapsed.attoseconds) / 1e18])
                case "overview":
                    operation = "overview/auth"
                    let auth = try await client.authStatus()
                    operation = "overview/health"
                    let health = try await client.health()
                    operation = "overview/stats"
                    let stats = try await client.stats()
                    operation = "overview/agents"
                    let agents = try await client.agents()
                    operation = "overview/validators"
                    let validators = try await client.validators()
                    operation = "overview/federation"
                    let federation = try await client.federation()
                    operation = "overview/memories"
                    let memories = try await client.memories(.init())
                    operation = "overview/tags"
                    let tags = try await client.tags()
                    operation = "overview/graph"
                    let graph = try await client.brainGraph(.init())
                    operation = "overview/synapses"
                    let synapses = try await client.connectome()
                    try emit(["ok": true, "version": health.version, "app_version": health.chain?.appVersion ?? "",
                              "auth_required": auth.authRequired, "memories": memories.total,
                              "stats_memories": stats.totalMemories, "agents": agents.agents.count,
                              "validators": validators.count, "federation_enabled": federation.isEnabled,
                              "tags": tags.tags.count, "graph_nodes": graph.nodes.count,
                              "connectome_neurons": synapses.neurons.count])
                case "events":
                    let reader = Task {
                        do {
                            for try await element in await client.events() {
                                await transcript.record(element)
                                if args.count > 3, case .state(.connected) = element {
                                    try Data("connected".utf8).write(to: URL(fileURLWithPath: args[3]))
                                }
                            }
                        } catch { await transcript.fail(error) }
                    }
                    try await Task.sleep(for: .milliseconds(Int64(args[2])!))
                    reader.cancel()
                    await reader.value
                    let snapshot = await transcript.snapshot()
                    var result = try JSONSerialization.jsonObject(with: JSONEncoder().encode(snapshot)) as! [String: Any]
                    result["ok"] = true
                    try emit(result)
                default: throw SAGEAPIError.invalidResponse
                }
            }
        } catch {
            // Expected transport failures must return normally, never crash.
            try? emit(["ok": false, "operation": operation, "error": error.localizedDescription,
                       "detail": String(describing: error)])
        }
    }
}
