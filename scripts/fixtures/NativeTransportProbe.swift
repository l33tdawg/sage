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

@main struct NativeTransportProbe {
    static func emit(_ value: [String: Any]) throws {
        let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        print(String(decoding: data, as: UTF8.self))
    }

    static func main() async {
        do {
            let args = Array(CommandLine.arguments.dropFirst())
            guard let command = args.first else { throw SAGEAPIError.invalidResponse }
            switch command {
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
                    let locked = try await client.authStatus()
                    try emit(["ok": true, "before": before.authenticated, "login": login.ok,
                              "after": after.authenticated, "other_session": other.authenticated,
                              "locked": locked.authenticated])
                case "overview":
                    let auth = try await client.authStatus()
                    let health = try await client.health()
                    let stats = try await client.stats()
                    let agents = try await client.agents()
                    let validators = try await client.validators()
                    let federation = try await client.federation()
                    let memories = try await client.memories(.init())
                    let tags = try await client.tags()
                    let graph = try await client.brainGraph(.init())
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
            try? emit(["ok": false, "error": error.localizedDescription])
        }
    }
}
