// Test-only release entry point. All session, menu, discovery and HTTP types
// below are the production source files, without mocks or DEBUG overrides.
import Foundation

@main
@MainActor
struct NativeSessionProbe {
    static func main() async throws {
        guard CommandLine.arguments.count == 2 else { throw ProbeFailure.invalidInput }
        let home = URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
        let session = AppSession(discover: {
            try await ShellControlClient.discoverConnection(sageHome: home, timeout: .seconds(1))
        })
        var retainedClient: (any SAGEAPI)?
        defer { session.stopMonitoring() }
        while let line = await Task.detached(operation: { readLine() }).value {
            guard let data = line.data(using: .utf8),
                  let command = try JSONSerialization.jsonObject(with: data) as? [String: String],
                  let operation = command["operation"] else { throw ProbeFailure.invalidInput }
            var result: [String: Any] = ["ok": true]
            switch operation {
            case "start":
                await session.connect()
                session.startMonitoring()
            case "snapshot": break
            case "route":
                guard let value = command["route"], let route = AppRoute(rawValue: value), route.isImplemented else {
                    throw ProbeFailure.invalidInput
                }
                session.route = route
            case "login":
                guard let passphrase = command["passphrase"] else { throw ProbeFailure.invalidInput }
                session.passphrase = passphrase
                await session.login()
            case "lock": await session.lock()
            case "retain-client":
                guard let api = session.api else { throw ProbeFailure.missingClient }
                retainedClient = api
            case "read", "retained-read":
                let api = operation == "read" ? session.api : retainedClient
                if let api {
                    do {
                        let health = try await api.health()
                        let memories = try await api.memories(MemoryListQuery())
                        result["read_succeeded"] = true
                        result["encrypted"] = health.encrypted
                        result["vault_locked"] = health.vaultLocked
                        result["app_version"] = health.chain?.appVersion ?? ""
                        result["fixture_memory_found"] = memories.memories.contains {
                            $0.id == command["memory_id"] && $0.content == command["content"]
                        }
                    } catch {
                        // Never print response bodies, credentials, or cookies.
                        result["read_succeeded"] = false
                    }
                } else { result["read_succeeded"] = false }
            case "finish":
                session.stopMonitoring()
                result["finished"] = true
            default: throw ProbeFailure.invalidInput
            }
            result["phase"] = switch session.phase {
            case .connecting: "connecting"
            case .ready: "ready"
            case .locked: "locked"
            case .failed: "failed"
            }
            result["has_api"] = session.api != nil
            result["ready_commands"] = session.acceptsReadyCommands
            result["route"] = session.route.rawValue
            result["epoch"] = session.sessionEpoch
            result["generation"] = session.connection?.status.instanceGeneration ?? ""
            result["login_error"] = session.loginError != nil
            result["passphrase_empty"] = session.passphrase.isEmpty
            let output = try JSONSerialization.data(withJSONObject: result, options: [.sortedKeys])
            FileHandle.standardOutput.write(output + Data([0x0A]))
            if operation == "finish" { return }
        }
        throw ProbeFailure.unexpectedEOF
    }

    enum ProbeFailure: Error { case invalidInput, missingClient, unexpectedEOF }
}
