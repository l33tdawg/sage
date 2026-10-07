import Darwin
import Foundation

struct ShellControlStatus: Decodable, Equatable, Sendable {
    enum State: String, Decodable, Sendable {
        case starting, locked, ready, degraded, draining, failed
    }

    let controlProtocol: Int
    let daemonVersion: String
    let apiSchema: Int
    let minimumShellProtocol: Int
    let maximumShellProtocol: Int
    let instanceGeneration: String
    let state: State
    let uiOrigin: String?
    let startupProof: String?

    enum CodingKeys: String, CodingKey, CaseIterable {
        case controlProtocol = "control_protocol"
        case daemonVersion = "daemon_version"
        case apiSchema = "api_schema"
        case minimumShellProtocol = "min_shell_protocol"
        case maximumShellProtocol = "max_shell_protocol"
        case instanceGeneration = "instance_generation"
        case state
        case uiOrigin = "ui_origin"
        case startupProof = "startup_proof"
    }

    // SSCP/1 is a closed lifecycle contract. Future fields require negotiation.
    init(from decoder: Decoder) throws {
        let fields = try decoder.container(keyedBy: ControlField.self)
        let known = Set(CodingKeys.allCases.map(\.rawValue))
        guard fields.allKeys.allSatisfy({ known.contains($0.stringValue) }) else {
            throw ShellControlError.invalidFrame
        }
        let values = try decoder.container(keyedBy: CodingKeys.self)
        controlProtocol = try values.decode(Int.self, forKey: .controlProtocol)
        daemonVersion = try values.decode(String.self, forKey: .daemonVersion)
        apiSchema = try values.decode(Int.self, forKey: .apiSchema)
        minimumShellProtocol = try values.decode(Int.self, forKey: .minimumShellProtocol)
        maximumShellProtocol = try values.decode(Int.self, forKey: .maximumShellProtocol)
        instanceGeneration = try values.decode(String.self, forKey: .instanceGeneration)
        state = try values.decode(State.self, forKey: .state)
        uiOrigin = try values.decodeIfPresent(String.self, forKey: .uiOrigin)
        startupProof = try values.decodeIfPresent(String.self, forKey: .startupProof)
    }

    init(
        controlProtocol: Int, daemonVersion: String, apiSchema: Int,
        minimumShellProtocol: Int, maximumShellProtocol: Int, instanceGeneration: String,
        state: State, uiOrigin: String?, startupProof: String?
    ) {
        self.controlProtocol = controlProtocol
        self.daemonVersion = daemonVersion
        self.apiSchema = apiSchema
        self.minimumShellProtocol = minimumShellProtocol
        self.maximumShellProtocol = maximumShellProtocol
        self.instanceGeneration = instanceGeneration
        self.state = state
        self.uiOrigin = uiOrigin
        self.startupProof = startupProof
    }

    private struct ControlField: CodingKey {
        let stringValue: String
        let intValue: Int? = nil
        init?(stringValue: String) { self.stringValue = stringValue }
        init?(intValue: Int) { return nil }
    }

    var canServeNativeUI: Bool { state == .ready || state == .degraded }
}

struct ShellControlConnection: Equatable, Sendable {
    let status: ShellControlStatus
    let origin: URL

    func hasSameIdentity(as other: Self) -> Bool {
        status.instanceGeneration == other.status.instanceGeneration
            && status.startupProof == other.status.startupProof
            && origin == other.origin
    }
}

enum ShellControlError: LocalizedError, Sendable {
    case unavailable(String)
    case invalidTimeout
    case unsafeEndpoint
    case invalidFrame
    case incompatible
    case notReady(ShellControlStatus.State)
    case unsafeOrigin

    var errorDescription: String? {
        switch self {
        case let .unavailable(message): "SAGE control is unavailable: \(message)"
        case .invalidTimeout: "The SAGE control timeout must be greater than zero and at most two seconds."
        case .unsafeEndpoint: "The SAGE control socket failed its ownership or permission check."
        case .invalidFrame: "SAGE returned an invalid control frame."
        case .incompatible: "The running SAGE daemon is not compatible with this native app."
        case let .notReady(state): "The SAGE daemon is \(state.rawValue)."
        case .unsafeOrigin: "SAGE returned an unsafe native API origin."
        }
    }
}

enum ShellControlClient {
    private static let maximumFrameSize = 16 * 1024

    static func defaultSAGEHome(environment: [String: String] = ProcessInfo.processInfo.environment) -> URL {
        if let explicit = environment["SAGE_HOME"], !explicit.isEmpty {
            return URL(fileURLWithPath: explicit, isDirectory: true).standardizedFileURL
        }
        return FileManager.default.homeDirectoryForCurrentUser
            .appending(path: ".sage-v12-beta", directoryHint: .isDirectory)
    }

    static func discoverAPIOrigin(
        sageHome: URL = defaultSAGEHome(),
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) async throws -> URL {
        try await discoverConnection(sageHome: sageHome, environment: environment).origin
    }

    static func discoverConnection(
        sageHome: URL = defaultSAGEHome(),
        environment: [String: String] = ProcessInfo.processInfo.environment,
        timeout: Duration = .seconds(2)
    ) async throws -> ShellControlConnection {
        guard timeout > .zero, timeout <= .seconds(2) else { throw ShellControlError.invalidTimeout }
        #if DEBUG
        if let raw = environment["SAGE_API_URL"], let override = URL(string: raw) {
            guard SAGEAPIClient.isSafeLoopback(override) else { throw ShellControlError.unsafeOrigin }
            return ShellControlConnection(status: ShellControlStatus(
                controlProtocol: 1, daemonVersion: "12.0.0-beta.1", apiSchema: 1,
                minimumShellProtocol: 1, maximumShellProtocol: 1,
                instanceGeneration: String(repeating: "A", count: 43), state: .ready,
                uiOrigin: override.absoluteString, startupProof: nil
            ), origin: override)
        }
        #endif
        let request = Data(#"{"control_protocol":1,"shell_protocol":1,"operation":"status"}"#.utf8)
        let response = try await exchange(request: request, sageHome: sageHome, timeout: timeout)
        let status = try JSONDecoder().decode(ShellControlStatus.self, from: response)
        try validate(status)
        guard status.canServeNativeUI else { throw ShellControlError.notReady(status.state) }
        guard let raw = status.uiOrigin, let origin = URL(string: raw),
              SAGEAPIClient.isSafeLoopback(origin), (origin.path.isEmpty || origin.path == "/"),
              origin.query == nil, origin.fragment == nil
        else { throw ShellControlError.unsafeOrigin }
        return ShellControlConnection(status: status, origin: origin)
    }

    static func validate(_ status: ShellControlStatus) throws {
        guard status.controlProtocol == 1,
              status.apiSchema == 1,
              status.minimumShellProtocol > 0,
              status.minimumShellProtocol <= 1,
              status.maximumShellProtocol >= 1,
              status.minimumShellProtocol <= status.maximumShellProtocol,
              validGeneration(status.instanceGeneration),
              supportedDaemonVersion(status.daemonVersion),
              status.startupProof.map(validStartupProof) ?? true
        else { throw ShellControlError.incompatible }
        if status.canServeNativeUI {
            guard status.uiOrigin?.isEmpty == false else { throw ShellControlError.incompatible }
        } else if status.uiOrigin != nil {
            throw ShellControlError.incompatible
        }
    }

    private static func validGeneration(_ value: String) -> Bool {
        let alphabet = Set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8)
        let canonicalLast = Set("AEIMQUYcgkosw048".utf8)
        return value.count == 43
            && value.utf8.allSatisfy(alphabet.contains)
            && value.utf8.last.map(canonicalLast.contains) == true
    }

    private static func validStartupProof(_ value: String) -> Bool {
        value.utf8.count == 64 && value.utf8.allSatisfy { (48 ... 57).contains($0) || (97 ... 102).contains($0) }
    }

    private static func supportedDaemonVersion(_ raw: String) -> Bool {
        let value = raw.hasPrefix("v") ? String(raw.dropFirst()) : raw
        let build = value.split(separator: "+", maxSplits: 1, omittingEmptySubsequences: false)
        guard let core = build.first,
              build.count == 1 || validIdentifiers(build[1], prerelease: false)
        else { return false }
        let pieces = core.split(separator: "-", maxSplits: 1, omittingEmptySubsequences: false)
        guard let release = pieces.first,
              pieces.count == 1 || validIdentifiers(pieces[1], prerelease: true)
        else { return false }
        let numbers = release.split(separator: ".", omittingEmptySubsequences: false)
        guard numbers.count == 3,
              numbers.allSatisfy(validNumber),
              let major = Int(numbers[0]), let minor = Int(numbers[1])
        else { return false }
        if major == 11 { return (10 ... 19).contains(minor) }
        return major == 12 && minor == 0 && pieces.count == 2
            && pieces[1].split(separator: ".").first == "beta"
    }

    private static func validNumber(_ value: Substring) -> Bool {
        !value.isEmpty && value.utf8.allSatisfy { (48 ... 57).contains($0) }
            && (value.count == 1 || value.first != "0")
    }

    private static func validIdentifiers(_ value: Substring, prerelease: Bool) -> Bool {
        let identifiers = value.split(separator: ".", omittingEmptySubsequences: false)
        return identifiers.allSatisfy { identifier in
            guard !identifier.isEmpty, identifier.utf8.allSatisfy({
                (48 ... 57).contains($0) || (65 ... 90).contains($0) || (97 ... 122).contains($0) || $0 == 45
            }) else { return false }
            let numeric = identifier.utf8.allSatisfy { (48 ... 57).contains($0) }
            return !prerelease || !numeric || validNumber(identifier)
        }
    }

    // Shared framing only. SSCP/1 status and SSCP/2 bootstrap each retain their
    // own closed response decoder; this does not widen the status contract.
    static func exchange(request: Data, sageHome: URL, timeout: Duration = .seconds(2),
                         challengeResponse: (@Sendable (Data) throws -> Data)? = nil) async throws -> Data {
        guard timeout > .zero, timeout <= .seconds(2) else { throw ShellControlError.invalidTimeout }
        try Task.checkCancellation()
        let response = try await Task.detached(priority: .userInitiated) {
            try exchangeFrame(request: request, sageHome: sageHome, timeout: timeout, challengeResponse: challengeResponse)
        }.value
        try Task.checkCancellation()
        return response
    }

    private static func exchangeFrame(request: Data, sageHome: URL, timeout: Duration,
                                      challengeResponse: (@Sendable (Data) throws -> Data)?) throws -> Data {
        let deadline = ContinuousClock.now.advanced(by: timeout)
        let runDirectory = sageHome.appending(path: "run", directoryHint: .isDirectory)
        let endpoint = runDirectory.appending(path: "shell-control.sock")
        guard secureAttributes(at: runDirectory, expectedType: .typeDirectory, permissionsMask: 0o077),
              secureAttributes(at: endpoint, expectedType: .typeSocket, permissionsMask: 0o077)
        else { throw ShellControlError.unsafeEndpoint }

        let descriptor = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw posixError() }
        defer { Darwin.close(descriptor) }

        // A per-read timeout can be kept alive indefinitely by a trickled frame.
        // Nonblocking I/O shares one monotonic deadline across connect/write/read.
        guard fcntl(descriptor, F_SETFL, O_NONBLOCK) == 0 else { throw posixError() }
        var noSignal: Int32 = 1
        guard setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &noSignal,
                         socklen_t(MemoryLayout.size(ofValue: noSignal))) == 0
        else { throw posixError() }

        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        let pathBytes = Array(endpoint.path.utf8CString)
        let capacity = MemoryLayout.size(ofValue: address.sun_path)
        guard pathBytes.count <= capacity else {
            throw ShellControlError.unavailable("control socket path is too long")
        }
        withUnsafeMutablePointer(to: &address.sun_path) { pointer in
            pointer.withMemoryRebound(to: CChar.self, capacity: capacity) { destination in
                for index in pathBytes.indices { destination[index] = pathBytes[index] }
            }
        }
        let connected = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(descriptor, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        if connected != 0 {
            guard errno == EINPROGRESS else { throw posixError() }
            try waitUntilReady(descriptor, events: Int16(POLLOUT), deadline: deadline)
            var error: Int32 = 0
            var size = socklen_t(MemoryLayout.size(ofValue: error))
            guard getsockopt(descriptor, SOL_SOCKET, SO_ERROR, &error, &size) == 0 else { throw posixError() }
            guard error == 0 else { throw posixError(error) }
        }
        var peerUID: uid_t = 0
        var peerGID: gid_t = 0
        guard getpeereid(descriptor, &peerUID, &peerGID) == 0, peerUID == getuid() else {
            throw ShellControlError.unsafeEndpoint
        }

        try writeFrame(request, to: descriptor, deadline: deadline)
        let response = try readFrame(from: descriptor, deadline: deadline)
        guard let challengeResponse else { return response }
        // The server challenge must be answered on this exact connection. All
        // four frames share the original connect/write/read deadline.
        let proof = try challengeResponse(response)
        try writeFrame(proof, to: descriptor, deadline: deadline)
        return try readFrame(from: descriptor, deadline: deadline)
    }

    private static func secureAttributes(
        at url: URL,
        expectedType: FileAttributeType,
        permissionsMask: Int
    ) -> Bool {
        guard let attributes = try? FileManager.default.attributesOfItem(atPath: url.path),
              attributes[.type] as? FileAttributeType == expectedType,
              (attributes[.ownerAccountID] as? NSNumber)?.uint32Value == getuid(),
              let permissions = (attributes[.posixPermissions] as? NSNumber)?.intValue
        else { return false }
        return permissions & permissionsMask == 0
    }

    private static func writeFrame(_ payload: Data, to descriptor: Int32, deadline: ContinuousClock.Instant) throws {
        guard !payload.isEmpty, payload.count <= maximumFrameSize else { throw ShellControlError.invalidFrame }
        var size = UInt32(payload.count).bigEndian
        try withUnsafeBytes(of: &size) { try writeAll($0, to: descriptor, deadline: deadline) }
        try payload.withUnsafeBytes { try writeAll($0, to: descriptor, deadline: deadline) }
    }

    private static func readFrame(from descriptor: Int32, deadline: ContinuousClock.Instant) throws -> Data {
        var size: UInt32 = 0
        try withUnsafeMutableBytes(of: &size) { try readAll($0, from: descriptor, deadline: deadline) }
        let count = Int(UInt32(bigEndian: size))
        guard count > 0, count <= maximumFrameSize else { throw ShellControlError.invalidFrame }
        var payload = Data(count: count)
        try payload.withUnsafeMutableBytes { try readAll($0, from: descriptor, deadline: deadline) }
        return payload
    }

    private static func writeAll(_ bytes: UnsafeRawBufferPointer, to descriptor: Int32, deadline: ContinuousClock.Instant) throws {
        var offset = 0
        while offset < bytes.count {
            try waitUntilReady(descriptor, events: Int16(POLLOUT), deadline: deadline)
            let result = Darwin.write(descriptor, bytes.baseAddress!.advanced(by: offset), bytes.count - offset)
            if result < 0, errno == EINTR || errno == EAGAIN { continue }
            guard result > 0 else { throw posixError() }
            offset += result
        }
    }

    private static func readAll(_ bytes: UnsafeMutableRawBufferPointer, from descriptor: Int32, deadline: ContinuousClock.Instant) throws {
        var offset = 0
        while offset < bytes.count {
            try waitUntilReady(descriptor, events: Int16(POLLIN), deadline: deadline)
            let result = Darwin.read(descriptor, bytes.baseAddress!.advanced(by: offset), bytes.count - offset)
            if result == 0 { throw ShellControlError.invalidFrame }
            if result < 0, errno == EINTR || errno == EAGAIN { continue }
            guard result > 0 else { throw posixError() }
            offset += result
        }
    }

    private static func waitUntilReady(
        _ descriptor: Int32, events: Int16, deadline: ContinuousClock.Instant
    ) throws {
        while true {
            let remaining = ContinuousClock.now.duration(to: deadline)
            guard remaining > .zero else { throw posixError(ETIMEDOUT) }
            let parts = remaining.components
            let milliseconds = parts.seconds * 1_000 + (parts.attoseconds + 999_999_999_999_999) / 1_000_000_000_000_000
            var entry = pollfd(fd: descriptor, events: events, revents: 0)
            let result = Darwin.poll(&entry, 1, Int32(min(milliseconds, Int64(Int32.max))))
            if result < 0, errno == EINTR { continue }
            guard result >= 0 else { throw posixError() }
            guard result > 0 else { throw posixError(ETIMEDOUT) }
            if entry.revents & Int16(POLLNVAL) != 0 { throw posixError(EBADF) }
            // HUP/ERR must reach read/write so EOF and EPIPE become typed errors.
            return
        }
    }

    private static func posixError(_ code: Int32 = errno) -> ShellControlError {
        ShellControlError.unavailable(String(cString: strerror(code)))
    }
}
