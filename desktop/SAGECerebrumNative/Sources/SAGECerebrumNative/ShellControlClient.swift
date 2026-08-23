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

    enum CodingKeys: String, CodingKey {
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

    var canServeNativeUI: Bool { state == .ready || state == .degraded }
}

enum ShellControlError: LocalizedError, Sendable {
    case unavailable(String)
    case unsafeEndpoint
    case invalidFrame
    case incompatible
    case notReady(ShellControlStatus.State)
    case unsafeOrigin

    var errorDescription: String? {
        switch self {
        case let .unavailable(message): "SAGE control is unavailable: \(message)"
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
        #if DEBUG
        if let raw = environment["SAGE_API_URL"], let override = URL(string: raw) {
            guard SAGEAPIClient.isSafeLoopback(override) else { throw ShellControlError.unsafeOrigin }
            return override
        }
        #endif

        let status: ShellControlStatus = try await Task.detached(priority: .userInitiated) {
            try readStatus(sageHome: sageHome)
        }.value
        guard status.canServeNativeUI else { throw ShellControlError.notReady(status.state) }
        guard let raw = status.uiOrigin, let origin = URL(string: raw),
              SAGEAPIClient.isSafeLoopback(origin), (origin.path.isEmpty || origin.path == "/"),
              origin.query == nil, origin.fragment == nil
        else { throw ShellControlError.unsafeOrigin }
        return origin
    }

    static func validate(_ status: ShellControlStatus) throws {
        guard status.controlProtocol == 1,
              status.apiSchema == 1,
              status.minimumShellProtocol <= 1,
              status.maximumShellProtocol >= 1,
              status.minimumShellProtocol <= status.maximumShellProtocol,
              validGeneration(status.instanceGeneration),
              supportedDaemonVersion(status.daemonVersion)
        else { throw ShellControlError.incompatible }
        if status.canServeNativeUI {
            guard status.uiOrigin?.isEmpty == false else { throw ShellControlError.incompatible }
        } else if status.uiOrigin?.isEmpty == false {
            throw ShellControlError.incompatible
        }
    }

    private static func validGeneration(_ value: String) -> Bool {
        let alphabet = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_"))
        let canonicalLast = CharacterSet(charactersIn: "AEIMQUYcgkosw048")
        return value.count == 43
            && value.unicodeScalars.allSatisfy(alphabet.contains)
            && value.unicodeScalars.last.map(canonicalLast.contains) == true
    }

    private static func supportedDaemonVersion(_ raw: String) -> Bool {
        let value = raw.hasPrefix("v") ? String(raw.dropFirst()) : raw
        let withoutBuild = value.split(separator: "+", maxSplits: 1).first.map(String.init) ?? value
        let pieces = withoutBuild.split(separator: "-", maxSplits: 1).map(String.init)
        let numbers = pieces[0].split(separator: ".", omittingEmptySubsequences: false)
        guard numbers.count == 3,
              numbers.allSatisfy({ !$0.isEmpty && $0.allSatisfy(\.isNumber) }),
              let major = Int(numbers[0]), let minor = Int(numbers[1])
        else { return false }
        if major == 11 { return (10 ... 19).contains(minor) }
        return major == 12 && minor == 0 && pieces.count == 2
            && pieces[1].split(separator: ".").first == "beta"
    }

    private static func readStatus(sageHome: URL) throws -> ShellControlStatus {
        let runDirectory = sageHome.appending(path: "run", directoryHint: .isDirectory)
        let endpoint = runDirectory.appending(path: "shell-control.sock")
        guard secureAttributes(at: runDirectory, expectedType: .typeDirectory, permissionsMask: 0o077),
              secureAttributes(at: endpoint, expectedType: .typeSocket, permissionsMask: 0o077)
        else { throw ShellControlError.unsafeEndpoint }

        let descriptor = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw posixError() }
        defer { Darwin.close(descriptor) }

        var timeout = timeval(tv_sec: 1, tv_usec: 0)
        setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout.size(ofValue: timeout)))
        setsockopt(descriptor, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout.size(ofValue: timeout)))

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
        guard connected == 0 else { throw posixError() }

        let request = Data(#"{"control_protocol":1,"shell_protocol":1,"operation":"status"}"#.utf8)
        try writeFrame(request, to: descriptor)
        let response = try readFrame(from: descriptor)
        let status = try JSONDecoder().decode(ShellControlStatus.self, from: response)
        try validate(status)
        return status
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

    private static func writeFrame(_ payload: Data, to descriptor: Int32) throws {
        guard !payload.isEmpty, payload.count <= maximumFrameSize else { throw ShellControlError.invalidFrame }
        var size = UInt32(payload.count).bigEndian
        try withUnsafeBytes(of: &size) { try writeAll($0, to: descriptor) }
        try payload.withUnsafeBytes { try writeAll($0, to: descriptor) }
    }

    private static func readFrame(from descriptor: Int32) throws -> Data {
        var size: UInt32 = 0
        try withUnsafeMutableBytes(of: &size) { try readAll($0, from: descriptor) }
        let count = Int(UInt32(bigEndian: size))
        guard count > 0, count <= maximumFrameSize else { throw ShellControlError.invalidFrame }
        var payload = Data(count: count)
        try payload.withUnsafeMutableBytes { try readAll($0, from: descriptor) }
        return payload
    }

    private static func writeAll(_ bytes: UnsafeRawBufferPointer, to descriptor: Int32) throws {
        var offset = 0
        while offset < bytes.count {
            let result = Darwin.write(descriptor, bytes.baseAddress!.advanced(by: offset), bytes.count - offset)
            guard result > 0 else { throw posixError() }
            offset += result
        }
    }

    private static func readAll(_ bytes: UnsafeMutableRawBufferPointer, from descriptor: Int32) throws {
        var offset = 0
        while offset < bytes.count {
            let result = Darwin.read(descriptor, bytes.baseAddress!.advanced(by: offset), bytes.count - offset)
            guard result > 0 else { throw posixError() }
            offset += result
        }
    }

    private static func posixError() -> ShellControlError {
        ShellControlError.unavailable(String(cString: strerror(errno)))
    }
}
