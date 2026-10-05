import Foundation
import Testing
@testable import SAGECerebrumNative

@Suite struct ShellControlQualification {
    private func response(_ version: String) -> ShellControlStatus {
        ShellControlStatus(controlProtocol: 1, daemonVersion: version, apiSchema: 1,
                           minimumShellProtocol: 1, maximumShellProtocol: 1,
                           instanceGeneration: String(repeating: "A", count: 43), state: .ready,
                           uiOrigin: "http://127.0.0.1:49152", startupProof: nil)
    }

    @Test(arguments: ["11.10.0", "11.19.999", "v11.19.1+build.01", "11.19.0-rc.1",
                      "12.0.0-beta", "12.0.0-beta.1", "v12.0.0-beta.9+build.01"])
    func supportedVersions(_ version: String) throws {
        try ShellControlClient.validate(response(version))
    }

    @Test(arguments: ["", "v", "dev", "-", "+", "12.0.0", "11.9.9", "11.20.0", "11.23.15",
                      "13.0.0-beta.1", "12.1.0-beta.1", "12.0.0-rc.1", "12.0.0-beta2",
                      "12.0.0-beta.", "12.0.0-beta..1", "12.0.0-beta.01", "12.0.0-beta.1+",
                      "12.0.0-beta.1+build..1", "12.0.0-beta.1+x+y", "012.0.0-beta.1",
                      "12.00.0-beta.1", "12.0.00-beta.1", "12.0.١-beta.1", "12.0.0-beta.α",
                      " 12.0.0-beta.1", "12.0.0-beta.1\n", "12.0-beta.1", "12.0.0.0-beta.1",
                      "11.19.x", "11.19.0+", "11.19.0-", "11.19.0-01", "vv11.19.0"])
    func malformedOrUnsupportedVersions(_ version: String) {
        #expect(throws: ShellControlError.self) { try ShellControlClient.validate(response(version)) }
    }

    @Test func unknownResponseFieldsRequireProtocolNegotiation() throws {
        let payload = Data(#"{"control_protocol":1,"daemon_version":"12.0.0-beta.1","api_schema":1,"min_shell_protocol":1,"max_shell_protocol":1,"instance_generation":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","state":"ready","ui_origin":"http://127.0.0.1:49152","new_authority":true}"#.utf8)
        #expect(throws: ShellControlError.self) { try JSONDecoder().decode(ShellControlStatus.self, from: payload) }
    }

    @Test(arguments: ["\n", "\r\n", "\r"])
    func eventByteFramingPreservesBlankLinesAndUnicode(_ separator: String) throws {
        let text = "\u{FEFF}event: consensus\(separator)data: {\"label\":\"脑 🧠\"}\(separator)\(separator)"
        var lines = SSELineAccumulator()
        var frames = SSEEventAccumulator()
        var events: [DashboardEvent] = []
        for byte in text.utf8 {
            if let line = try lines.consume(byte), let event = frames.consume(line) { events.append(event) }
        }
        #expect(events.count == 1)
        #expect(events.first?.name == "consensus")
        #expect(events.first?.data == #"{"label":"脑 🧠"}"#)
    }

    @Test func unfinishedEventDoesNotDispatchAtEOF() throws {
        var lines = SSELineAccumulator()
        var frames = SSEEventAccumulator()
        for byte in "event: consensus\ndata: incomplete".utf8 {
            if let line = try lines.consume(byte) { #expect(frames.consume(line) == nil) }
        }
    }

    @Test func eventLineBufferIsBounded() throws {
        var lines = SSELineAccumulator()
        for _ in 0 ..< 1_048_576 { _ = try lines.consume(65) }
        #expect(throws: DashboardEventError.self) { try lines.consume(65) }
    }
}
