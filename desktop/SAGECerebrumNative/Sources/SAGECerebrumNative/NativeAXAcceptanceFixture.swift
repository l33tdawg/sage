#if DEBUG
import Foundation

struct NativeAXAcceptanceFixture: Equatable {
    enum RetryResult: String {
        case fail
        case restore
    }

    let retryResult: RetryResult
    let retryDelay: Duration

    init?(environment: [String: String] = ProcessInfo.processInfo.environment) {
        guard environment["SAGE_NATIVE_DESIGN_PREVIEW"] == "1",
              environment["SAGE_NATIVE_AX_METAL"] == "unavailable",
              let rawResult = environment["SAGE_NATIVE_AX_RETRY_RESULT"],
              let retryResult = RetryResult(rawValue: rawResult)
        else { return nil }

        let rawDelay = environment["SAGE_NATIVE_AX_RETRY_DELAY_MS"] ?? "750"
        guard let delayMilliseconds = Int(rawDelay), (100...5_000).contains(delayMilliseconds) else {
            return nil
        }
        self.retryResult = retryResult
        retryDelay = .milliseconds(delayMilliseconds)
    }

    @MainActor
    func makeBrainView(api: any SAGEAPI) -> BrainView {
        BrainView(
            api: api,
            rendererBootstrap: { _ in .failure(.rendererInitialization) },
            retryRendererBootstrap: {
                do {
                    try await Task.sleep(for: retryDelay)
                } catch {
                    return .failure(.rendererInitialization)
                }
                guard !Task.isCancelled,
                      retryResult == .restore,
                      let renderer = BrainMetalRenderer(onPick: { _ in })
                else { return .failure(.rendererInitialization) }
                return .success(renderer)
            }
        )
    }
}
#endif
