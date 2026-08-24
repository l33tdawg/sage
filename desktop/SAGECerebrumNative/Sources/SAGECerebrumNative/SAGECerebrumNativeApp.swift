import SwiftUI

@main
struct SAGECerebrumNativeApp: App {
    @State private var session: AppSession

    init() {
        #if DEBUG
        if ProcessInfo.processInfo.environment["SAGE_NATIVE_DESIGN_PREVIEW"] == "1" {
            _session = State(initialValue: AppSession(previewAPI: DesignPreviewAPI()))
            return
        }
        #endif
        _session = State(initialValue: AppSession())
    }

    var body: some Scene {
        Window("SAGE CEREBRUM", id: "main") {
            RootView(session: session)
                .frame(minWidth: 820, minHeight: 600)
                #if DEBUG
                .background {
                    if let fixture = NativeAppSceneAcceptanceFixture() {
                        NativeAppSceneAcceptanceProbe(fixture: fixture, session: session)
                            .frame(width: 0, height: 0)
                    }
                }
                #endif
        }
        .defaultSize(width: 1180, height: 800)
        .commands {
            SidebarCommands()
            CommandGroup(replacing: .newItem) {}
            CommandGroup(replacing: .appSettings) {}
            CerebrumViewCommands(session: session)
        }
    }
}
