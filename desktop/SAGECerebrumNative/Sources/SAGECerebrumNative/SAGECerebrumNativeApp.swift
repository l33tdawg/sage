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
        }
        .defaultSize(width: 1180, height: 800)
        .commands {
            SidebarCommands()
            CommandGroup(replacing: .newItem) {}
            CommandMenu("Navigate") {
                ForEach(AppRoute.implemented) { route in
                    if let shortcut = route.navigationShortcut {
                        Button(route.title) { session.route = route }
                            .keyboardShortcut(KeyEquivalent(shortcut), modifiers: .command)
                            .disabled(!session.acceptsReadyCommands)
                    }
                }
            }
            CommandGroup(replacing: .appSettings) {}
            CommandMenu("CEREBRUM") {
                Button("Lock CEREBRUM") { Task { await session.lock() } }
                    .keyboardShortcut("l", modifiers: .command)
                    .disabled(!session.acceptsReadyCommands)
            }
        }
    }
}
