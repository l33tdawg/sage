import SwiftUI

struct CerebrumRouteCommandActions {
    let route: AppRoute
    let isRefreshing: Bool
    let refresh: () -> Void
    var blocksGlobalCommands = false
    var brain: BrainCommandActions?
}

enum CerebrumCommandID: String, CaseIterable {
    case focusSearch = "global.command.focus-search"
    case keyboardShortcuts = "global.command.keyboard-shortcuts"
    case overviewRefresh = "overview.command.refresh"
    case searchRefresh = "search.command.refresh"
    case brainRefresh = "brain.command.refresh"
    case brainToggleInspector = "brain.command.toggle-inspector"
    case brainModeMemory = "brain.command.mode-memory-map"
    case brainModeAgent = "brain.command.mode-agent-network"
    case brainPresentationInteractive = "brain.command.presentation-interactive-map"
    case brainPresentationList = "brain.command.presentation-list-view"
    case brainClearSelection = "brain.command.clear-selection"
    case brainViewOptions = "brain.command.view-options"

    var specification: CerebrumCommandSpecification {
        switch self {
        case .focusSearch: .init(label: "Focus Search", key: "f", modifiers: .command, display: "⌘F", section: "Search")
        case .keyboardShortcuts: .init(label: "Keyboard Shortcuts…", key: "/", modifiers: .command, display: "⌘/", section: "Global")
        case .overviewRefresh: .init(label: "Refresh Overview", key: "r", modifiers: .command, display: "⌘R", section: "Global")
        case .searchRefresh: .init(label: "Refresh Search", key: "r", modifiers: .command, display: "⌘R", section: "Global")
        case .brainRefresh: .init(label: "Refresh Brain", key: "r", modifiers: .command, display: "⌘R", section: "Global")
        case .brainToggleInspector: .init(label: "Show or Hide Inspector", key: "i", modifiers: [.control, .command], display: "⌃⌘I", section: "Brain")
        case .brainModeMemory: .init(label: "Memory Map", key: "1", modifiers: [.control, .option], display: "⌥⌃1", section: "Brain")
        case .brainModeAgent: .init(label: "Agent Network", key: "2", modifiers: [.control, .option], display: "⌥⌃2", section: "Brain")
        case .brainPresentationInteractive: .init(label: "Interactive Map", key: "m", modifiers: [.control, .command], display: "⌃⌘M", section: "Brain")
        case .brainPresentationList: .init(label: "List View", key: "l", modifiers: [.control, .command], display: "⌃⌘L", section: "Brain")
        case .brainClearSelection: .init(label: "Clear Brain Selection", key: nil, modifiers: [], display: "", section: "Brain")
        case .brainViewOptions: .init(label: "Show or Hide View Options", key: "v", modifiers: [.control, .option], display: "⌥⌃V", section: "Brain")
        }
    }
}

struct CerebrumCommandSpecification {
    let label: String
    let key: Character?
    let modifiers: EventModifiers
    let display: String
    let section: String
}

struct BrainCommandActions {
    let mode: BrainMode
    let presentation: BrainPresentation
    let inspectorIsPresented: Bool
    let hasInspector: Bool
    let hasSelection: Bool
    let viewOptionsArePresented: Bool
    let interactiveMapIsEnabled: Bool
    let setMode: (BrainMode) -> Void
    let setPresentation: (BrainPresentation) -> Void
    let toggleInspector: () -> Void
    let clearSelection: () -> Void
    let toggleViewOptions: () -> Void
}

private struct CerebrumRouteCommandActionsKey: FocusedValueKey {
    typealias Value = CerebrumRouteCommandActions
}

extension FocusedValues {
    var cerebrumRouteCommandActions: CerebrumRouteCommandActions? {
        get { self[CerebrumRouteCommandActionsKey.self] }
        set { self[CerebrumRouteCommandActionsKey.self] = newValue }
    }
}

struct CerebrumViewCommands: Commands {
    @FocusedValue(\.cerebrumRouteCommandActions) private var routeActions
    @Bindable var session: AppSession

    var body: some Commands {
        CommandMenu("Navigate") {
            ForEach(AppRoute.implemented) { route in
                if let shortcut = route.navigationShortcut {
                    Button(route.title) { session.route = route }
                        .keyboardShortcut(KeyEquivalent(shortcut), modifiers: .command)
                        .disabled(!routeCommandsAreEnabled)
                }
            }
        }

        CommandGroup(after: .sidebar) {
            Divider()
            Button(CerebrumCommandID.focusSearch.specification.label) { session.focusSearch() }
                .cerebrumShortcut(CerebrumCommandID.focusSearch)
                .disabled(!focusSearchIsEnabled)
                .accessibilityIdentifier(CerebrumCommandID.focusSearch.rawValue)
            if let routeActions = activeRouteActions {
                Button("Refresh \(routeActions.route.title)", action: routeActions.refresh)
                    .cerebrumShortcut(refreshCommandID(for: routeActions.route))
                    .disabled(routeActions.isRefreshing)
                    .accessibilityIdentifier(refreshCommandID(for: routeActions.route).rawValue)
            }

            if let brain = activeRouteActions?.brain {
                Divider()
                Menu("Brain Mode") {
                    Toggle(
                        CerebrumCommandID.brainModeMemory.specification.label,
                        isOn: commandToggle(
                            selected: brain.mode == .memory,
                            select: { brain.setMode(.memory) }
                        )
                    )
                    .cerebrumShortcut(CerebrumCommandID.brainModeMemory)
                    .accessibilityIdentifier(CerebrumCommandID.brainModeMemory.rawValue)

                    Toggle(
                        CerebrumCommandID.brainModeAgent.specification.label,
                        isOn: commandToggle(
                            selected: brain.mode == .connectome,
                            select: { brain.setMode(.connectome) }
                        )
                    )
                    .cerebrumShortcut(CerebrumCommandID.brainModeAgent)
                    .accessibilityIdentifier(CerebrumCommandID.brainModeAgent.rawValue)
                }

                Menu("Brain Presentation") {
                    Toggle(
                        CerebrumCommandID.brainPresentationInteractive.specification.label,
                        isOn: commandToggle(
                            selected: brain.presentation == .mri,
                            select: { brain.setPresentation(.mri) }
                        )
                    )
                    .cerebrumShortcut(CerebrumCommandID.brainPresentationInteractive)
                    .disabled(!brain.interactiveMapIsEnabled)
                    .accessibilityIdentifier(CerebrumCommandID.brainPresentationInteractive.rawValue)

                    Toggle(
                        CerebrumCommandID.brainPresentationList.specification.label,
                        isOn: commandToggle(
                            selected: brain.presentation == .table,
                            select: { brain.setPresentation(.table) }
                        )
                    )
                    .cerebrumShortcut(CerebrumCommandID.brainPresentationList)
                    .accessibilityIdentifier(CerebrumCommandID.brainPresentationList.rawValue)
                }

                Button(brain.inspectorIsPresented ? "Hide Inspector" : "Show Inspector") {
                    brain.toggleInspector()
                }
                .cerebrumShortcut(CerebrumCommandID.brainToggleInspector)
                .disabled(!brain.hasInspector)
                .accessibilityIdentifier(CerebrumCommandID.brainToggleInspector.rawValue)

                Button(brain.viewOptionsArePresented ? "Hide View Options" : "Show View Options") {
                    brain.toggleViewOptions()
                }
                .cerebrumShortcut(CerebrumCommandID.brainViewOptions)
                .accessibilityIdentifier(CerebrumCommandID.brainViewOptions.rawValue)

                Button("Clear Brain Selection") { brain.clearSelection() }
                    .disabled(!brain.hasSelection)
                    .accessibilityIdentifier(CerebrumCommandID.brainClearSelection.rawValue)
            }
        }

        CommandGroup(before: .help) {
            Button(CerebrumCommandID.keyboardShortcuts.specification.label) { session.showsKeyboardShortcuts = true }
                .cerebrumShortcut(CerebrumCommandID.keyboardShortcuts)
                .disabled(session.showsKeyboardShortcuts || activeRouteBlocksGlobalCommands)
                .accessibilityIdentifier(CerebrumCommandID.keyboardShortcuts.rawValue)
        }

        CommandMenu("CEREBRUM") {
            Button("Lock CEREBRUM") { Task { await session.lock() } }
                .keyboardShortcut("l", modifiers: .command)
                .disabled(!routeCommandsAreEnabled)
        }
    }

    private var activeRouteActions: CerebrumRouteCommandActions? {
        guard let routeActions,
              session.acceptsRouteCommands(for: routeActions.route),
              !session.showsKeyboardShortcuts,
              !routeActions.blocksGlobalCommands else { return nil }
        return routeActions
    }

    private var activeRouteBlocksGlobalCommands: Bool {
        guard let routeActions else { return false }
        return routeActions.route != session.route || routeActions.blocksGlobalCommands
    }

    private var focusSearchIsEnabled: Bool {
        routeCommandsAreEnabled
    }

    private var routeCommandsAreEnabled: Bool {
        session.acceptsReadyCommands && session.api != nil &&
            !session.showsKeyboardShortcuts && !activeRouteBlocksGlobalCommands
    }

    private func refreshCommandID(for route: AppRoute) -> CerebrumCommandID {
        switch route {
        case .overview: .overviewRefresh
        case .search: .searchRefresh
        case .brain: .brainRefresh
        default: .overviewRefresh
        }
    }

    private func commandToggle(selected: Bool, select: @escaping () -> Void) -> Binding<Bool> {
        Binding(
            get: { selected },
            set: { if $0 { select() } }
        )
    }
}

private extension View {
    @ViewBuilder
    func cerebrumShortcut(_ command: CerebrumCommandID) -> some View {
        if let key = command.specification.key {
            keyboardShortcut(KeyEquivalent(key), modifiers: command.specification.modifiers)
        } else {
            self
        }
    }
}

struct CerebrumKeyboardShortcutsView: View {
    @Environment(\.dismiss) private var dismiss
    @FocusState private var doneFocused: Bool

    private var sections: [(String, [(String, String)])] {
        [
            ("Global", [
                ("Overview", "⌘1"), ("Brain", "⌘2"), ("Search", "⌘3"),
                ("Refresh Current View", "⌘R"), ("Lock CEREBRUM", "⌘L"),
                Self.shortcutRow(.keyboardShortcuts),
            ]),
            ("Search", [Self.shortcutRow(.focusSearch)]),
            ("Brain", [
                Self.shortcutRow(.brainModeMemory), Self.shortcutRow(.brainModeAgent),
                Self.shortcutRow(.brainPresentationInteractive), Self.shortcutRow(.brainPresentationList),
                Self.shortcutRow(.brainToggleInspector), Self.shortcutRow(.brainViewOptions),
                ("Dismiss Current Brain Focus", "Esc"),
            ]),
        ]
    }

    private static func shortcutRow(_ command: CerebrumCommandID) -> (String, String) {
        (command.specification.label, command.specification.display)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Keyboard Shortcuts")
                        .font(.title2.weight(.bold))
                        .accessibilityAddTraits(.isHeader)
                    Text("Commands adapt to the active native CEREBRUM view.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button("Done") { dismiss() }
                    .keyboardShortcut(.cancelAction)
                    .focused($doneFocused)
            }

            Divider()

            ScrollView {
                Grid(alignment: .leading, horizontalSpacing: 28, verticalSpacing: 9) {
                    ForEach(Array(sections.enumerated()), id: \.offset) { _, section in
                        GridRow {
                            Text(section.0.uppercased())
                                .font(.caption2.weight(.bold))
                                .foregroundStyle(CerebrumTheme.cyan)
                                .accessibilityAddTraits(.isHeader)
                            Color.clear.frame(width: 1, height: 1)
                        }
                        ForEach(section.1, id: \.0) { action, shortcut in
                            GridRow {
                                Text(action)
                                Text(shortcut)
                                    .font(.system(.body, design: .monospaced).weight(.medium))
                                    .foregroundStyle(.secondary)
                                    .frame(maxWidth: .infinity, alignment: .trailing)
                            }
                            .accessibilityElement(children: .ignore)
                            .accessibilityLabel("\(action), \(shortcut)")
                        }
                    }
                }
            }
        }
        .padding(24)
        .frame(minWidth: 360, idealWidth: 500, maxWidth: 620, minHeight: 340, idealHeight: 460, maxHeight: 620)
        .accessibilityElement(children: .contain)
        .task { doneFocused = true }
    }
}
