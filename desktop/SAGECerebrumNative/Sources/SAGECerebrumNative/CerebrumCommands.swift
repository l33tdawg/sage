import AppKit
import SwiftUI

@MainActor
final class CerebrumNativeMenuCoordinator: NSObject, NSMenuItemValidation {
    static let shared = CerebrumNativeMenuCoordinator()
    private static let inspectorIdentifier = NSUserInterfaceItemIdentifier(CerebrumCommandID.searchToggleInspector.rawValue)
    private weak var session: AppSession?
    private weak var navigationMenu: NSMenu?
    private var observingMenuChanges = false
    private var refreshing = false
    private var refreshScheduled = false

    init(session: AppSession? = nil) {
        self.session = session
        super.init()
    }

    func install(session: AppSession) {
        self.session = session
        if !observingMenuChanges {
            observingMenuChanges = true
            for name in [NSMenu.didAddItemNotification, NSMenu.didRemoveItemNotification] {
                NotificationCenter.default.addObserver(self, selector: #selector(menuContentsChanged(_:)), name: name, object: nil)
            }
            NotificationCenter.default.addObserver(self, selector: #selector(viewMenuWillTrack(_:)), name: NSMenu.didBeginTrackingNotification, object: nil)
        }
        Task { @MainActor [weak self] in
            for _ in 0..<100 {
                if self?.refresh() == true { return }
                await Task.yield()
                try? await Task.sleep(for: .milliseconds(10))
            }
        }
    }

    @objc private func toggleSearchInspector(_ sender: NSMenuItem) {
        session?.requestSearchInspectorToggle()
    }

    func validateMenuItem(_ menuItem: NSMenuItem) -> Bool {
        if menuItem.action == #selector(performGlobalCommand(_:)) {
            return validateGlobalMenuItem(menuItem)
        }
        if menuItem.action == #selector(performBrainCommand(_:)) {
            return validateBrainMenuItem(menuItem)
        }
        guard menuItem.action == #selector(toggleSearchInspector(_:)), let session else { return false }
        menuItem.title = session.searchInspectorIsPresented ? "Hide Inspector" : "Show Inspector"
        return session.searchHasInspector && !session.searchInspectorCommandsBlocked &&
            session.searchInspectorToggleRequestID == session.consumedSearchInspectorToggleRequestID &&
            session.acceptsRouteCommands(for: .search) && !session.showsKeyboardShortcuts
    }

    @discardableResult
    func refresh() -> Bool {
        guard !refreshing else { return true }
        refreshing = true
        defer { refreshing = false }
        guard let session else { return false }
        guard let mainMenu = NSApp.mainMenu else { return false }
        if let menu = mainMenu.items.first(where: { $0.title == "Navigate" })?.submenu {
            installNavigationTracking(for: menu)
            refreshNavigationMenu(in: menu)
        }
        guard let viewMenu = mainMenu.items.first(where: { $0.title == "View" })?.submenu else { return false }
        refreshFocusSearchMenuItem(in: viewMenu)
        refreshBrainMenu(in: viewMenu)
        let existing = viewMenu.items.first { $0.identifier == Self.inspectorIdentifier }
        guard session.route == .search else {
            if let existing { viewMenu.removeItem(existing) }
            return true
        }
        let item = existing ?? NSMenuItem(
            title: "Show Inspector",
            action: #selector(toggleSearchInspector(_:)),
            keyEquivalent: "i"
        )
        item.identifier = Self.inspectorIdentifier
        item.target = self
        item.action = #selector(toggleSearchInspector(_:))
        item.keyEquivalentModifierMask = [.control, .command]
        if existing == nil {
            let focusIndex = viewMenu.items.firstIndex { $0.title == CerebrumCommandID.focusSearch.specification.label }
            viewMenu.insertItem(item, at: min((focusIndex ?? viewMenu.items.count - 1) + 1, viewMenu.items.count))
        }
        item.title = session.searchInspectorIsPresented ? "Hide Inspector" : "Show Inspector"
        item.isEnabled = validateMenuItem(item)
        return true
    }

    private struct BrainMenuTarget {
        let command: CerebrumCommandID
        let owner: UUID
    }

    // AppKit owns these concrete route items. SwiftUI's focused Commands can
    // remain absent even after the destination is mounted and publishes state.
    func refreshBrainMenu(in menu: NSMenu) {
        guard let session, session.acceptsRouteCommands(for: .brain),
              let owner = session.brainCommandOwner, session.brainCommandState != nil else {
            for item in menu.items where item.identifier?.rawValue.hasPrefix("brain.") == true {
                menu.removeItem(item)
            }
            return
        }
        let refresh = brainMenuItem(.brainRefresh, owner: owner, in: menu)
        let mode = brainSubmenu("Brain Mode", identifier: "brain.menu.mode", in: menu)
        for command in [CerebrumCommandID.brainModeMemory, .brainModeAgent] {
            _ = brainMenuItem(command, owner: owner, in: mode)
        }
        let presentation = brainSubmenu("Brain Presentation", identifier: "brain.menu.presentation", in: menu)
        for command in [CerebrumCommandID.brainPresentationInteractive, .brainPresentationList] {
            _ = brainMenuItem(command, owner: owner, in: presentation)
        }
        for command in [CerebrumCommandID.brainToggleInspector, .brainViewOptions, .brainClearSelection] {
            _ = brainMenuItem(command, owner: owner, in: menu)
        }
        // Keep Refresh beside Focus Search without giving Command-R another owner.
        if let focus = menu.items.firstIndex(where: { $0.title == CerebrumCommandID.focusSearch.specification.label }),
           menu.index(of: refresh) != focus + 1 {
            menu.removeItem(refresh)
            menu.insertItem(refresh, at: min(focus + 1, menu.items.count))
        }
    }

    private func brainSubmenu(_ title: String, identifier: String, in menu: NSMenu) -> NSMenu {
        let id = NSUserInterfaceItemIdentifier(identifier)
        if let existing = menu.items.first(where: { $0.identifier == id })?.submenu { return existing }
        let item = NSMenuItem(title: title, action: nil, keyEquivalent: "")
        item.identifier = id
        let submenu = NSMenu(title: title)
        item.submenu = submenu
        menu.addItem(item)
        return submenu
    }

    private func brainMenuItem(_ command: CerebrumCommandID, owner: UUID, in menu: NSMenu) -> NSMenuItem {
        let identifier = NSUserInterfaceItemIdentifier(command.rawValue)
        var existing = menu.items.first(where: { $0.identifier == identifier })
        if let previous = existing, (previous.representedObject as? BrainMenuTarget)?.owner != owner {
            // Never retarget a retained menu item from an earlier mount. An
            // already-captured target/action must keep its revoked owner.
            menu.removeItem(previous)
            existing = nil
        }
        let item = existing ?? NSMenuItem(
            title: command.specification.label,
            action: #selector(performBrainCommand(_:)),
            keyEquivalent: command.specification.key.map(String.init) ?? ""
        )
        item.identifier = identifier
        item.target = self
        item.action = #selector(performBrainCommand(_:))
        item.representedObject = BrainMenuTarget(command: command, owner: owner)
        item.keyEquivalentModifierMask = command == .brainRefresh ? [.command] :
            (command.specification.key == nil ? [] : [.control, .command])
        if item.menu == nil { menu.addItem(item) }
        item.isEnabled = validateBrainMenuItem(item)
        return item
    }

    private func validateBrainMenuItem(_ item: NSMenuItem) -> Bool {
        guard let target = item.representedObject as? BrainMenuTarget,
              let session, let state = session.brainCommandState,
              session.brainCommandOwner == target.owner else { return false }
        switch target.command {
        case .brainModeMemory: item.state = state.mode == .memory ? .on : .off
        case .brainModeAgent: item.state = state.mode == .connectome ? .on : .off
        case .brainPresentationInteractive: item.state = state.presentation == .mri ? .on : .off
        case .brainPresentationList: item.state = state.presentation == .table ? .on : .off
        case .brainToggleInspector: item.title = state.inspectorIsPresented ? "Hide Inspector" : "Show Inspector"
        case .brainViewOptions: item.title = state.viewOptionsArePresented ? "Hide View Options" : "Show View Options"
        default: break
        }
        return session.acceptsRouteCommands(for: .brain) && !session.showsKeyboardShortcuts &&
            session.brainCommandRequest == nil && state.allows(target.command)
    }

    @objc private func performBrainCommand(_ sender: NSMenuItem) {
        guard validateBrainMenuItem(sender), let target = sender.representedObject as? BrainMenuTarget else { return }
        session?.requestBrainCommand(target.command, owner: target.owner)
    }

    @objc private func menuContentsChanged(_ notification: Notification) {
        guard !refreshing, !refreshScheduled, let menu = notification.object as? NSMenu,
              let mainMenu = NSApp.mainMenu,
              menu === mainMenu || menu === navigationMenu ||
                menu === mainMenu.items.first(where: { $0.title == "View" })?.submenu else { return }
        // SwiftUI can replace its menu contents after a route update. Restore
        // the owned items once that update finishes, including before a shortcut.
        refreshScheduled = true
        Task { @MainActor [weak self] in
            await Task.yield()
            self?.refreshScheduled = false
            self?.refresh()
        }
    }

    @objc private func viewMenuWillTrack(_ notification: Notification) {
        guard let menu = notification.object as? NSMenu,
              menu === NSApp.mainMenu?.items.first(where: { $0.title == "View" })?.submenu else { return }
        refresh()
    }

    private func installNavigationTracking(for menu: NSMenu) {
        guard navigationMenu !== menu else { return }
        if let navigationMenu {
            NotificationCenter.default.removeObserver(
                self,
                name: NSMenu.didBeginTrackingNotification,
                object: navigationMenu
            )
        }
        navigationMenu = menu
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(navigationMenuDidBeginTracking(_:)),
            name: NSMenu.didBeginTrackingNotification,
            object: menu
        )
    }

    @objc private func navigationMenuDidBeginTracking(_ notification: Notification) {
        guard let menu = notification.object as? NSMenu, menu === navigationMenu else { return }
        refreshNavigationMenu(in: menu)
    }

    private enum GlobalMenuTarget {
        case navigate(AppRoute)
        case focusSearch
    }

    // Keep SwiftUI's single shortcut item, but validate and dispatch against the
    // current session. Its cached enabled state can outlive an attached sheet.
    func refreshNavigationMenu(in menu: NSMenu) {
        for route in AppRoute.implemented {
            guard let shortcut = route.navigationShortcut else { continue }
            let matches = menu.items.filter {
                $0.title == route.title && $0.keyEquivalent == String(shortcut) &&
                    $0.keyEquivalentModifierMask.intersection(.deviceIndependentFlagsMask) == [.command]
            }
            guard matches.count == 1, let item = matches.first else { continue }
            configureGlobalMenuItem(item, target: .navigate(route))
        }
    }

    func refreshFocusSearchMenuItem(in menu: NSMenu) {
        let matches = menu.items.filter {
            $0.title == CerebrumCommandID.focusSearch.specification.label && $0.keyEquivalent == "f" &&
                $0.keyEquivalentModifierMask.intersection(.deviceIndependentFlagsMask) == [.command]
        }
        guard matches.count == 1, let item = matches.first else { return }
        configureGlobalMenuItem(item, target: .focusSearch)
    }

    private func configureGlobalMenuItem(_ item: NSMenuItem, target: GlobalMenuTarget) {
        item.target = self
        item.action = #selector(performGlobalCommand(_:))
        item.representedObject = target
        item.isEnabled = validateGlobalMenuItem(item)
    }

    private func validateGlobalMenuItem(_ item: NSMenuItem) -> Bool {
        guard let target = item.representedObject as? GlobalMenuTarget, let session else { return false }
        if case let .navigate(route) = target {
            item.state = session.route == route ? .on : .off
            guard route.isImplemented else { return false }
        }
        return session.acceptsGlobalCommands
    }

    @objc private func performGlobalCommand(_ sender: NSMenuItem) {
        guard validateGlobalMenuItem(sender), let target = sender.representedObject as? GlobalMenuTarget else { return }
        switch target {
        case let .navigate(route): session?.navigate(to: route)
        case .focusSearch: session?.focusSearch()
        }
    }
}

struct CerebrumRouteCommandActions {
    let route: AppRoute
    let isRefreshing: Bool
    let refresh: () -> Void
    var blocksGlobalCommands = false
    var search: SearchCommandActions?
}

struct SearchCommandActions {
    let inspectorIsPresented: Bool
    let hasInspector: Bool
    let hasSelection: Bool
    let toggleInspector: () -> Void
    let clearSelection: () -> Void
}

enum CerebrumCommandID: String, CaseIterable {
    case focusSearch = "global.command.focus-search"
    case keyboardShortcuts = "global.command.keyboard-shortcuts"
    case overviewRefresh = "overview.command.refresh"
    case searchRefresh = "search.command.refresh"
    case searchToggleInspector = "search.command.toggle-inspector"
    case searchClearSelection = "search.command.clear-selection"
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
        case .searchToggleInspector: .init(label: "Show or Hide Inspector", key: "i", modifiers: [.control, .command], display: "⌃⌘I", section: "Search")
        case .searchClearSelection: .init(label: "Clear Search Selection", key: nil, modifiers: [], display: "", section: "Search")
        case .brainRefresh: .init(label: "Refresh Brain", key: "r", modifiers: .command, display: "⌘R", section: "Global")
        case .brainToggleInspector: .init(label: "Show or Hide Inspector", key: "i", modifiers: [.control, .command], display: "⌃⌘I", section: "Brain")
        case .brainModeMemory: .init(label: "Memory Map", key: "1", modifiers: [.control, .command], display: "⌃⌘1", section: "Brain")
        case .brainModeAgent: .init(label: "Agent Network", key: "2", modifiers: [.control, .command], display: "⌃⌘2", section: "Brain")
        case .brainPresentationInteractive: .init(label: "Interactive Map", key: "m", modifiers: [.control, .command], display: "⌃⌘M", section: "Brain")
        case .brainPresentationList: .init(label: "List View", key: "l", modifiers: [.control, .command], display: "⌃⌘L", section: "Brain")
        case .brainClearSelection: .init(label: "Clear Brain Selection", key: nil, modifiers: [], display: "", section: "Brain")
        case .brainViewOptions: .init(label: "Show or Hide View Options", key: "v", modifiers: [.control, .command], display: "⌃⌘V", section: "Brain")
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

// A mounted Brain publishes values, not closures that retain its view or session.
// Requests are delivered back to that exact mount and revalidated before use.
struct BrainCommandState: Equatable {
    var mode: BrainMode = .memory
    var presentation: BrainPresentation = .mri
    var isRefreshing = false
    var inspectorIsPresented = false
    var hasInspector = false
    var hasSelection = false
    var viewOptionsArePresented = false
    var interactiveMapIsEnabled = true
    var blocksGlobalCommands = false

    func allows(_ command: CerebrumCommandID) -> Bool {
        guard !blocksGlobalCommands else { return false }
        switch command {
        case .brainRefresh: return !isRefreshing
        case .brainToggleInspector: return hasInspector
        case .brainClearSelection: return hasSelection
        case .brainPresentationInteractive: return interactiveMapIsEnabled
        case .brainModeMemory, .brainModeAgent, .brainPresentationList, .brainViewOptions: return true
        default: return false
        }
    }
}

struct BrainCommandRequest: Equatable {
    let id: UInt64
    let owner: UUID
    let command: CerebrumCommandID
}

private struct CerebrumSessionKey: EnvironmentKey {
    static let defaultValue: AppSession? = nil
}

extension EnvironmentValues {
    var cerebrumSession: AppSession? {
        get { self[CerebrumSessionKey.self] }
        set { self[CerebrumSessionKey.self] = newValue }
    }
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
                    Toggle(
                        route.title,
                        isOn: commandToggle(
                            selected: session.route == route,
                            select: { session.navigate(to: route) }
                        )
                    )
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
                Button("Refresh \(routeActions.route.title)") {
                    guard session.acceptsRouteCommands(for: routeActions.route), routeCommandsAreEnabled else { return }
                    routeActions.refresh()
                }
                    .cerebrumShortcut(refreshCommandID(for: routeActions.route))
                    .disabled(routeActions.isRefreshing)
                    .accessibilityIdentifier(refreshCommandID(for: routeActions.route).rawValue)
            }

            if let search = activeRouteActions?.search {
                Button("Clear Search Selection") { search.clearSelection() }
                    .disabled(!search.hasSelection)
                    .accessibilityIdentifier(CerebrumCommandID.searchClearSelection.rawValue)
            }

        }

        CommandGroup(before: .help) {
            Button(CerebrumCommandID.keyboardShortcuts.specification.label) {
                guard !session.showsKeyboardShortcuts, !activeRouteBlocksGlobalCommands else { return }
                session.showsKeyboardShortcuts = true
            }
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
        guard session.route != .brain, let routeActions,
              session.acceptsRouteCommands(for: routeActions.route),
              !session.showsKeyboardShortcuts,
              !routeActions.blocksGlobalCommands else { return nil }
        return routeActions
    }

    private var activeRouteBlocksGlobalCommands: Bool {
        if session.route == .brain {
            // Navigation must not wait for a focused descendant to appear. Until
            // the destination registers, route actions remain unavailable.
            return session.brainCommandState?.blocksGlobalCommands ?? false
        }
        guard let routeActions else { return false }
        return routeActions.route != session.route || routeActions.blocksGlobalCommands
    }

    private var focusSearchIsEnabled: Bool {
        routeCommandsAreEnabled
    }

    private var routeCommandsAreEnabled: Bool {
        session.acceptsGlobalCommands && !activeRouteBlocksGlobalCommands
    }

    private func refreshCommandID(for route: AppRoute) -> CerebrumCommandID {
        switch route {
        case .overview: .overviewRefresh
        case .search: .searchRefresh
        case .brain: .brainRefresh
        default: .overviewRefresh
        }
    }

    func commandToggle(selected: Bool, select: @escaping () -> Void) -> Binding<Bool> {
        Binding(
            get: { selected },
            // These are mutually exclusive choices, not switches. AppKit can
            // still carry the previous checkmark during a SwiftUI update; an
            // activation must select its destination even if it proposes false.
            set: { _ in select() }
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
            ("Search", [
                Self.shortcutRow(.focusSearch), Self.shortcutRow(.searchToggleInspector),
                ("Clear Search Selection and Details", "Esc"),
            ]),
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
                    .accessibilityIdentifier("keyboard-shortcuts-done")
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
