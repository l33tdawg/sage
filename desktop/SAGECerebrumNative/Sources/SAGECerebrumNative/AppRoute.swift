import Foundation

enum AppRoute: String, CaseIterable, Identifiable, Sendable {
    case overview
    case brain
    case search
    case tasks
    case importData = "import"
    case network
    case access
    case federation
    case settings

    var id: String { rawValue }

    var isImplemented: Bool {
        switch self {
        case .overview, .brain, .search: true
        default: false
        }
    }

    static var implemented: [AppRoute] { allCases.filter(\.isImplemented) }

    var navigationShortcut: Character? {
        switch self {
        case .overview: "1"
        case .brain: "2"
        case .search: "3"
        default: nil
        }
    }

    var title: String {
        switch self {
        case .overview: "Overview"
        case .brain: "Brain"
        case .search: "Search"
        case .tasks: "Tasks & Messages"
        case .importData: "Import"
        case .network: "Agents"
        case .access: "Access Controls"
        case .federation: "Federation"
        case .settings: "Settings"
        }
    }

    var systemImage: String {
        switch self {
        case .overview: "gauge.with.dots.needle.67percent"
        case .brain: "brain.head.profile"
        case .search: "magnifyingglass"
        case .tasks: "checklist"
        case .importData: "square.and.arrow.down"
        case .network: "person.3"
        case .access: "person.badge.key"
        case .federation: "network"
        case .settings: "gearshape"
        }
    }

    var cerebrumHash: String {
        switch self {
        case .brain: "#/"
        default: "#/\(rawValue)"
        }
    }
}
