import AppKit
import Foundation
import SwiftUI

extension CerebrumEventStreamState {
    var title: String {
        switch self {
        case .connecting: "Connecting event updates"
        case .connected: "Event updates connected"
        case .reconnecting: "Reconnecting event updates"
        case .stopped: "Event updates stopped"
        }
    }

    var systemImage: String {
        switch self {
        case .connecting: "ellipsis.circle"
        case .connected: "bolt.horizontal.circle"
        case .reconnecting: "arrow.triangle.2.circlepath"
        case .stopped: "pause.circle"
        }
    }
}

enum CerebrumSnapshotState: Equatable, Sendable {
    case loading
    case available(updatedAt: Date)
    case partial(updatedAt: Date, detail: String?)
    case refreshFailed(updatedAt: Date)
    case unavailable

    var title: String {
        switch self {
        case .loading: "Loading…"
        case .available: "Updated"
        case .partial: "Partially updated"
        case .refreshFailed: "Refresh failed"
        case .unavailable: "Data unavailable"
        }
    }

    var updatedAt: Date? {
        switch self {
        case .loading, .unavailable: nil
        case let .available(updatedAt), let .partial(updatedAt, _),
             let .refreshFailed(updatedAt): updatedAt
        }
    }

    var detail: String? {
        guard case let .partial(_, detail) = self else { return nil }
        return detail
    }
}

struct CerebrumDataStatus: Equatable, Sendable {
    let snapshot: CerebrumSnapshotState
    let events: CerebrumEventStreamState
    var isRefreshing = false
    var hasPendingUpdate = false
}

struct CerebrumDataStatusView: View {
    @Environment(\.accessibilityReduceTransparency) private var reduceTransparency
    let status: CerebrumDataStatus

    var body: some View {
        VStack(alignment: .trailing, spacing: 4) {
            HStack(spacing: 6) {
                Image(systemName: snapshotSystemImage)
                    .foregroundStyle(snapshotColor)
                    .accessibilityHidden(true)
                if status.isRefreshing {
                    ProgressView()
                        .controlSize(.mini)
                        .accessibilityHidden(true)
                }
                Text(status.snapshot.title)
                    .font(.caption.weight(.semibold))
                if let updatedAt = status.snapshot.updatedAt {
                    Text("·")
                        .foregroundStyle(.tertiary)
                        .accessibilityHidden(true)
                    Text(updatedAt, style: .relative)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .accessibilityElement(children: .ignore)
            .accessibilityLabel("Data status")
            .accessibilityValue(snapshotAccessibilityValue)

            if let detail = status.snapshot.detail {
                Text(detail)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.trailing)
                    .lineLimit(2)
                    .accessibilityHidden(true)
            }

            if status.hasPendingUpdate {
                Label("Update available", systemImage: "arrow.down.circle")
                    .font(.caption2.weight(.medium))
                    .foregroundStyle(.primary)
                    .accessibilityElement(children: .ignore)
                    .accessibilityLabel("Update available")
                    .accessibilityValue("Current selection is preserved")
            }

            HStack(spacing: 5) {
                Image(systemName: status.events.systemImage)
                    .accessibilityHidden(true)
                Text(status.events.title)
            }
            .font(.caption2)
            .foregroundStyle(.secondary)
            .accessibilityElement(children: .ignore)
            .accessibilityLabel("Event updates")
            .accessibilityValue(eventAccessibilityValue)
        }
        .padding(.horizontal, reduceTransparency ? 8 : 0)
        .padding(.vertical, reduceTransparency ? 6 : 0)
        .background {
            if reduceTransparency {
                RoundedRectangle(cornerRadius: 8)
                    .fill(Color(nsColor: .controlBackgroundColor))
                    .overlay(
                        RoundedRectangle(cornerRadius: 8)
                            .stroke(Color(nsColor: .separatorColor), lineWidth: 1)
                    )
            }
        }
    }

    private var snapshotColor: Color {
        switch status.snapshot {
        case .loading: .secondary
        case .available: CerebrumTheme.cyan
        case .partial, .refreshFailed: CerebrumTheme.amber
        case .unavailable: .red
        }
    }

    private var snapshotSystemImage: String {
        switch status.snapshot {
        case .loading: "hourglass"
        case .available: "checkmark.circle"
        case .partial: "circle.lefthalf.filled"
        case .refreshFailed: "clock.badge.exclamationmark"
        case .unavailable: "exclamationmark.triangle"
        }
    }

    private var snapshotAccessibilityValue: Text {
        var value = Text(status.snapshot.title)
        if let detail = status.snapshot.detail {
            value = value + Text(". \(detail)")
        }
        if let updatedAt = status.snapshot.updatedAt {
            value = value + Text(". Last updated ") + Text(updatedAt, style: .relative)
                + Text(", ") + Text(updatedAt, format: .dateTime)
        }
        if status.isRefreshing {
            value = value + Text(". Refreshing data")
        }
        return value
    }

    private var eventAccessibilityValue: String {
        let state: String
        switch status.events {
        case .connecting: state = "Connecting"
        case .connected: state = "Connected. Changes trigger a data refresh"
        case .reconnecting: state = "Reconnecting"
        case .stopped: state = "Stopped. Unlock CEREBRUM to reconnect"
        }
        return state
    }
}
