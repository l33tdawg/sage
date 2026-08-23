import AppKit
import SwiftUI

enum CerebrumTheme {
    static let cyan = Color(red: 0.02, green: 0.71, blue: 0.83)
    static let violet = Color(red: 0.55, green: 0.36, blue: 0.96)
    static let green = Color(red: 0.06, green: 0.73, blue: 0.51)
    static let amber = Color(red: 0.96, green: 0.62, blue: 0.04)

    static let brandGradient = LinearGradient(
        colors: [cyan, violet],
        startPoint: .topLeading,
        endPoint: .bottomTrailing
    )

    static let pagePadding: CGFloat = 28
    static let sectionSpacing: CGFloat = 20
    static let cardRadius: CGFloat = 14
}

struct CerebrumBackdrop: View {
    var body: some View {
        ZStack {
            Color(nsColor: .windowBackgroundColor)
            RadialGradient(
                colors: [CerebrumTheme.cyan.opacity(0.10), .clear],
                center: .topLeading,
                startRadius: 0,
                endRadius: 560
            )
            RadialGradient(
                colors: [CerebrumTheme.violet.opacity(0.07), .clear],
                center: .bottomTrailing,
                startRadius: 0,
                endRadius: 620
            )
        }
        .ignoresSafeArea()
        .accessibilityHidden(true)
    }
}

struct CerebrumBrandMark: View {
    var size: CGFloat = 38

    var body: some View {
        Image(systemName: "brain.head.profile.fill")
            .font(.system(size: size * 0.55, weight: .semibold))
            .foregroundStyle(.white)
            .frame(width: size, height: size)
            .background(CerebrumTheme.brandGradient, in: RoundedRectangle(cornerRadius: size * 0.28))
            .shadow(color: CerebrumTheme.cyan.opacity(0.18), radius: 8, y: 3)
            .accessibilityHidden(true)
    }
}

struct CerebrumPageHeader<Trailing: View>: View {
    let eyebrow: String
    let title: String
    let subtitle: String
    @ViewBuilder let trailing: Trailing

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 20) {
            VStack(alignment: .leading, spacing: 5) {
                Text(eyebrow.uppercased())
                    .font(.caption.weight(.bold))
                    .tracking(1.4)
                    .foregroundStyle(CerebrumTheme.cyan)
                Text(title)
                    .font(.largeTitle.weight(.bold))
                    .fontDesign(.rounded)
                Text(subtitle)
                    .font(.callout)
                    .foregroundStyle(.secondary)
            }
            Spacer(minLength: 24)
            trailing
        }
    }
}

struct CerebrumStatusPill: View {
    enum Tone {
        case healthy, warning, critical, neutral

        var color: Color {
            switch self {
            case .healthy: CerebrumTheme.green
            case .warning: CerebrumTheme.amber
            case .critical: .red
            case .neutral: .secondary
            }
        }
    }

    let text: String
    let systemImage: String
    let tone: Tone

    var body: some View {
        Label(text, systemImage: systemImage)
            .font(.caption.weight(.semibold))
            .foregroundStyle(tone.color)
            .padding(.horizontal, 9)
            .padding(.vertical, 5)
            .background(tone.color.opacity(0.11), in: Capsule())
            .overlay(Capsule().stroke(tone.color.opacity(0.20), lineWidth: 1))
            .accessibilityElement(children: .combine)
    }
}

struct CerebrumCard<Content: View>: View {
    @Environment(\.colorSchemeContrast) private var contrast
    let title: String
    let subtitle: String?
    let systemImage: String
    let stale: Bool
    @ViewBuilder let content: Content

    init(
        _ title: String,
        subtitle: String? = nil,
        systemImage: String,
        stale: Bool = false,
        @ViewBuilder content: () -> Content
    ) {
        self.title = title
        self.subtitle = subtitle
        self.systemImage = systemImage
        self.stale = stale
        self.content = content()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack(alignment: .top, spacing: 11) {
                Image(systemName: systemImage)
                    .font(.body.weight(.semibold))
                    .foregroundStyle(CerebrumTheme.cyan)
                    .frame(width: 30, height: 30)
                    .background(CerebrumTheme.cyan.opacity(0.10), in: RoundedRectangle(cornerRadius: 8))
                    .accessibilityHidden(true)
                VStack(alignment: .leading, spacing: 2) {
                    Text(title).font(.headline)
                        .accessibilityAddTraits(.isHeader)
                    if let subtitle {
                        Text(subtitle)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
                Spacer()
                if stale {
                    CerebrumStatusPill(text: "Stale", systemImage: "clock.badge.exclamationmark", tone: .warning)
                }
            }
            Divider().opacity(0.55)
            content
        }
        .padding(18)
        .background(
            Color(nsColor: .controlBackgroundColor).opacity(0.94),
            in: RoundedRectangle(cornerRadius: CerebrumTheme.cardRadius)
        )
        .overlay(
            RoundedRectangle(cornerRadius: CerebrumTheme.cardRadius)
                .stroke(Color(nsColor: .separatorColor).opacity(contrast == .increased ? 0.80 : 0.46), lineWidth: 1)
        )
        .shadow(color: .black.opacity(0.035), radius: 2, y: 1)
    }
}

struct CerebrumMetric: Identifiable {
    let id: String
    let label: String
    let value: String
    let systemImage: String?

    init(_ label: String, _ value: String, systemImage: String? = nil) {
        id = label
        self.label = label
        self.value = value
        self.systemImage = systemImage
    }
}

struct CerebrumMetricGrid: View {
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    let metrics: [CerebrumMetric]
    var minimumWidth: CGFloat = 135

    var body: some View {
        LazyVGrid(
            columns: [GridItem(
                .adaptive(minimum: dynamicTypeSize.isAccessibilitySize ? max(minimumWidth, 220) : minimumWidth),
                spacing: 14,
                alignment: .leading
            )],
            alignment: .leading,
            spacing: 16
        ) {
            ForEach(metrics) { metric in
                VStack(alignment: .leading, spacing: 5) {
                    HStack(spacing: 5) {
                        if let systemImage = metric.systemImage {
                            Image(systemName: systemImage).accessibilityHidden(true)
                        }
                        Text(metric.label)
                    }
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    Text(metric.value)
                        .font(.title3.weight(.semibold))
                        .fontDesign(.rounded)
                        .monospacedDigit()
                        .fixedSize(horizontal: false, vertical: true)
                        .textSelection(.enabled)
                        .contentTransition(.numericText())
                        .animation(reduceMotion ? nil : .snappy(duration: 0.32), value: metric.value)
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(metric.label)
                .accessibilityValue(metric.value)
            }
        }
    }
}

struct CerebrumLiveIndicator: View {
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @State private var pulsing = false
    let connected: Bool

    var body: some View {
        HStack(spacing: 7) {
            ZStack {
                if connected && !reduceMotion {
                    Circle()
                        .fill(CerebrumTheme.green.opacity(0.25))
                        .scaleEffect(pulsing ? 1.9 : 1)
                        .opacity(pulsing ? 0 : 0.75)
                }
                Circle()
                    .fill(connected ? CerebrumTheme.green : Color.secondary)
            }
            .frame(width: 7, height: 7)
            Text(connected ? "Live" : "Polling")
                .font(.caption.weight(.semibold))
                .foregroundStyle(connected ? CerebrumTheme.green : .secondary)
        }
        .accessibilityLabel(connected ? "Live updates connected" : "Using periodic updates")
        .onAppear {
            guard !reduceMotion else { return }
            withAnimation(.easeOut(duration: 1.6).repeatForever(autoreverses: false)) {
                pulsing = true
            }
        }
    }
}
