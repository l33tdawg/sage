import SwiftUI

struct OverviewView: View {
    @Environment(\.scenePhase) private var scenePhase
    @State private var model: OverviewViewModel

    init(api: any SAGEAPI) {
        _model = State(initialValue: OverviewViewModel(api: api))
    }

    var body: some View {
        ZStack {
            CerebrumBackdrop()
            ScrollView {
                LazyVStack(alignment: .leading, spacing: CerebrumTheme.sectionSpacing) {
                    pageHeader
                    nodeHero
                    headlineMetrics
                    detailsGrid
                }
                .padding(CerebrumTheme.pagePadding)
                .frame(maxWidth: 1240, alignment: .leading)
            }
        }
        .navigationTitle("Overview")
        .toolbar {
            ToolbarItem {
                Button {
                    Task { await model.refresh() }
                } label: {
                    Label(model.isRefreshing ? "Refreshing" : "Refresh", systemImage: "arrow.clockwise")
                }
                .disabled(model.isRefreshing)
                .keyboardShortcut("r", modifiers: .command)
            }
        }
        .task(id: scenePhase) {
            guard scenePhase == .active else { return }
            await model.runLiveUpdates()
        }
    }

    private var pageHeader: some View {
        CerebrumPageContextBar(
            routeTitle: "Overview",
            context: "Local intelligence, consensus, and network at a glance."
        ) {
            CerebrumDataStatusView(status: model.dataStatus)
        }
    }

    private var nodeHero: some View {
        ZStack {
            RoundedRectangle(cornerRadius: 18)
                .fill(CerebrumTheme.brandGradient)
            nodeHeroContent.padding(20)
        }
        .shadow(color: CerebrumTheme.cyan.opacity(0.14), radius: 14, y: 6)
        .accessibilityElement(children: .contain)
    }

    private var nodeHeroContent: some View {
        HStack(spacing: 18) {
            ZStack {
                Circle().fill(.white.opacity(0.14))
                Image(systemName: nodeIsHealthy ? "checkmark.shield.fill" : "waveform.path.ecg.rectangle")
                    .font(.system(size: 27, weight: .semibold))
                    .foregroundStyle(.white)
            }
            .frame(width: 54, height: 54)
            .accessibilityHidden(true)

            VStack(alignment: .leading, spacing: 5) {
                Text(nodeHeadline)
                    .font(.title2.weight(.bold))
                    .fontDesign(.rounded)
                    .foregroundStyle(.white)
                Text(model.health?.chain?.chainID ?? "Waiting for the local sovereign memory node")
                    .font(.callout)
                    .foregroundStyle(.white.opacity(0.78))
                    .lineLimit(1)
            }
            Spacer(minLength: 20)
            HStack(spacing: 8) {
                heroPill(model.health?.encrypted == true ? "Encrypted" : "Local", "lock.shield")
                heroPill(syncStatus, "point.3.connected.trianglepath.dotted")
            }
        }
    }

    private func heroPill(_ text: String, _ systemImage: String) -> some View {
        Label(text, systemImage: systemImage)
            .font(.caption.weight(.semibold))
            .foregroundStyle(.white)
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(.white.opacity(0.13), in: Capsule())
    }

    private var headlineMetrics: some View {
        CerebrumCard("At a glance", subtitle: "Local telemetry", systemImage: "waveform.path.ecg") {
            CerebrumMetricGrid(metrics: [
                .init("Memories", model.stats?.totalMemories.formatted() ?? "—", systemImage: "brain"),
                .init("Block", model.health?.chain?.blockHeight ?? "—", systemImage: "cube.transparent"),
                .init("Peers", model.health?.chain?.peers.map(String.init) ?? "—", systemImage: "link"),
                .init("Agents", model.agents?.agents.count.formatted() ?? "—", systemImage: "person.3"),
                .init("Uptime", model.health?.uptime ?? "—", systemImage: "clock"),
            ], minimumWidth: 125)
        }
    }

    private var detailsGrid: some View {
        LazyVGrid(
            columns: [GridItem(.adaptive(minimum: 360), spacing: 18, alignment: .top)],
            alignment: .leading,
            spacing: 18
        ) {
            memoryCard
            chainCard
            intelligenceCard
            networkCard
            federationCard
        }
    }

    private var memoryCard: some View {
        CerebrumCard("Memory", subtitle: "Stored sovereign knowledge", systemImage: "brain", stale: model.statsAreStale) {
            CerebrumMetricGrid(metrics: [
                .init("Stored", model.stats?.totalMemories.formatted() ?? "—"),
                .init("Domains", model.stats?.byDomain.count.formatted() ?? "—"),
                .init("Agents represented", model.stats?.byAgent?.count.formatted() ?? "—"),
                .init("Database", databaseSize),
            ])
        }
    }

    private var chainCard: some View {
        CerebrumCard("Consensus", subtitle: "Local chain health", systemImage: "point.3.connected.trianglepath.dotted", stale: model.healthIsStale) {
            CerebrumMetricGrid(metrics: [
                .init("Block height", model.health?.chain?.blockHeight ?? "—"),
                .init("Sync", syncStatus),
                .init("App version", model.health?.chain?.appVersion.map { "v\($0)" } ?? "—"),
                .init("Pending txs", model.health?.chain?.mempoolTransactions ?? "—"),
            ])
        }
    }

    private var intelligenceCard: some View {
        CerebrumCard("Memory intelligence", subtitle: "Embedding and recall runtime", systemImage: "sparkles", stale: model.healthIsStale) {
            CerebrumMetricGrid(metrics: [
                .init("Provider", model.health?.embedder?.provider ?? "—"),
                .init("Runtime", model.health?.embedder?.online == true ? "Online" : "Offline"),
                .init("Model", model.health?.embedder?.model ?? "—"),
                .init("Dimensions", model.health?.embedder?.dimension.formatted() ?? "—"),
                .init("Semantic recall", model.health?.embedder?.semantic == true ? "Enabled" : "Limited"),
                .init("Reranker", model.health?.embedder?.reranker?.enabled == true ? "Enabled" : "Off"),
            ])
        }
    }

    private var networkCard: some View {
        CerebrumCard("Agents & validators", subtitle: "Participants in your memory network", systemImage: "person.3.sequence", stale: model.agentsAreStale || model.validatorsAreStale) {
            CerebrumMetricGrid(metrics: [
                .init("Registered agents", model.agents?.agents.count.formatted() ?? "—"),
                .init("Active agents", activeAgentCount),
                .init("Validators", model.validators?.count.formatted() ?? "—"),
                .init("Voting power", model.validators?.totalVotingPower ?? "—"),
                .init("Validator health", validatorHealth),
                .init("Agent memories", agentMemoryCount),
            ])
        }
    }

    private var federationCard: some View {
        CerebrumCard("Federation", subtitle: "Trusted SAGE connections", systemImage: "network", stale: model.federationIsStale) {
            CerebrumMetricGrid(metrics: [
                .init("Network", model.federation?.localNetworkName ?? "—"),
                .init("Connections", federationConnectionCount),
                .init("Active links", activeFederationCount),
                .init("Sharing", federationSharingStatus),
            ])
        }
    }

    private var nodeIsHealthy: Bool {
        model.health?.sage == "running" && model.health?.chain != nil && model.health?.chain?.stuck != true
    }

    private var nodeHeadline: String {
        guard model.health != nil else { return "Connecting to your SAGE" }
        if model.healthIsStale {
            return nodeIsHealthy ? "Last known: your SAGE was online" : "Last known: your SAGE needed attention"
        }
        return nodeIsHealthy ? "Your SAGE is online" : "Your SAGE needs attention"
    }

    private var syncStatus: String {
        guard let catchingUp = model.health?.chain?.catchingUp else { return "Unknown" }
        return catchingUp ? "Catching up" : "In sync"
    }

    private var databaseSize: String {
        guard let bytes = model.stats?.databaseSizeBytes else { return "—" }
        return ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
    }

    private var activeAgentCount: String {
        guard let agents = model.agents?.agents else { return "—" }
        return agents.filter { $0.status?.lowercased() == "active" }.count.formatted()
    }

    private var agentMemoryCount: String {
        guard let agents = model.agents?.agents else { return "—" }
        return agents.compactMap(\.memoryCount).reduce(0, +).formatted()
    }

    private var validatorHealth: String {
        guard let validators = model.validators else { return "—" }
        return validators.error == nil ? "Available" : "Degraded"
    }

    private var federationConnectionCount: String {
        guard let federation = model.federation else { return "—" }
        return federation.isEnabled ? federation.connections.count.formatted() : "Not enabled"
    }

    private var activeFederationCount: String {
        guard let federation = model.federation, federation.isEnabled else { return "—" }
        return federation.connections.filter { $0.status == "active" && !$0.expired }.count.formatted()
    }

    private var pausedFederationCount: Int {
        model.federation?.connections.filter(\.sharingPaused).count ?? 0
    }

    private var federationSharingStatus: String {
        guard let federation = model.federation else { return "—" }
        guard federation.isEnabled else { return "Not enabled" }
        return pausedFederationCount == 0 ? "Available" : "Partly paused"
    }
}
