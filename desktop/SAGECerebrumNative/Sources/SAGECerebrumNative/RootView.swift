import SwiftUI

struct RootView: View {
    @Bindable var session: AppSession

    var body: some View {
        ZStack {
            CerebrumBackdrop()
            switch session.phase {
            case .connecting:
                NativeStateView(
                    title: "Connecting to SAGE",
                    message: "Discovering your local sovereign memory node.",
                    systemImage: "brain.head.profile",
                    progress: true
                )
                .task { await session.connect() }
            case .locked:
                LoginView(session: session)
            case let .failed(message):
                NativeStateView(
                    title: "SAGE is unavailable",
                    message: message,
                    systemImage: "exclamationmark.triangle",
                    actionTitle: "Try Again"
                ) { Task { await session.connect() } }
            case .ready:
                if let api = session.api { nativeApplication(api: api) }
            }
        }
        .tint(CerebrumTheme.cyan)
        .preferredColorScheme(designPreviewColorScheme)
    }

    private var designPreviewColorScheme: ColorScheme? {
        #if DEBUG
        ProcessInfo.processInfo.environment["SAGE_NATIVE_PREVIEW_DARK"] == "1" ? .dark : nil
        #else
        nil
        #endif
    }

    private func nativeApplication(api: any SAGEAPI) -> some View {
        NavigationSplitView {
            VStack(spacing: 0) {
                sidebarBrand
                List(selection: $session.route) {
                    sidebarSection("Intelligence", routes: [.overview, .brain, .search])
                    sidebarSection("Workflow", routes: [.tasks, .importData])
                    sidebarSection("Network", routes: [.network, .access, .federation])
                    sidebarSection("System", routes: [.settings])
                }
                .listStyle(.sidebar)
                sidebarFooter
            }
            .background(.thinMaterial)
            .navigationSplitViewColumnWidth(min: 200, ideal: 224, max: 270)
        } detail: {
            destination(for: session.route, api: api)
        }
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Menu {
                    Button("Lock CEREBRUM", systemImage: "lock") {
                        Task { await session.lock() }
                    }
                    .keyboardShortcut("l", modifiers: .command)
                    Divider()
                    Text("Local session")
                } label: {
                    Label("Session", systemImage: "lock.shield")
                }
                .help("Lock CEREBRUM and manage the local session")
            }
        }
    }

    private var sidebarBrand: some View {
        HStack(spacing: 11) {
            CerebrumBrandMark(size: 34)
            VStack(alignment: .leading, spacing: 1) {
                Text("CEREBRUM")
                    .font(.headline.weight(.bold))
                    .fontDesign(.rounded)
                Text("SAGE native")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
        .padding(.horizontal, 14)
        .padding(.top, 14)
        .padding(.bottom, 10)
    }

    private func sidebarSection(_ title: String, routes: [AppRoute]) -> some View {
        Section(title) {
            ForEach(routes) { route in
                Label(route.title, systemImage: route.systemImage)
                    .tag(route)
                    .accessibilityLabel(route.title)
            }
        }
    }

    private var sidebarFooter: some View {
        HStack(spacing: 8) {
            Circle()
                .fill(CerebrumTheme.green)
                .frame(width: 7, height: 7)
                .accessibilityHidden(true)
            Text("Local SAGE connected")
                .font(.caption)
                .foregroundStyle(.secondary)
            Spacer()
        }
        .padding(.horizontal, 15)
        .padding(.vertical, 12)
        .overlay(alignment: .top) { Divider() }
        .accessibilityElement(children: .combine)
    }

    @ViewBuilder
    private func destination(for route: AppRoute, api: any SAGEAPI) -> some View {
        switch route {
        case .overview: OverviewView(api: api)
        case .brain:
            #if DEBUG
            if let fixture = NativeAXAcceptanceFixture() {
                fixture.makeBrainView(api: api)
            } else {
                BrainView(api: api)
            }
            #else
            BrainView(api: api)
            #endif
        case .search: SearchView(api: api)
        default: NativePlaceholderView(route: route)
        }
    }
}

private struct LoginView: View {
    @Bindable var session: AppSession
    @FocusState private var passphraseFocused: Bool

    var body: some View {
        VStack(spacing: 0) {
            Spacer(minLength: 36)
            VStack(spacing: 22) {
                CerebrumBrandMark(size: 64)
                VStack(spacing: 7) {
                    Text("Welcome back")
                        .font(.largeTitle.weight(.bold))
                        .fontDesign(.rounded)
                    Text("Unlock CEREBRUM to enter your sovereign memory.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                }

                VStack(alignment: .leading, spacing: 9) {
                    Text("VAULT PASSPHRASE")
                        .font(.caption2.weight(.bold))
                        .tracking(0.9)
                        .foregroundStyle(.secondary)
                    SecureField("Enter passphrase", text: $session.passphrase)
                        .textFieldStyle(.roundedBorder)
                        .controlSize(.large)
                        .onSubmit { Task { await session.login() } }
                        .focused($passphraseFocused)
                        .disabled(session.isLoggingIn)
                        .accessibilityHint("Unlocks the local encrypted SAGE vault")
                    if let error = session.loginError {
                        Label(error, systemImage: "exclamationmark.circle.fill")
                            .font(.callout)
                            .foregroundStyle(.red)
                            .accessibilityLabel("Unlock failed: \(error)")
                    }
                }

                Button { Task { await session.login() } } label: {
                    HStack(spacing: 8) {
                        if session.isLoggingIn { ProgressView().controlSize(.small) }
                        Text(session.isLoggingIn ? "Unlocking…" : "Unlock CEREBRUM")
                    }
                    .frame(maxWidth: .infinity)
                }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.large)
                    .keyboardShortcut(.defaultAction)
                    .disabled(session.passphrase.isEmpty || session.isLoggingIn)
                    .frame(maxWidth: .infinity)

                HStack(spacing: 14) {
                    Label("Local", systemImage: "desktopcomputer")
                    Label("Encrypted", systemImage: "lock.shield")
                    Label("Sovereign", systemImage: "person.crop.circle.badge.checkmark")
                }
                .font(.caption)
                .foregroundStyle(.secondary)
            }
            .padding(34)
            .frame(width: 440)
            .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 20))
            .overlay(RoundedRectangle(cornerRadius: 20).stroke(Color.primary.opacity(0.09)))
            .shadow(color: .black.opacity(0.10), radius: 24, y: 10)
            Spacer(minLength: 36)
            Text("SAGE · Sovereign Agent Governed Experience")
                .font(.caption2)
                .foregroundStyle(.tertiary)
                .padding(.bottom, 18)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .task { passphraseFocused = true }
        .onChange(of: session.loginFailureID) { _, _ in passphraseFocused = true }
    }
}

private struct NativeStateView: View {
    let title: String
    let message: String
    let systemImage: String
    var progress = false
    var actionTitle: String?
    var action: (() -> Void)?

    var body: some View {
        VStack(spacing: 17) {
            CerebrumBrandMark(size: 56)
            Label(title, systemImage: systemImage)
                .font(.title2.weight(.bold))
                .fontDesign(.rounded)
            Text(message)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .frame(maxWidth: 440)
            if progress { ProgressView().controlSize(.small) }
            if let actionTitle, let action {
                Button(actionTitle, action: action)
                    .buttonStyle(.borderedProminent)
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(36)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 18))
        .overlay(RoundedRectangle(cornerRadius: 18).stroke(Color.primary.opacity(0.08)))
    }
}

private struct NativePlaceholderView: View {
    let route: AppRoute

    var body: some View {
        ZStack {
            CerebrumBackdrop()
            ContentUnavailableView {
                Label(route.title, systemImage: route.systemImage)
            } description: {
                Text("The native \(route.title) surface is mapped and queued for its 1:1 CEREBRUM implementation.")
            }
        }
        .navigationTitle(route.title)
    }
}
