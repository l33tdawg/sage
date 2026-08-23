import SwiftUI

struct MemoryInspectorView: View {
    let memory: MemorySummary
    let authorLabel: String?
    let tags: [String]
    let tagsAreLoading: Bool
    let tagsError: String?
    @Binding var newTag: String
    let isMutating: Bool
    let onAddTag: () -> Void
    let onRemoveTag: (String) -> Void
    let onForget: () -> Void

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 22) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 5) {
                        Text(memory.domainTag.uppercased())
                            .font(.caption2.weight(.bold))
                            .tracking(1)
                            .foregroundStyle(CerebrumTheme.cyan)
                        Text("Memory Inspector")
                            .font(.title2.weight(.bold))
                            .fontDesign(.rounded)
                    }
                    Spacer()
                    CerebrumStatusPill(
                        text: memory.status.capitalized,
                        systemImage: memory.status == "deprecated" ? "archivebox" : "checkmark.seal",
                        tone: memory.status == "deprecated" ? .warning : .healthy
                    )
                }

                VStack(alignment: .leading, spacing: 8) {
                    Text("CONTENT").inspectorLabel()
                    Text(memory.content.isEmpty ? "No content available" : memory.content)
                        .font(.body)
                        .textSelection(.enabled)
                        .lineSpacing(3)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(14)
                        .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 11))
                }

                VStack(alignment: .leading, spacing: 9) {
                    Text("TAGS").inspectorLabel()
                    if tagsAreLoading {
                        ProgressView().controlSize(.small)
                    } else if let tagsError {
                        Label(tagsError, systemImage: "exclamationmark.circle")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else if tags.isEmpty {
                        Text("No tags").font(.callout).foregroundStyle(.secondary)
                    } else {
                        FlowLayout(spacing: 6) {
                            ForEach(tags, id: \.self) { tag in
                                HStack(spacing: 4) {
                                    Text(tag)
                                    Button {
                                        onRemoveTag(tag)
                                    } label: {
                                        Image(systemName: "xmark.circle.fill")
                                            .symbolRenderingMode(.hierarchical)
                                    }
                                    .buttonStyle(.plain)
                                    .help("Remove tag \(tag)")
                                    .accessibilityLabel("Remove tag \(tag)")
                                    .disabled(isMutating)
                                }
                                .font(.caption.weight(.medium))
                                .padding(.horizontal, 8)
                                .padding(.vertical, 4)
                                .background(CerebrumTheme.cyan.opacity(0.10), in: Capsule())
                                .foregroundStyle(CerebrumTheme.cyan)
                            }
                        }
                    }
                    HStack {
                        TextField("Add tag", text: $newTag)
                            .textFieldStyle(.roundedBorder)
                            .onSubmit(onAddTag)
                            .disabled(isMutating)
                        Button(action: onAddTag) {
                            Image(systemName: "plus")
                        }
                        .help("Add tag")
                        .disabled(newTag.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || isMutating)
                    }
                }

                VStack(alignment: .leading, spacing: 8) {
                    HStack {
                    Text("STORED CONFIDENCE").inspectorLabel()
                        Spacer()
                        Text(memory.confidenceScore, format: .percent.precision(.fractionLength(0)))
                            .font(.callout.weight(.semibold))
                            .foregroundStyle(confidenceColor)
                    }
                    ProgressView(value: memory.confidenceScore)
                        .tint(confidenceColor)
                        .accessibilityLabel("Stored confidence")
                        .accessibilityValue(Text(memory.confidenceScore, format: .percent))
                }

                Grid(alignment: .leading, horizontalSpacing: 18, verticalSpacing: 12) {
                    inspectorRow("Type", memory.memoryType.capitalized)
                    inspectorRow("Created", memory.createdAt.formatted(date: .abbreviated, time: .shortened))
                    inspectorRow("Author", authorLabel ?? memory.submittingAgent)
                    inspectorRow("Provider", memory.provider ?? "Unknown")
                    inspectorRow("Corroborations", (memory.corroborationCount ?? 0).formatted())
                }

                Divider()
                VStack(alignment: .leading, spacing: 10) {
                    Text("LEDGER IDENTITY").inspectorLabel()
                    technicalValue("Memory ID", memory.memoryID)
                    if let hash = memory.contentHash { technicalValue("Content hash", hash) }
                    if let parent = memory.parentHash { technicalValue("Parent hash", parent) }
                }

                Divider()
                Button("Forget Memory…", systemImage: "archivebox") {
                    onForget()
                }
                .foregroundStyle(.red)
                .disabled(isMutating)
                .help("Move this memory to audit-only history")
            }
            .padding(20)
        }
        .frame(minWidth: 320, idealWidth: 380)
        .background(Color(nsColor: .windowBackgroundColor))
        .navigationTitle("Memory Inspector")
    }

    @ViewBuilder
    private func inspectorRow(_ label: String, _ value: String) -> some View {
        GridRow {
            Text(label).foregroundStyle(.secondary)
            Text(value).textSelection(.enabled)
        }
    }

    private func technicalValue(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(label).font(.caption).foregroundStyle(.secondary)
            Text(value).font(.system(.caption, design: .monospaced)).textSelection(.enabled)
        }
    }

    private var confidenceColor: Color {
        if memory.confidenceScore >= 0.8 { return CerebrumTheme.green }
        if memory.confidenceScore >= 0.5 { return CerebrumTheme.amber }
        return .red
    }
}

private struct FlowLayout: Layout {
    let spacing: CGFloat

    func sizeThatFits(proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) -> CGSize {
        layout(proposal: proposal, subviews: subviews).size
    }

    func placeSubviews(in bounds: CGRect, proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) {
        let result = layout(proposal: ProposedViewSize(width: bounds.width, height: proposal.height), subviews: subviews)
        for (index, point) in result.points.enumerated() {
            subviews[index].place(at: CGPoint(x: bounds.minX + point.x, y: bounds.minY + point.y), proposal: .unspecified)
        }
    }

    private func layout(proposal: ProposedViewSize, subviews: Subviews) -> (size: CGSize, points: [CGPoint]) {
        let width = proposal.width ?? .infinity
        var points: [CGPoint] = []
        var x: CGFloat = 0
        var y: CGFloat = 0
        var lineHeight: CGFloat = 0
        for subview in subviews {
            let size = subview.sizeThatFits(.unspecified)
            if x > 0, x + size.width > width {
                x = 0
                y += lineHeight + spacing
                lineHeight = 0
            }
            points.append(CGPoint(x: x, y: y))
            x += size.width + spacing
            lineHeight = max(lineHeight, size.height)
        }
        return (CGSize(width: proposal.width ?? x, height: y + lineHeight), points)
    }
}

private extension View {
    func inspectorLabel() -> some View {
        font(.caption2.weight(.bold)).tracking(0.8).foregroundStyle(.secondary)
    }
}
