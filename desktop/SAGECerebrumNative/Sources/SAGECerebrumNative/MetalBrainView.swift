import MetalKit
import SwiftUI
import simd

enum BrainMetalLayout: Equatable {
    case memory
    case connectome
}

struct BrainRenderEdgeID: Equatable, Hashable, Sendable {
    let source: String
    let target: String
    let type: String
}

enum BrainMetalPick: Equatable, Sendable {
    case node(String)
    case edge(BrainRenderEdgeID)
    case background
}

struct MetalBrainView: NSViewRepresentable {
    let nodes: [BrainNode]
    let edges: [BrainEdge]
    let selectedID: String?
    let highlightedEdge: BrainEdge?
    let layout: BrainMetalLayout
    let autoRotate: Bool
    let flow: Bool
    let hullOpacity: Double
    let onPick: (BrainMetalPick) -> Void

    func makeCoordinator() -> BrainMetalRenderer? { BrainMetalRenderer(onPick: onPick) }

    func makeNSView(context: Context) -> InteractiveMetalView {
        let device = context.coordinator?.metalDevice
        let view = InteractiveMetalView(frame: .zero, device: device)
        view.colorPixelFormat = .bgra8Unorm_srgb
        view.depthStencilPixelFormat = .depth32Float
        view.sampleCount = 4
        view.framebufferOnly = false
        view.preferredFramesPerSecond = 60
        view.enableSetNeedsDisplay = true
        view.isPaused = false
        view.clearColor = MTLClearColor(red: 0.004, green: 0.004, blue: 0.008, alpha: 1)
        view.renderer = context.coordinator
        view.delegate = context.coordinator
        view.setAccessibilityLabel("Interactive memory brain MRI")
        view.setAccessibilityHelp("Drag to orbit, scroll to zoom, or switch to Accessible Table for the same memories.")
        context.coordinator?.attach(view)
        if context.coordinator == nil {
            view.setAccessibilityLabel("Memory MRI unavailable")
            view.setAccessibilityHelp("Metal could not initialize. Switch to Accessible Table to inspect the same memories.")
        }
        return view
    }

    func updateNSView(_ view: InteractiveMetalView, context: Context) {
        context.coordinator?.onPick = onPick
        context.coordinator?.update(nodes: nodes, edges: edges, highlightedEdge: highlightedEdge, layout: layout)
        context.coordinator?.setSelectedID(
            selectedID,
            reduceMotion: NSWorkspace.shared.accessibilityDisplayShouldReduceMotion
        )
        context.coordinator?.autoRotate = autoRotate
        context.coordinator?.flow = flow
        context.coordinator?.hullOpacity = Float(hullOpacity)
        view.isPaused = !(autoRotate || flow)
        view.setAccessibilityLabel(layout == .memory ? "Interactive memory brain MRI" : "Interactive agent connectome MRI")
        view.setAccessibilityHelp(
            layout == .memory
                ? "Drag to orbit, scroll to zoom, or switch to Table for complete keyboard-accessible memory inspection."
                : "Drag to orbit, scroll to zoom, or switch to Table and the agent inspector for complete keyboard-accessible agent, engram, and directed-connection inspection."
        )
        if view.isPaused { view.setNeedsDisplay(view.bounds) }
    }
}

final class InteractiveMetalView: MTKView {
    weak var renderer: BrainMetalRenderer?
    private var lastDragPoint: CGPoint?
    private var mouseDownPoint: CGPoint?
    private var didCrossDragThreshold = false

    override var acceptsFirstResponder: Bool { true }

    override func mouseDown(with event: NSEvent) {
        window?.makeFirstResponder(self)
        let point = convert(event.locationInWindow, from: nil)
        mouseDownPoint = point
        lastDragPoint = point
        didCrossDragThreshold = false
    }

    override func mouseDragged(with event: NSEvent) {
        let point = convert(event.locationInWindow, from: nil)
        if let lastDragPoint {
            renderer?.orbit(deltaX: Float(point.x - lastDragPoint.x), deltaY: Float(point.y - lastDragPoint.y))
        }
        if let mouseDownPoint, hypot(point.x - mouseDownPoint.x, point.y - mouseDownPoint.y) >= 6 {
            didCrossDragThreshold = true
        }
        lastDragPoint = point
    }

    override func mouseUp(with event: NSEvent) {
        let point = convert(event.locationInWindow, from: nil)
        if mouseDownPoint != nil, !didCrossDragThreshold {
            renderer?.select(at: point, viewSize: bounds.size, backingScale: Float(window?.backingScaleFactor ?? 1))
        }
        mouseDownPoint = nil
        lastDragPoint = nil
        didCrossDragThreshold = false
    }

    override func scrollWheel(with event: NSEvent) {
        renderer?.zoom(by: Float(event.scrollingDeltaY))
    }

    override func keyDown(with event: NSEvent) {
        if event.keyCode == 53 { renderer?.onPick(.background) }
        else { super.keyDown(with: event) }
    }
}

final class BrainMetalRenderer: NSObject, MTKViewDelegate, @unchecked Sendable {
    var onPick: (BrainMetalPick) -> Void
    private(set) var selectedID: String?
    var autoRotate = true
    var flow = true
    var hullOpacity: Float = 0.18

    private weak var view: MTKView?
    let metalDevice: MTLDevice
    private let queue: MTLCommandQueue
    private let pipeline: MTLRenderPipelineState
    private let additivePipeline: MTLRenderPipelineState
    private let billboardPipeline: MTLRenderPipelineState
    private let additiveBillboardPipeline: MTLRenderPipelineState
    private let ribbonPipeline: MTLRenderPipelineState
    private let additiveRibbonPipeline: MTLRenderPipelineState
    private let bloomExtractPipeline: MTLRenderPipelineState?
    private let bloomBlurPipeline: MTLRenderPipelineState?
    private let bloomCompositePipeline: MTLRenderPipelineState?
    private let bloomSampler: MTLSamplerState?
    private let depthState: MTLDepthStencilState
    private var nodeVertices: [BrainMetalVertex] = []
    private var nodeBillboardVertices: [BrainBillboardVertex] = []
    private var haloBillboardVertices: [BrainBillboardVertex] = []
    private var ribbonVertices: [BrainRibbonVertex] = []
    private var flowVertices: [BrainMetalVertex] = []
    private var flowSegments: [BrainFlowSegment] = []
    private var renderedRibbonEdges: [BrainRenderedRibbon] = []
    private var hullVertices: [BrainMetalVertex] = []
    private var nodeBillboardBuffer: MTLBuffer?
    private var haloBillboardBuffer: MTLBuffer?
    private var ribbonBuffer: MTLBuffer?
    private var flowBuffers: [MTLBuffer?] = Array(repeating: nil, count: 3)
    private var flowFrameIndex = 0
    private let inFlightFrames = DispatchSemaphore(value: 3)
    private var bloomTargets: BrainBloomTargets?
    private var hullBuffer: MTLBuffer?
    private var nodeIDs: [String] = []
    private var renderedNodes: [BrainNode] = []
    private var renderedEdges: [BrainEdge] = []
    private var renderedHighlightedEdge: BrainEdge?
    private var renderedLayout: BrainMetalLayout = .memory
    private var yaw: Float = 0.72
    private var pitch: Float = -0.08
    private var cameraDistance: Float = 7.05
    private var cameraFocus = SIMD3<Float>.zero
    private var cameraFocusTarget = SIMD3<Float>.zero
    private var focusAnimation: BrainFocusAnimation?
    private var lastFrame = CACurrentMediaTime()
    private var lastPlasticityRefresh = Date.distantPast
    private var lastMVP = matrix_identity_float4x4

    init?(onPick: @escaping (BrainMetalPick) -> Void) {
        guard let device = MTLCreateSystemDefaultDevice(), let queue = device.makeCommandQueue() else {
            return nil
        }
        self.onPick = onPick
        self.metalDevice = device
        self.queue = queue
        guard let library = try? device.makeLibrary(source: Self.shaderSource, options: nil) else { return nil }
        let descriptor = MTLRenderPipelineDescriptor()
        descriptor.vertexFunction = library.makeFunction(name: "brainVertex")
        descriptor.fragmentFunction = library.makeFunction(name: "brainFragment")
        descriptor.colorAttachments[0].pixelFormat = .bgra8Unorm_srgb
        descriptor.depthAttachmentPixelFormat = .depth32Float
        descriptor.rasterSampleCount = 4
        descriptor.colorAttachments[0].isBlendingEnabled = true
        descriptor.colorAttachments[0].sourceRGBBlendFactor = .sourceAlpha
        descriptor.colorAttachments[0].destinationRGBBlendFactor = .oneMinusSourceAlpha
        guard let pipeline = try? device.makeRenderPipelineState(descriptor: descriptor) else { return nil }
        self.pipeline = pipeline
        descriptor.colorAttachments[0].sourceRGBBlendFactor = .sourceAlpha
        descriptor.colorAttachments[0].destinationRGBBlendFactor = .one
        guard let additivePipeline = try? device.makeRenderPipelineState(descriptor: descriptor) else { return nil }
        self.additivePipeline = additivePipeline
        descriptor.vertexFunction = library.makeFunction(name: "billboardVertex")
        descriptor.fragmentFunction = library.makeFunction(name: "billboardFragment")
        descriptor.colorAttachments[0].sourceRGBBlendFactor = .sourceAlpha
        descriptor.colorAttachments[0].destinationRGBBlendFactor = .oneMinusSourceAlpha
        guard let billboardPipeline = try? device.makeRenderPipelineState(descriptor: descriptor) else { return nil }
        self.billboardPipeline = billboardPipeline
        descriptor.colorAttachments[0].destinationRGBBlendFactor = .one
        guard let additiveBillboardPipeline = try? device.makeRenderPipelineState(descriptor: descriptor) else { return nil }
        self.additiveBillboardPipeline = additiveBillboardPipeline
        descriptor.vertexFunction = library.makeFunction(name: "ribbonVertex")
        descriptor.fragmentFunction = library.makeFunction(name: "ribbonFragment")
        descriptor.colorAttachments[0].sourceRGBBlendFactor = .sourceAlpha
        descriptor.colorAttachments[0].destinationRGBBlendFactor = .oneMinusSourceAlpha
        guard let ribbonPipeline = try? device.makeRenderPipelineState(descriptor: descriptor) else { return nil }
        self.ribbonPipeline = ribbonPipeline
        descriptor.colorAttachments[0].destinationRGBBlendFactor = .one
        guard let additiveRibbonPipeline = try? device.makeRenderPipelineState(descriptor: descriptor) else { return nil }
        self.additiveRibbonPipeline = additiveRibbonPipeline

        let bloomDescriptor = MTLRenderPipelineDescriptor()
        bloomDescriptor.vertexFunction = library.makeFunction(name: "fullscreenVertex")
        bloomDescriptor.fragmentFunction = library.makeFunction(name: "bloomExtractFragment")
        bloomDescriptor.colorAttachments[0].pixelFormat = .rgba16Float
        self.bloomExtractPipeline = try? device.makeRenderPipelineState(descriptor: bloomDescriptor)
        bloomDescriptor.fragmentFunction = library.makeFunction(name: "bloomBlurFragment")
        self.bloomBlurPipeline = try? device.makeRenderPipelineState(descriptor: bloomDescriptor)
        bloomDescriptor.fragmentFunction = library.makeFunction(name: "bloomCompositeFragment")
        bloomDescriptor.colorAttachments[0].pixelFormat = .bgra8Unorm_srgb
        bloomDescriptor.colorAttachments[0].isBlendingEnabled = true
        bloomDescriptor.colorAttachments[0].sourceRGBBlendFactor = .one
        bloomDescriptor.colorAttachments[0].destinationRGBBlendFactor = .one
        bloomDescriptor.colorAttachments[0].sourceAlphaBlendFactor = .zero
        bloomDescriptor.colorAttachments[0].destinationAlphaBlendFactor = .one
        self.bloomCompositePipeline = try? device.makeRenderPipelineState(descriptor: bloomDescriptor)
        let samplerDescriptor = MTLSamplerDescriptor()
        samplerDescriptor.minFilter = .linear
        samplerDescriptor.magFilter = .linear
        samplerDescriptor.sAddressMode = .clampToEdge
        samplerDescriptor.tAddressMode = .clampToEdge
        self.bloomSampler = device.makeSamplerState(descriptor: samplerDescriptor)
        let depth = MTLDepthStencilDescriptor()
        depth.depthCompareFunction = .lessEqual
        depth.isDepthWriteEnabled = false
        guard let depthState = device.makeDepthStencilState(descriptor: depth) else { return nil }
        self.depthState = depthState
        super.init()
        buildHull()
        hullBuffer = makeBuffer(hullVertices)
    }

    func attach(_ view: MTKView) { self.view = view }

    func update(nodes: [BrainNode], edges: [BrainEdge], highlightedEdge: BrainEdge?, layout: BrainMetalLayout) {
        var seenIDs = Set<String>()
        let safeNodes = nodes.prefix(4_096).filter { !$0.id.isEmpty && seenIDs.insert($0.id).inserted }
        let visibleIDs = Set(safeNodes.map(\.id))
        let validEdges = edges.lazy
            .filter { visibleIDs.contains($0.source) && visibleIDs.contains($0.target) }
            .prefix(16_384)
        let boundedNodes = Array(safeNodes)
        let boundedEdges = Self.selectRenderEdges(Array(validEdges), highlighted: highlightedEdge)
        guard boundedNodes != renderedNodes || boundedEdges != renderedEdges ||
              highlightedEdge != renderedHighlightedEdge || layout != renderedLayout else { return }
        renderedNodes = boundedNodes
        renderedEdges = boundedEdges
        renderedHighlightedEdge = highlightedEdge
        renderedLayout = layout
        lastPlasticityRefresh = .now
        let positions = Self.positions(for: boundedNodes, layout: layout)
        let byID = Dictionary(uniqueKeysWithValues: zip(boundedNodes.map(\.id), positions))
        nodeIDs = boundedNodes.map(\.id)
        nodeVertices = zip(boundedNodes, positions).map { node, position in
            let appearance = Self.nodeAppearance(node)
            let baseSize = node.memoryType == "__engram__" ? 7.0 : 10.0
            let size = Float(baseSize + node.confidence * 7 + Double(min(node.corroborationCount, 5)) * 1.5)
            return .init(positionSize: SIMD4(position.x, position.y, position.z, size), color: appearance)
        }
        nodeBillboardVertices = Self.billboards(from: nodeVertices, halo: false)
        haloBillboardVertices = Self.billboards(from: nodeVertices, halo: true)
        flowSegments = []
        ribbonVertices = []
        renderedRibbonEdges = []
        let positiveWeights = boundedEdges.compactMap(\.weight).filter { $0 > 0 }.sorted()
        let percentileWeight = positiveWeights.isEmpty
            ? 1
            : positiveWeights[max(0, Int(ceil(Double(positiveWeights.count) * 0.95)) - 1)]
        let directedKeys = Set(boundedEdges.map { "\($0.source)\u{0}\($0.target)\u{0}\($0.type)" })
        for edge in boundedEdges {
            guard let source = byID[edge.source], let target = byID[edge.target] else { continue }
            let isHighlighted = Self.isSameDirectedEdge(highlightedEdge, edge)
            var color: SIMD4<Float>
            if isHighlighted {
                color = SIMD4(0.90, 0.82, 1.0, 0.92)
            } else if highlightedEdge != nil, edge.type == "synapse" {
                color = SIMD4(0.20, 0.76, 0.86, 0.10)
            } else if edge.type == "contradicts" {
                color = SIMD4(0.96, 0.31, 0.38, 0.46)
            } else if edge.type == "engram" {
                color = SIMD4(0.63, 0.43, 0.98, 0.46)
            } else if edge.type == "corroborates" {
                color = SIMD4(0.12, 0.82, 0.63, 0.38)
            } else {
                color = SIMD4(0.20, 0.76, 0.86, 0.25)
            }
            let plasticity = Self.edgePlasticity(lastFired: edge.lastFired)
            if edge.type == "synapse", !isHighlighted { color.w *= plasticity }
            let phase = Float(Self.stableUnit("\(edge.source)>\(edge.target)", seed: 29))
            flowSegments.append(.init(source: source, target: target, color: color, phase: phase))
            let normalizedWeight = Float(min(1, log1p(max(0, edge.weight ?? 0)) / log1p(max(1, percentileWeight))))
            let width = 0.65 + normalizedWeight * plasticity * 2.2 + (isHighlighted ? 2.0 : 0)
            let isLoop = edge.source == edge.target || simd_distance(source, target) <= 0.001
            let hasReverse = directedKeys.contains("\(edge.target)\u{0}\(edge.source)\u{0}\(edge.type)") && edge.source != edge.target
            let stableSign: Float = Self.stableUnit("\(min(edge.source, edge.target))|\(max(edge.source, edge.target))|\(edge.type)", seed: 41) < 0.5 ? -1 : 1
            let curvature: Float = isLoop ? 0 : (hasReverse ? 18 : 6 * stableSign)
            let loopAngle = Float(Self.stableUnit("\(edge.source)|\(edge.type)", seed: 53) * Double.pi * 2)
            let segmentCount = isLoop ? 12 : 8
            renderedRibbonEdges.append(.init(
                edge: edge, source: source, target: target, width: width,
                curvature: curvature, loopAngle: loopAngle, isLoop: isLoop
            ))
            for segment in 0 ..< segmentCount {
                let start = Float(segment) / Float(segmentCount)
                let end = Float(segment + 1) / Float(segmentCount)
                let corners: [SIMD2<Float>] = [
                    .init(start, -1), .init(start, 1), .init(end, -1),
                    .init(end, -1), .init(start, 1), .init(end, 1),
                ]
                ribbonVertices.append(contentsOf: corners.map {
                    BrainRibbonVertex(
                        source: SIMD4(source, 1), target: SIMD4(target, 1), color: color,
                        corner: $0, width: width, curvature: curvature,
                        loopAngle: loopAngle, loop: isLoop ? 1 : 0
                    )
                })
            }
        }
        nodeBillboardBuffer = makeBillboardBuffer(nodeBillboardVertices)
        haloBillboardBuffer = makeBillboardBuffer(haloBillboardVertices)
        ribbonBuffer = makeRibbonBuffer(ribbonVertices)
        flowVertices = flowSegments.map {
            .init(positionSize: SIMD4($0.source.x, $0.source.y, $0.source.z, 3.2), color: SIMD4($0.color.x, $0.color.y, $0.color.z, min(0.9, $0.color.w + 0.35)))
        }
        flowBuffers = (0 ..< 3).map { _ in makeEmptyBuffer(vertexCapacity: flowVertices.count) }
    }

    func orbit(deltaX: Float, deltaY: Float) {
        yaw += deltaX * 0.006
        pitch = min(0.9, max(-0.9, pitch - deltaY * 0.006))
        requestFrameIfPaused()
    }

    func zoom(by delta: Float) {
        cameraDistance = min(10, max(3.8, cameraDistance + delta * 0.018))
        requestFrameIfPaused()
    }

    func setSelectedID(_ newSelectedID: String?, reduceMotion: Bool) {
        let selectionChanged = newSelectedID != selectedID
        selectedID = newSelectedID
        let now = CACurrentMediaTime()
        cameraFocus = currentFocus(at: now)
        let target = newSelectedID
            .flatMap(nodeIDs.firstIndex(of:))
            .map { SIMD3(nodeVertices[$0].positionSize.x, nodeVertices[$0].positionSize.y, nodeVertices[$0].positionSize.z) }
            ?? .zero
        guard selectionChanged || simd_distance(target, cameraFocusTarget) > 0.001 else { return }
        cameraFocusTarget = target

        if reduceMotion {
            cameraFocus = target
            focusAnimation = nil
        } else {
            focusAnimation = BrainFocusAnimation(start: cameraFocus, target: target, startedAt: now, duration: 0.34)
        }
        requestFrameIfPaused()
    }

    func select(at point: CGPoint, viewSize: CGSize, backingScale: Float) {
        guard viewSize.width > 0, viewSize.height > 0 else { return }
        let backingScale = max(1, backingScale)
        var best: (index: Int, distance: CGFloat, depth: Float)?
        for (index, vertex) in nodeVertices.enumerated() {
            let clip = lastMVP * SIMD4(vertex.positionSize.x, vertex.positionSize.y, vertex.positionSize.z, 1)
            guard clip.w > 0 else { continue }
            let ndc = clip / clip.w
            guard ndc.x.isFinite, ndc.y.isFinite, ndc.z.isFinite, ndc.z >= 0, ndc.z <= 1 else { continue }
            let screen = CGPoint(x: CGFloat((ndc.x + 1) * 0.5) * viewSize.width, y: CGFloat((ndc.y + 1) * 0.5) * viewSize.height)
            let distance = hypot(point.x - screen.x, point.y - screen.y)
            let radius = max(7, CGFloat(abs(vertex.positionSize.w) / backingScale) * 0.5 + 4)
            guard distance <= radius else { continue }
            if best == nil || distance < best!.distance - 1 ||
                (abs(distance - best!.distance) <= 1 && ndc.z < best!.depth) {
                best = (index, distance, ndc.z)
            }
        }
        if let best {
            onPick(.node(nodeIDs[best.index]))
            return
        }

        var bestEdge: (edge: BrainEdge, distance: CGFloat)?
        for ribbon in renderedRibbonEdges {
            guard let points = projectedPoints(for: ribbon, viewSize: viewSize, backingScale: backingScale) else { continue }
            let distance = zip(points, points.dropFirst()).reduce(CGFloat.greatestFiniteMagnitude) {
                min($0, Self.distance(from: point, toSegmentFrom: $1.0, to: $1.1))
            }
            let tolerance = CGFloat(min(12, ribbon.width / backingScale * 0.5 + 6))
            guard distance <= tolerance else { continue }
            if bestEdge == nil || distance < bestEdge!.distance ||
                (distance == bestEdge!.distance && Self.edgeSortKey(ribbon.edge) < Self.edgeSortKey(bestEdge!.edge)) {
                bestEdge = (ribbon.edge, distance)
            }
        }
        if let edge = bestEdge?.edge {
            onPick(.edge(.init(source: edge.source, target: edge.target, type: edge.type)))
        } else {
            onPick(.background)
        }
    }

    private func projectedPoints(
        for ribbon: BrainRenderedRibbon, viewSize: CGSize, backingScale: Float
    ) -> [CGPoint]? {
        func project(_ point: SIMD3<Float>) -> CGPoint? {
            let clip = lastMVP * SIMD4(point, 1)
            guard clip.w > 0, clip.x.isFinite, clip.y.isFinite else { return nil }
            let ndc = clip / clip.w
            return CGPoint(
                x: CGFloat((ndc.x + 1) * 0.5) * viewSize.width,
                y: CGFloat((ndc.y + 1) * 0.5) * viewSize.height
            )
        }
        guard let source = project(ribbon.source), let target = project(ribbon.target) else { return nil }
        let segmentCount = ribbon.isLoop ? 12 : 8
        if ribbon.isLoop {
            let direction = CGVector(dx: CGFloat(cos(ribbon.loopAngle)), dy: CGFloat(sin(ribbon.loopAngle)))
            let radius = CGFloat((18 + ribbon.width * 2) / backingScale)
            let center = CGPoint(x: source.x + direction.dx * radius, y: source.y + direction.dy * radius)
            return (0 ... segmentCount).map { index in
                let t = CGFloat(index) / CGFloat(segmentCount)
                let theta = CGFloat(ribbon.loopAngle) + .pi + 2 * .pi * t
                return CGPoint(x: center.x + cos(theta) * radius, y: center.y + sin(theta) * radius)
            }
        }
        let dx = target.x - source.x
        let dy = target.y - source.y
        let length = max(0.0001, hypot(dx, dy))
        let normal = CGVector(dx: -dy / length, dy: dx / length)
        let curvature = CGFloat(ribbon.curvature / backingScale)
        return (0 ... segmentCount).map { index in
            let t = CGFloat(index) / CGFloat(segmentCount)
            return CGPoint(
                x: source.x + dx * t + normal.dx * curvature * sin(.pi * t),
                y: source.y + dy * t + normal.dy * curvature * sin(.pi * t)
            )
        }
    }

    private static func distance(from point: CGPoint, toSegmentFrom start: CGPoint, to end: CGPoint) -> CGFloat {
        let dx = end.x - start.x
        let dy = end.y - start.y
        let lengthSquared = dx * dx + dy * dy
        guard lengthSquared > 0 else { return hypot(point.x - start.x, point.y - start.y) }
        let t = max(0, min(1, ((point.x - start.x) * dx + (point.y - start.y) * dy) / lengthSquared))
        return hypot(point.x - (start.x + t * dx), point.y - (start.y + t * dy))
    }

    private static func edgeSortKey(_ edge: BrainEdge) -> String {
        "\(edge.source)\u{0}\(edge.target)\u{0}\(edge.type)"
    }

    func mtkView(_ view: MTKView, drawableSizeWillChange size: CGSize) {
        requestFrameIfPaused()
    }

    func draw(in view: MTKView) {
        if Date.now.timeIntervalSince(lastPlasticityRefresh) >= 30,
           renderedEdges.contains(where: { $0.type == "synapse" }) {
            let nodes = renderedNodes
            let edges = renderedEdges
            let highlighted = renderedHighlightedEdge
            let layout = renderedLayout
            renderedEdges = []
            update(nodes: nodes, edges: edges, highlightedEdge: highlighted, layout: layout)
        }
        guard inFlightFrames.wait(timeout: .now()) == .success else {
            if focusAnimation != nil { requestFrameIfPaused() }
            return
        }
        guard let pass = view.currentRenderPassDescriptor,
              let drawable = view.currentDrawable,
              let command = queue.makeCommandBuffer(),
              let encoder = command.makeRenderCommandEncoder(descriptor: pass)
        else {
            inFlightFrames.signal()
            if focusAnimation != nil { requestFrameIfPaused() }
            return
        }
        let frameSemaphore = inFlightFrames
        let now = CACurrentMediaTime()
        let delta = min(0.05, now - lastFrame)
        lastFrame = now
        if autoRotate { yaw += Float(delta) * 0.12 }
        cameraFocus = currentFocus(at: now)
        let aspect = Float(max(view.drawableSize.width, 1) / max(view.drawableSize.height, 1))
        let projection = simd_float4x4.perspective(fovY: 0.72, aspect: aspect, near: 0.1, far: 50)
        let model = simd_float4x4.translation(0, 0, -cameraDistance)
            * .rotationX(pitch)
            * .rotationY(yaw)
            * .translation(-cameraFocus.x, -cameraFocus.y, -cameraFocus.z)
        let mvp = projection * model
        lastMVP = mvp
        let flowBuffer = flowBuffers[flowFrameIndex]
        flowFrameIndex = (flowFrameIndex + 1) % flowBuffers.count
        if flow { updateFlowBuffer(flowBuffer, time: Float(now)) }
        var uniforms = BrainUniforms(
            mvp: mvp, selectedIndex: selectedID.flatMap(nodeIDs.firstIndex(of:)).map(Float.init) ?? -1,
            opacityMultiplier: 1,
            viewportSize: SIMD2(Float(max(view.drawableSize.width, 1)), Float(max(view.drawableSize.height, 1)))
        )

        encoder.setDepthStencilState(depthState)
        encoder.setRenderPipelineState(additivePipeline)
        draw(buffer: hullBuffer, count: hullVertices.count, primitive: .line, opacity: hullOpacity, encoder: encoder, uniforms: &uniforms)
        if flow { draw(buffer: flowBuffer, count: flowVertices.count, primitive: .point, opacity: 1, encoder: encoder, uniforms: &uniforms) }
        encoder.setRenderPipelineState(additiveRibbonPipeline)
        drawRibbon(opacity: 0.48, encoder: encoder, uniforms: &uniforms)
        encoder.setRenderPipelineState(additiveBillboardPipeline)
        draw(buffer: haloBillboardBuffer, count: haloBillboardVertices.count, primitive: .triangle, opacity: 1, encoder: encoder, uniforms: &uniforms)
        encoder.setRenderPipelineState(ribbonPipeline)
        drawRibbon(opacity: 1, encoder: encoder, uniforms: &uniforms)
        encoder.setRenderPipelineState(billboardPipeline)
        draw(buffer: nodeBillboardBuffer, count: nodeBillboardVertices.count, primitive: .triangle, opacity: 1, encoder: encoder, uniforms: &uniforms)
        encoder.endEncoding()
        _ = encodeBloom(source: drawable.texture, command: command)
        command.addCompletedHandler { [weak self] _ in
            frameSemaphore.signal()
            Task { @MainActor [weak self] in
                guard let self, self.focusAnimation != nil,
                      let view = self.view, view.isPaused else { return }
                view.setNeedsDisplay(view.bounds)
            }
        }
        command.present(drawable)
        command.commit()
    }

    private func draw(
        buffer: MTLBuffer?, count: Int, primitive: MTLPrimitiveType, opacity: Float,
        encoder: MTLRenderCommandEncoder, uniforms: inout BrainUniforms
    ) {
        guard count > 0, let buffer else { return }
        uniforms.opacityMultiplier = opacity
        encoder.setVertexBytes(&uniforms, length: MemoryLayout<BrainUniforms>.stride, index: 1)
        encoder.setVertexBuffer(buffer, offset: 0, index: 0)
        encoder.drawPrimitives(type: primitive, vertexStart: 0, vertexCount: count)
    }

    private func drawRibbon(opacity: Float, encoder: MTLRenderCommandEncoder, uniforms: inout BrainUniforms) {
        guard !ribbonVertices.isEmpty, let ribbonBuffer else { return }
        uniforms.opacityMultiplier = opacity
        encoder.setVertexBytes(&uniforms, length: MemoryLayout<BrainUniforms>.stride, index: 1)
        encoder.setVertexBuffer(ribbonBuffer, offset: 0, index: 0)
        encoder.drawPrimitives(type: .triangle, vertexStart: 0, vertexCount: ribbonVertices.count)
    }

    private func encodeBloom(source: MTLTexture, command: MTLCommandBuffer) -> BrainBloomTargets? {
        guard let extract = bloomExtractPipeline,
              let blur = bloomBlurPipeline,
              let composite = bloomCompositePipeline,
              let sampler = bloomSampler,
              let targets = bloomTargets(for: source)
        else { return nil }

        guard encodeFullscreen(
            source: source, target: targets.bright, pipeline: extract,
            sampler: sampler, direction: .zero, loadAction: .clear, command: command
        ), encodeFullscreen(
            source: targets.bright, target: targets.scratch, pipeline: blur,
            sampler: sampler, direction: SIMD2(1 / Float(targets.width), 0), loadAction: .clear, command: command
        ), encodeFullscreen(
            source: targets.scratch, target: targets.bright, pipeline: blur,
            sampler: sampler, direction: SIMD2(0, 1 / Float(targets.height)), loadAction: .clear, command: command
        ), encodeFullscreen(
            source: targets.bright, target: source, pipeline: composite,
            sampler: sampler, direction: .zero, loadAction: .load, command: command
        ) else { return nil }
        return targets
    }

    private func encodeFullscreen(
        source: MTLTexture, target: MTLTexture, pipeline: MTLRenderPipelineState,
        sampler: MTLSamplerState, direction: SIMD2<Float>, loadAction: MTLLoadAction,
        command: MTLCommandBuffer
    ) -> Bool {
        let pass = MTLRenderPassDescriptor()
        pass.colorAttachments[0].texture = target
        pass.colorAttachments[0].loadAction = loadAction
        pass.colorAttachments[0].storeAction = .store
        pass.colorAttachments[0].clearColor = MTLClearColorMake(0, 0, 0, 0)
        guard let encoder = command.makeRenderCommandEncoder(descriptor: pass) else { return false }
        var direction = direction
        encoder.setRenderPipelineState(pipeline)
        encoder.setFragmentTexture(source, index: 0)
        encoder.setFragmentSamplerState(sampler, index: 0)
        encoder.setFragmentBytes(&direction, length: MemoryLayout<SIMD2<Float>>.stride, index: 0)
        encoder.drawPrimitives(type: .triangle, vertexStart: 0, vertexCount: 3)
        encoder.endEncoding()
        return true
    }

    private func bloomTargets(for drawable: MTLTexture) -> BrainBloomTargets? {
        guard let plan = BrainBloomTexturePlan(drawableWidth: drawable.width, drawableHeight: drawable.height) else {
            return nil
        }
        if let bloomTargets, bloomTargets.plan == plan { return bloomTargets }
        let descriptor = MTLTextureDescriptor.texture2DDescriptor(
            pixelFormat: .rgba16Float, width: plan.width, height: plan.height, mipmapped: false
        )
        descriptor.usage = [.renderTarget, .shaderRead]
        descriptor.storageMode = .private
        guard let bright = metalDevice.makeTexture(descriptor: descriptor),
              let scratch = metalDevice.makeTexture(descriptor: descriptor) else { return nil }
        let targets = BrainBloomTargets(plan: plan, bright: bright, scratch: scratch)
        bloomTargets = targets
        return targets
    }

    private func makeBuffer(_ vertices: [BrainMetalVertex]) -> MTLBuffer? {
        guard !vertices.isEmpty else { return nil }
        let (length, overflow) = MemoryLayout<BrainMetalVertex>.stride.multipliedReportingOverflow(by: vertices.count)
        guard !overflow, length > 0 else { return nil }
        return metalDevice.makeBuffer(bytes: vertices, length: length, options: .storageModeShared)
    }

    private func makeEmptyBuffer(vertexCapacity: Int) -> MTLBuffer? {
        let (length, overflow) = MemoryLayout<BrainMetalVertex>.stride.multipliedReportingOverflow(by: vertexCapacity)
        guard !overflow, length > 0 else { return nil }
        return metalDevice.makeBuffer(length: length, options: .storageModeShared)
    }

    private func makeBillboardBuffer(_ vertices: [BrainBillboardVertex]) -> MTLBuffer? {
        guard !vertices.isEmpty else { return nil }
        let (length, overflow) = MemoryLayout<BrainBillboardVertex>.stride.multipliedReportingOverflow(by: vertices.count)
        guard !overflow, length > 0 else { return nil }
        return metalDevice.makeBuffer(bytes: vertices, length: length, options: .storageModeShared)
    }

    private func makeRibbonBuffer(_ vertices: [BrainRibbonVertex]) -> MTLBuffer? {
        guard !vertices.isEmpty else { return nil }
        let (length, overflow) = MemoryLayout<BrainRibbonVertex>.stride.multipliedReportingOverflow(by: vertices.count)
        guard !overflow, length > 0 else { return nil }
        return metalDevice.makeBuffer(bytes: vertices, length: length, options: .storageModeShared)
    }

    private func updateFlowBuffer(_ flowBuffer: MTLBuffer?, time: Float) {
        guard flowVertices.count == flowSegments.count, let flowBuffer else { return }
        for index in flowSegments.indices {
            let segment = flowSegments[index]
            let travel = (time * 0.13 + segment.phase).truncatingRemainder(dividingBy: 1)
            let position = simd_mix(segment.source, segment.target, SIMD3<Float>(repeating: travel))
            flowVertices[index].positionSize = SIMD4(position.x, position.y, position.z, 3.4)
        }
        flowVertices.withUnsafeBytes { bytes in
            guard let source = bytes.baseAddress else { return }
            memcpy(flowBuffer.contents(), source, bytes.count)
        }
    }

    private func requestFrameIfPaused() {
        Task { @MainActor [weak view] in
            guard let view, view.isPaused else { return }
            view.setNeedsDisplay(view.bounds)
        }
    }

    private func currentFocus(at time: CFTimeInterval) -> SIMD3<Float> {
        guard let animation = focusAnimation else { return cameraFocus }
        let progress = Float(min(1, max(0, (time - animation.startedAt) / animation.duration)))
        let eased = progress * progress * (3 - 2 * progress)
        let focus = simd_mix(animation.start, animation.target, SIMD3<Float>(repeating: eased))
        if progress >= 1 { focusAnimation = nil }
        return focus
    }

    private func buildHull() {
        if let url = Self.anatomicalBrainURL(), buildHull(fromOBJ: url) {
            return
        }
        buildProceduralHull()
    }

    private static func anatomicalBrainURL() -> URL? {
        let resourceBundleName = "SAGECerebrumNative_SAGECerebrumNative.bundle"
        if let resources = Bundle.main.resourceURL,
           let bundle = Bundle(url: resources.appendingPathComponent(resourceBundleName)),
           let url = bundle.url(forResource: "brain", withExtension: "obj") {
            return url
        }
        #if DEBUG
        return Bundle.module.url(forResource: "brain", withExtension: "obj")
        #else
        return nil
        #endif
    }

    private func buildHull(fromOBJ url: URL) -> Bool {
        guard let source = try? String(contentsOf: url, encoding: .utf8) else { return false }
        var positions: [SIMD3<Float>] = []
        var faces: [SIMD3<Int>] = []
        positions.reserveCapacity(24_500)
        faces.reserveCapacity(46_000)
        for line in source.split(separator: "\n") {
            if line.hasPrefix("v ") {
                let parts = line.split(whereSeparator: \.isWhitespace)
                guard parts.count >= 4, let x = Float(parts[1]), let y = Float(parts[2]), let z = Float(parts[3]) else { continue }
                positions.append(SIMD3(x, y, z))
            } else if line.hasPrefix("f ") {
                let parts = line.split(whereSeparator: \.isWhitespace)
                guard parts.count >= 4 else { continue }
                let indices = parts[1 ... 3].compactMap { token -> Int? in
                    token.split(separator: "/").first.flatMap { Int($0) }.map { $0 - 1 }
                }
                if indices.count == 3 { faces.append(SIMD3(indices[0], indices[1], indices[2])) }
            }
        }
        guard positions.count >= 3, !faces.isEmpty else { return false }
        var minimum = positions[0]
        var maximum = positions[0]
        for point in positions.dropFirst() {
            minimum = simd_min(minimum, point)
            maximum = simd_max(maximum, point)
        }
        let center = (minimum + maximum) * 0.5
        let radius = positions.reduce(Float(0)) { max($0, simd_length($1 - center)) }
        guard radius.isFinite, radius > 0 else { return false }
        let scale: Float = 2.75 / radius
        let transformed = positions.map { ($0 - center) * scale }
        let color = SIMD4<Float>(0.42, 0.75, 1.0, 0.92)
        hullVertices.reserveCapacity(faces.count * 3)
        var emittedEdges = Set<UInt64>()
        emittedEdges.reserveCapacity(faces.count * 2)
        func appendEdge(_ first: Int, _ second: Int) {
            let lower = min(first, second)
            let upper = max(first, second)
            let key = (UInt64(lower) << 32) | UInt64(upper)
            guard emittedEdges.insert(key).inserted else { return }
            hullVertices.append(.init(positionSize: SIMD4(transformed[lower], 1), color: color))
            hullVertices.append(.init(positionSize: SIMD4(transformed[upper], 1), color: color))
        }
        for face in faces {
            guard face.x >= 0, face.y >= 0, face.z >= 0,
                  face.x < transformed.count, face.y < transformed.count, face.z < transformed.count else { continue }
            appendEdge(face.x, face.y)
            appendEdge(face.y, face.z)
            appendEdge(face.z, face.x)
        }
        return !hullVertices.isEmpty
    }

    private func buildProceduralHull() {
        let longitudeCount = 128
        let latitudeCount = 72
        let color = SIMD4<Float>(0.29, 0.64, 1.0, 0.92)
        func vertex(longitude: Int, latitude: Int) -> BrainMetalVertex {
            let u = Float(longitude % longitudeCount) / Float(longitudeCount) * 2 * Float.pi
            let v = -Float.pi / 2 + Float(latitude) / Float(latitudeCount) * Float.pi
            return .init(positionSize: SIMD4(Self.brainSurfacePoint(longitude: u, latitude: v), 1), color: color)
        }
        for latitude in 0 ... latitudeCount {
            for longitude in 0 ..< longitudeCount {
                hullVertices.append(vertex(longitude: longitude, latitude: latitude))
                hullVertices.append(vertex(longitude: longitude + 1, latitude: latitude))
                if latitude < latitudeCount {
                    hullVertices.append(vertex(longitude: longitude, latitude: latitude))
                    hullVertices.append(vertex(longitude: longitude, latitude: latitude + 1))
                }
            }
        }
    }

    private static func brainSurfacePoint(longitude: Float, latitude: Float) -> SIMD3<Float> {
        let cosLatitude = cos(latitude)
        let x = cosLatitude * cos(longitude)
        let y = sin(latitude)
        let z = cosLatitude * sin(longitude)
        var radius: Float = 1
            + 0.052 * sin(8 * z + 3 * y)
            + 0.044 * sin(10 * y + 4 * x)
            + 0.040 * sin(12 * x + 6 * z)
            + 0.028 * sin(17 * z) * cos(15 * y)
            + 0.020 * sin(23 * y + 14 * x)
            + 0.014 * sin(29 * x + 19 * z)
        radius -= exp(-(x * x) * 60) * 0.20 * max(0, y)
        let cerebellum = exp(-((z + 0.8) * (z + 0.8) * 5 + (y + 0.5) * (y + 0.5) * 6 + x * x * 3))
        radius += cerebellum * (0.035 + 0.045 * abs(sin(38 * z + 22 * x)))
        var point = SIMD3<Float>(x * radius * 2.35, y * radius * 1.95, z * radius * 2.75)
        if point.y < -0.74 { point.y = -0.74 + (point.y + 0.74) * 0.5 }
        return point
    }

    private static func positions(for nodes: [BrainNode], layout: BrainMetalLayout, now: Date = .now) -> [SIMD3<Float>] {
        if layout == .connectome { return connectomePositions(for: nodes) }
        let domains = Array(Set(nodes.map(\.domain))).sorted()
        let domainIndex = Dictionary(uniqueKeysWithValues: domains.enumerated().map { ($0.element, $0.offset) })
        let count = max(domains.count, 1)
        return nodes.map { node in
            let days = max(0, now.timeIntervalSince(node.createdAt) / 86_400)
            let age = min(1, days / 365)
            let recency = 1 - age
            let jitter = (stableUnit(node.id, seed: 3) - 0.5) * 0.10
            let depth = Float(max(0.20, min(0.89, 0.25 + pow(recency, 0.68) * 0.59 + jitter)))
            let wedge = 2 * Double.pi / Double(count)
            let azimuth = Double(domainIndex[node.domain] ?? 0) * wedge + (stableUnit(node.id, seed: 1) - 0.5) * wedge * 0.82
            let elevation = (stableUnit(node.id, seed: 2) - 0.5) * Double.pi * 0.96
            let ce = cos(elevation)
            let sine = Float(sin(elevation))
            let verticalExtent: Float = sine < 0 ? 1.32 : 1.82
            let lowered = Float(pow(age, 1.35)) * 1.82 * 0.12
            return SIMD3(
                2.15 * depth * Float(ce * cos(azimuth)),
                max(-1.52, verticalExtent * depth * sine - lowered),
                2.58 * depth * Float(ce * sin(azimuth))
            )
        }
    }

    private static func connectomePositions(for nodes: [BrainNode]) -> [SIMD3<Float>] {
        let agents = nodes.filter { $0.memoryType != "__engram__" }
        var positions: [String: SIMD3<Float>] = [:]
        let domains = Array(Set(agents.map(\.domain))).sorted()
        let domainIndex = Dictionary(uniqueKeysWithValues: domains.enumerated().map { ($0.element, $0.offset) })
        let count = max(domains.count, 1)
        for node in agents {
            let traffic = max(0, min(1, (node.confidence - 0.45) / 0.55))
            let jitter = (stableUnit(node.id, seed: 3) - 0.5) * 0.10
            let depth = Float(max(0.20, min(0.89, 0.25 + pow(1 - traffic, 0.68) * 0.59 + jitter)))
            let wedge = 2 * Double.pi / Double(count)
            let azimuth = Double(domainIndex[node.domain] ?? 0) * wedge + (stableUnit(node.id, seed: 1) - 0.5) * wedge * 0.82
            let elevation = (stableUnit(node.id, seed: 2) - 0.5) * Double.pi * 0.96
            let sine = Float(sin(elevation))
            let verticalExtent: Float = sine < 0 ? 1.32 : 1.82
            let lowered = Float(pow(traffic, 1.35)) * 1.82 * 0.12
            let cosine = Float(cos(elevation))
            positions[node.id] = SIMD3(
                2.15 * depth * cosine * Float(cos(azimuth)),
                max(-1.52, verticalExtent * depth * sine - lowered),
                2.58 * depth * cosine * Float(sin(azimuth))
            )
        }
        let groupedEngrams = Dictionary(grouping: nodes.filter { $0.memoryType == "__engram__" }, by: \.agent)
        for (agentID, engrams) in groupedEngrams {
            let anchor = positions["agent:\(agentID)"] ?? .zero
            let total = max(engrams.count, 1)
            for (index, engram) in engrams.enumerated() {
                let angle = Float(index) / Float(total) * 2 * Float.pi + Float(stableUnit(engram.id, seed: 13)) * 0.35
                let ring = 0.40 + Float(index % 3) * 0.12
                let lift = Float(stableUnit(engram.id, seed: 14) - 0.5) * 0.46
                positions[engram.id] = anchor + SIMD3(ring * cos(angle), lift, ring * sin(angle))
            }
        }
        return nodes.map { positions[$0.id] ?? .zero }
    }

    private static func stableUnit(_ value: String, seed: UInt32) -> Double {
        var hash = seed == 0 ? UInt32(1) : seed
        for byte in value.utf8 { hash = (hash ^ UInt32(byte)) &* 16_777_619 }
        return Double(hash % 10_000) / 10_000
    }

    private static func billboards(from nodes: [BrainMetalVertex], halo: Bool) -> [BrainBillboardVertex] {
        let corners: [SIMD2<Float>] = [
            .init(-1, -1), .init(1, -1), .init(-1, 1),
            .init(-1, 1), .init(1, -1), .init(1, 1),
        ]
        return nodes.enumerated().flatMap { index, node in
            let size = node.positionSize.w * (halo ? 2.9 : 1)
            let color = halo
                ? SIMD4(node.color.x, node.color.y, node.color.z, node.color.w * 0.20)
                : node.color
            return corners.map {
                BrainBillboardVertex(
                    centerSize: SIMD4(node.positionSize.x, node.positionSize.y, node.positionSize.z, size),
                    color: color, corner: $0, nodeIndex: Float(index), style: halo ? 1 : 0
                )
            }
        }
    }

    private static func domainColor(_ domain: String) -> SIMD3<Float> {
        let palette: [SIMD3<Float>] = [
            .init(1.00, 0.42, 0.62), .init(1.00, 0.82, 0.40), .init(0.37, 0.89, 0.63),
            .init(0.35, 0.69, 1.00), .init(0.75, 0.55, 1.00), .init(1.00, 0.62, 0.35),
            .init(0.30, 0.84, 0.77), .init(0.97, 0.45, 0.54), .init(0.60, 0.82, 0.29),
            .init(0.48, 0.63, 1.00),
        ]
        return palette[Int(stableUnit(domain, seed: 7) * Double(palette.count)) % palette.count]
    }

    static func isSameDirectedEdge(_ highlighted: BrainEdge?, _ candidate: BrainEdge) -> Bool {
        guard let highlighted else { return false }
        return highlighted.source == candidate.source && highlighted.target == candidate.target && highlighted.type == candidate.type
    }

    static func selectRenderEdges(
        _ edges: [BrainEdge], highlighted: BrainEdge?, limit: Int = 2_048
    ) -> [BrainEdge] {
        guard limit > 0, edges.count > limit else { return limit > 0 ? edges : [] }
        let sorted = edges.sorted {
            if ($0.weight ?? 0) != ($1.weight ?? 0) { return ($0.weight ?? 0) > ($1.weight ?? 0) }
            if ($0.lastFired ?? .distantPast) != ($1.lastFired ?? .distantPast) {
                return ($0.lastFired ?? .distantPast) > ($1.lastFired ?? .distantPast)
            }
            return edgeSortKey($0) < edgeSortKey($1)
        }
        var selected = Array(sorted.prefix(limit))
        if let highlighted,
           !selected.contains(where: { isSameDirectedEdge(highlighted, $0) }),
           let required = edges.first(where: { isSameDirectedEdge(highlighted, $0) }) {
            selected[selected.count - 1] = required
        }
        return selected
    }

    static func edgePlasticity(lastFired: Date?, now: Date = .now) -> Float {
        guard let lastFired else { return 0.15 }
        let age = max(0, now.timeIntervalSince(lastFired))
        return 0.15 + 0.85 * pow(2, Float(-age / 1_800))
    }

    private static func nodeAppearance(_ node: BrainNode, now: Date = .now) -> SIMD4<Float> {
        if node.status == "deprecated" { return .init(0.42, 0.47, 0.57, 0.30) }
        if node.status == "challenged" { return .init(0.59, 0.64, 0.73, 0.55) }
        var color = domainColor(node.domain)
        let consolidation = Float(min(1, Double(node.corroborationCount) / 8))
        color += (SIMD3<Float>(repeating: 1) - color) * consolidation * 0.5
        let freshness = Float(max(0, min(1, 1 - now.timeIntervalSince(node.createdAt) / 86_400)))
        let freshAccent = SIMD3<Float>(0.35, 0.86, 1.0)
        color += (freshAccent - color) * freshness * 0.72
        let opacity = Float(max(0.60, min(1, 0.60 + node.confidence * 0.40)))
        return SIMD4(color.x, color.y, color.z, max(opacity, 0.85 + freshness * 0.15))
    }

    private static let shaderSource = #"""
    #include <metal_stdlib>
    using namespace metal;
    struct VertexIn { float4 positionSize; float4 color; };
    struct Uniforms { float4x4 mvp; float selectedIndex; float opacityMultiplier; float2 viewportSize; };
    struct VertexOut { float4 position [[position]]; float4 color; float pointSize [[point_size]]; float selected; float halo; };
    vertex VertexOut brainVertex(const device VertexIn *vertices [[buffer(0)]], constant Uniforms &u [[buffer(1)]], uint id [[vertex_id]]) {
        VertexOut out;
        VertexIn v = vertices[id];
        out.position = u.mvp * float4(v.positionSize.xyz, 1.0);
        out.color = float4(v.color.rgb, v.color.a * u.opacityMultiplier);
        out.selected = fabs(float(id) - u.selectedIndex) < 0.5 ? 1.0 : 0.0;
        out.halo = v.positionSize.w < 0.0 ? 1.0 : 0.0;
        out.pointSize = abs(v.positionSize.w) + out.selected * (out.halo < 0.5 ? 7.0 : 0.0);
        return out;
    }
    fragment float4 brainFragment(VertexOut in [[stage_in]], float2 point [[point_coord]]) {
        float d = distance(point, float2(0.5));
        if (in.pointSize > 1.5 && d > 0.5) discard_fragment();
        float glow = in.pointSize > 1.5 ? (in.halo > 0.5 ? pow(smoothstep(0.5, 0.0, d), 2.2) : smoothstep(0.52, 0.28, d)) : 1.0;
        float3 selected = mix(in.color.rgb, float3(1.0), in.selected * 0.65);
        if (in.selected > 0.5 && in.halo < 0.5) {
            float ring = 1.0 - smoothstep(0.018, 0.055, abs(d - 0.41));
            selected = mix(selected, float3(0.22, 0.82, 1.0), ring);
            glow = max(glow, ring);
        }
        return float4(selected, in.color.a * glow);
    }
    struct BillboardIn { float4 centerSize; float4 color; float2 corner; float nodeIndex; float style; };
    struct BillboardOut { float4 position [[position]]; float4 color; float2 uv; float selected; float style; };
    vertex BillboardOut billboardVertex(const device BillboardIn *vertices [[buffer(0)]], constant Uniforms &u [[buffer(1)]], uint id [[vertex_id]]) {
        BillboardIn v = vertices[id];
        BillboardOut out;
        float4 clip = u.mvp * float4(v.centerSize.xyz, 1.0);
        float2 pixelOffset = v.corner * v.centerSize.w / max(u.viewportSize, float2(1.0)) * clip.w;
        clip.xy += pixelOffset;
        out.position = clip;
        out.color = float4(v.color.rgb, v.color.a * u.opacityMultiplier);
        out.uv = v.corner * 0.5 + 0.5;
        out.selected = fabs(v.nodeIndex - u.selectedIndex) < 0.5 ? 1.0 : 0.0;
        out.style = v.style;
        return out;
    }
    fragment float4 billboardFragment(BillboardOut in [[stage_in]]) {
        float2 q = (in.uv - 0.5) * 2.0;
        float radius = length(q);
        if (radius > 1.0) discard_fragment();
        if (in.style > 0.5) {
            float halo = pow(smoothstep(1.0, 0.0, radius), 2.4);
            return float4(in.color.rgb, in.color.a * halo);
        }
        float z = sqrt(max(0.0, 1.0 - radius * radius));
        float lighting = 0.56 + 0.44 * max(0.0, dot(normalize(float3(q.x, -q.y, z)), normalize(float3(-0.35, 0.45, 0.82))));
        float3 color = in.color.rgb * lighting;
        float alpha = in.color.a * smoothstep(1.0, 0.82, radius);
        if (in.selected > 0.5) {
            float outerRing = 1.0 - smoothstep(0.035, 0.085, abs(radius - 0.91));
            float innerRing = 1.0 - smoothstep(0.025, 0.070, abs(radius - 0.73));
            color = mix(color, float3(0.20, 0.82, 1.0), outerRing);
            color = mix(color, float3(1.0), innerRing);
            alpha = max(alpha, max(outerRing, innerRing));
        }
        return float4(color, alpha);
    }

    struct RibbonIn {
        float4 source; float4 target; float4 color; float2 corner;
        float width; float curvature; float loopAngle; float loop;
    };
    struct RibbonOut { float4 position [[position]]; float4 color; };
    vertex RibbonOut ribbonVertex(
        const device RibbonIn *vertices [[buffer(0)]], constant Uniforms &u [[buffer(1)]], uint id [[vertex_id]]) {
        RibbonIn ribbon = vertices[id];
        float4 source = u.mvp * ribbon.source;
        float4 target = u.mvp * ribbon.target;
        float2 viewport = max(u.viewportSize, float2(1.0));
        float2 sourceNDC = source.xy / max(source.w, 0.0001);
        float2 targetNDC = target.xy / max(target.w, 0.0001);
        float2 sourcePixel = sourceNDC * viewport * 0.5;
        float2 targetPixel = targetNDC * viewport * 0.5;
        float t = ribbon.corner.x;
        float4 clip = mix(source, target, t);
        float2 centerPixel;
        float2 tangent;
        if (ribbon.loop > 0.5) {
            float2 loopDirection = float2(cos(ribbon.loopAngle), sin(ribbon.loopAngle));
            float radius = 18.0 + ribbon.width * 2.0;
            float2 loopCenter = sourcePixel + loopDirection * radius;
            float theta = ribbon.loopAngle + M_PI_F + 2.0 * M_PI_F * t;
            centerPixel = loopCenter + float2(cos(theta), sin(theta)) * radius;
            tangent = float2(-sin(theta), cos(theta));
            clip = source;
        } else {
            float2 projectedDelta = targetPixel - sourcePixel;
            float projectedLengthSquared = dot(projectedDelta, projectedDelta);
            float2 direction = projectedLengthSquared > 0.000001
                ? projectedDelta * rsqrt(projectedLengthSquared)
                : float2(1.0, 0.0);
            float2 bendNormal = float2(-direction.y, direction.x);
            centerPixel = mix(sourcePixel, targetPixel, t) + bendNormal * ribbon.curvature * sin(M_PI_F * t);
            tangent = projectedDelta + bendNormal * ribbon.curvature * M_PI_F * cos(M_PI_F * t);
        }
        float tangentLengthSquared = dot(tangent, tangent);
        float2 direction = tangentLengthSquared > 0.000001 ? tangent * rsqrt(tangentLengthSquared) : float2(1.0, 0.0);
        float2 normal = float2(-direction.y, direction.x);
        float2 desiredNDC = centerPixel * 2.0 / viewport;
        clip.xy = (desiredNDC + normal * ribbon.corner.y * ribbon.width * 2.0 / viewport) * clip.w;
        RibbonOut out;
        out.position = clip;
        out.color = float4(ribbon.color.rgb, ribbon.color.a * u.opacityMultiplier);
        return out;
    }
    fragment float4 ribbonFragment(RibbonOut in [[stage_in]]) {
        return in.color;
    }

    struct FullscreenOut { float4 position [[position]]; float2 uv; };
    vertex FullscreenOut fullscreenVertex(uint id [[vertex_id]]) {
        const float2 positions[3] = { float2(-1.0, -1.0), float2(3.0, -1.0), float2(-1.0, 3.0) };
        FullscreenOut out;
        out.position = float4(positions[id], 0.0, 1.0);
        out.uv = positions[id] * float2(0.5, -0.5) + 0.5;
        return out;
    }
    fragment float4 bloomExtractFragment(
        FullscreenOut in [[stage_in]], texture2d<float> source [[texture(0)]], sampler linearSampler [[sampler(0)]]) {
        float3 color = source.sample(linearSampler, in.uv).rgb;
        float brightness = max(color.r, max(color.g, color.b));
        float contribution = smoothstep(0.32, 0.44, brightness);
        return float4(color * contribution, 1.0);
    }
    fragment float4 bloomBlurFragment(
        FullscreenOut in [[stage_in]], texture2d<float> source [[texture(0)]], sampler linearSampler [[sampler(0)]],
        constant float2 &direction [[buffer(0)]]) {
        float3 color = source.sample(linearSampler, in.uv).rgb * 0.227027;
        color += source.sample(linearSampler, in.uv + direction * 1.384615).rgb * 0.316216;
        color += source.sample(linearSampler, in.uv - direction * 1.384615).rgb * 0.316216;
        color += source.sample(linearSampler, in.uv + direction * 3.230769).rgb * 0.070270;
        color += source.sample(linearSampler, in.uv - direction * 3.230769).rgb * 0.070270;
        return float4(color, 1.0);
    }
    fragment float4 bloomCompositeFragment(
        FullscreenOut in [[stage_in]], texture2d<float> bloom [[texture(0)]], sampler linearSampler [[sampler(0)]]) {
        return float4(bloom.sample(linearSampler, in.uv).rgb * 0.55, 0.0);
    }
    """#
}

struct BrainBloomTexturePlan: Equatable {
    let width: Int
    let height: Int

    init?(drawableWidth: Int, drawableHeight: Int) {
        guard drawableWidth > 0, drawableHeight > 0 else { return nil }
        width = (drawableWidth + 1) / 2
        height = (drawableHeight + 1) / 2
    }
}

private final class BrainBloomTargets {
    let plan: BrainBloomTexturePlan
    let bright: MTLTexture
    let scratch: MTLTexture
    var width: Int { plan.width }
    var height: Int { plan.height }

    init(plan: BrainBloomTexturePlan, bright: MTLTexture, scratch: MTLTexture) {
        self.plan = plan
        self.bright = bright
        self.scratch = scratch
    }
}

private struct BrainMetalVertex {
    var positionSize: SIMD4<Float>
    var color: SIMD4<Float>
}

private struct BrainFlowSegment {
    let source: SIMD3<Float>
    let target: SIMD3<Float>
    let color: SIMD4<Float>
    let phase: Float
}

private struct BrainFocusAnimation {
    let start: SIMD3<Float>
    let target: SIMD3<Float>
    let startedAt: CFTimeInterval
    let duration: CFTimeInterval
}

private struct BrainBillboardVertex {
    let centerSize: SIMD4<Float>
    let color: SIMD4<Float>
    let corner: SIMD2<Float>
    let nodeIndex: Float
    let style: Float
}

private struct BrainRibbonVertex {
    let source: SIMD4<Float>
    let target: SIMD4<Float>
    let color: SIMD4<Float>
    let corner: SIMD2<Float>
    let width: Float
    let curvature: Float
    let loopAngle: Float
    let loop: Float
}

private struct BrainRenderedRibbon {
    let edge: BrainEdge
    let source: SIMD3<Float>
    let target: SIMD3<Float>
    let width: Float
    let curvature: Float
    let loopAngle: Float
    let isLoop: Bool
}

private struct BrainUniforms {
    var mvp: simd_float4x4
    var selectedIndex: Float
    var opacityMultiplier: Float
    var viewportSize: SIMD2<Float>
}

private extension simd_float4x4 {
    static func perspective(fovY: Float, aspect: Float, near: Float, far: Float) -> Self {
        let y = 1 / tan(fovY * 0.5)
        let x = y / aspect
        let z = far / (near - far)
        return .init(columns: (
            SIMD4(x, 0, 0, 0), SIMD4(0, y, 0, 0),
            SIMD4(0, 0, z, -1), SIMD4(0, 0, z * near, 0)
        ))
    }

    static func translation(_ x: Float, _ y: Float, _ z: Float) -> Self {
        .init(columns: (SIMD4(1, 0, 0, 0), SIMD4(0, 1, 0, 0), SIMD4(0, 0, 1, 0), SIMD4(x, y, z, 1)))
    }

    static func rotationX(_ angle: Float) -> Self {
        let c = cos(angle), s = sin(angle)
        return .init(columns: (SIMD4(1, 0, 0, 0), SIMD4(0, c, s, 0), SIMD4(0, -s, c, 0), SIMD4(0, 0, 0, 1)))
    }

    static func rotationY(_ angle: Float) -> Self {
        let c = cos(angle), s = sin(angle)
        return .init(columns: (SIMD4(c, 0, -s, 0), SIMD4(0, 1, 0, 0), SIMD4(s, 0, c, 0), SIMD4(0, 0, 0, 1)))
    }
}
