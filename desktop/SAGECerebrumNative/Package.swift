// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "SAGECerebrumNative",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "SAGECerebrumNative", targets: ["SAGECerebrumNative"]),
    ],
    targets: [
        .executableTarget(
            name: "SAGECerebrumNative",
            resources: [.copy("Resources/brain.obj")]
        ),
        .testTarget(
            name: "SAGECerebrumNativeTests",
            dependencies: ["SAGECerebrumNative"]
        ),
    ]
)
