// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "DoomTrend",
    platforms: [
        .macOS(.v13),
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "DoomTrend",
            targets: ["DoomTrend"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/SteveTrewick/DoomModels.git", from: "0.1.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "DoomTrend",
            dependencies: [
                .product(name: "DoomModels", package: "DoomModels"),
            ]
        ),
        .testTarget(
            name: "DoomTrendTests",
            dependencies: [
                "DoomTrend",
                .product(name: "DoomModels", package: "DoomModels"),
            ]
        ),
    ]
)
