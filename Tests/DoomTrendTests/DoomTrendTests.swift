import Foundation
import Testing
import DoomModels
@testable import DoomTrend

private func makeItem(
    title: String,
    body: String? = nil,
    source: String = "Source",
    publishedAt: Date
) -> NewsItem {
    let url = URL(string: "https://example.com/\(UUID().uuidString)")!
    return NewsItem(
        feedID: "feed",
        source: source,
        title: title,
        body: body,
        url: url,
        publishedAt: publishedAt,
        ingestedAt: publishedAt
    )
}

@Test func allCapsTokensBypassStopwords() async throws {
    let config = TrendDetector.Configuration(
        shortWindow: 60,
        baselineWindow: 600,
        bucketSize: 1,
        minShortCount: 1,
        minUniqueSources: 1,
        lowercaseMinShortCount: 1,
        lowercaseMinUniqueSources: 1,
        enableDynamicStopwords: false
    )
    let detector = TrendDetector(configuration: config)
    let now = Date()
    let item = makeItem(title: "US strikes announced", body: "us", publishedAt: now)
    await detector.ingest(item)

    let topics = await detector.trending(now: now.addingTimeInterval(1))

    #expect(topics.contains(where: { $0.term == "US" }))
    #expect(!topics.contains(where: { $0.term == "us" }))
}

@Test func aliasMappingCanonicalizesTokens() async throws {
    let config = TrendDetector.Configuration(
        shortWindow: 60,
        baselineWindow: 600,
        bucketSize: 1,
        aliasMap: ["usa": "united states"],
        minShortCount: 1,
        minUniqueSources: 1,
        lowercaseMinShortCount: 1,
        lowercaseMinUniqueSources: 1,
        enableDynamicStopwords: false
    )
    let detector = TrendDetector(configuration: config)
    let now = Date()
    let item = makeItem(title: "USA markets jump", publishedAt: now)
    await detector.ingest(item)

    let topics = await detector.trending(now: now.addingTimeInterval(1))

    #expect(topics.contains(where: { $0.term == "united states" }))
}

@Test func dynamicStopwordsSuppressFlatTerms() async throws {
    let config = TrendDetector.Configuration(
        shortWindow: 60,
        baselineWindow: 600,
        bucketSize: 1,
        maxTermsPerItem: 8,
        minTokenLength: 3,
        enableBigrams: false,
        enableTrigrams: false,
        enableTitleCasePhrases: false,
        enableDedupe: false,
        maxItemsPerSourcePerBucket: nil,
        minShortCount: 1,
        minUniqueSources: 1,
        lowercaseMinShortCount: 1,
        lowercaseMinUniqueSources: 1,
        enableDynamicStopwords: true,
        dynamicStopwordBaselineMin: 3,
        dynamicStopwordBurstZMax: 0.5,
        dynamicStopwordBurstRatioMax: 1.2,
        dynamicStopwordMinSources: 1
    )
    let detector = TrendDetector(configuration: config)
    let now = Date()

    for index in 0..<10 {
        let secondsAgo = Double(30 + index * 60)
        let timestamp = now.addingTimeInterval(-secondsAgo)
        let item = makeItem(title: "market update", publishedAt: timestamp)
        await detector.ingest(item)
    }

    let topics = await detector.trending(now: now)

    #expect(!topics.contains(where: { $0.term == "market" }))
}

@Test func selectTopTermsLimitsPerItem() async throws {
    let config = TrendDetector.Configuration(
        shortWindow: 60,
        baselineWindow: 600,
        bucketSize: 1,
        maxTermsPerItem: 1,
        selectTopTermsPerItem: true,
        minTokenLength: 3,
        enableBigrams: false,
        enableTrigrams: false,
        enableTitleCasePhrases: false,
        minShortCount: 1,
        minUniqueSources: 1,
        lowercaseMinShortCount: 1,
        lowercaseMinUniqueSources: 1,
        enableDynamicStopwords: false
    )
    let detector = TrendDetector(configuration: config)
    let now = Date()
    let item = makeItem(title: "US economy slows", publishedAt: now)
    await detector.ingest(item)

    let topics = await detector.trending(now: now.addingTimeInterval(1))

    #expect(topics.count == 1)
    #expect(topics.first?.term == "US")
}
