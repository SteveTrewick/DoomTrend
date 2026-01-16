# DoomTrend

DoomTrend is a lightweight Swift package for detecting trending topics from a stream of `NewsItem` values. It focuses on recent bursts while keeping a longer baseline window for context, and it supports stopwords, alias mapping, case-aware weighting, and optional phrase extraction.

## Features
- Short vs baseline windows with bucketed counts
- Case-aware scoring (Title Case and ALL CAPS terms can weigh higher)
- Title vs body weighting
- Stopwords + phrase stopwords + dynamic stopwords
- Optional bigrams/trigrams and title-case phrase extraction
- Simple API for ingest + trend snapshots

## Requirements
- macOS 13+
- Swift 6.2

## Installation
Add the package dependency (local path or git URL if published):

```swift
.package(path: "../DoomTrend")
```

Then depend on the product:

```swift
.product(name: "DoomTrend", package: "DoomTrend")
```

## Basic Usage

```swift
import DoomTrend
import DoomModels

let detector = TrendDetector()

let item = NewsItem(
    feedID: "feed",
    source: "Example",
    title: "USA markets jump as inflation eases",
    body: "...",
    url: URL(string: "https://example.com/1")!,
    publishedAt: Date(),
    ingestedAt: Date()
)

await detector.ingest(item)
let topics = await detector.trending()
```

## Configuration

```swift
let config = TrendDetector.Configuration(
    shortWindow: 10 * 60,
    baselineWindow: 6 * 60 * 60,
    bucketSize: 60,
    minShortCount: 2,
    minUniqueSources: 2,
    maxTermsPerItem: 40,
    selectTopTermsPerItem: false,
    enableBigrams: true,
    enableTrigrams: false,
    weights: .init(
        burstWeight: 1.0,
        countWeight: 0.6,
        accelWeight: 0.2,
        sourceWeight: 0.3,
        titleCaseWeight: 1.25,
        allCapsWeight: 1.5,
        titleTokenWeight: 1.0,
        bodyTokenWeight: 0.6
    )
)

let detector = TrendDetector(configuration: config)
```

To restrict each item to a single term (best-scoring term wins). This is what DBRSSTrend uses when `--single-term` is passed:

```swift
let config = TrendDetector.Configuration(
    maxTermsPerItem: 1,
    selectTopTermsPerItem: true
)
```

## Stopwords and Aliases

```swift
await detector.addStopwords(["breaking", "live", "update"])
await detector.removeStopwords(["us"]) // allow lower-case "us"
await detector.addPhraseStopwords(["reported", "warning"])
await detector.addAliasMappings([
    "usa": "united states",
    "u.s.": "united states"
])
```

Dynamic stopwords can be enabled to suppress high-frequency, low-burst terms in the background. Use `clearDynamicStopwords()` to reset them.

## Output Fields
`TrendingTopic` includes:
- `term`: canonical topic string (after alias mapping)
- `score`: composite score based on burst, volume, acceleration, and sources
- `shortCount`: raw count within the short window
- `baselineCount`: raw count across the baseline window
- `uniqueSources`: number of distinct sources in the short window
- `burstZ`: z-score of short vs expected short baseline
- `burstRatio`: short / expected short
- `titleShare`: fraction of short-window hits from titles
- `sampleHeadlines`: small set of recent headlines
- `lastSeenAt`: most recent timestamp for the term

## Notes
- All-caps tokens bypass stopwords by default (e.g. "US").
- Short proper nouns (e.g. "Wu") are allowed even if below the minimum token length.
- Use `reset()` to clear all internal state.
