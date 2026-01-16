import Foundation
import DoomModels

public struct TrendingTopic: Sendable, Codable, Hashable {
    public let term: String
    public let score: Double
    public let shortCount: Int
    public let baselineCount: Int
    public let uniqueSources: Int
    public let burstZ: Double
    public let burstRatio: Double
    public let titleShare: Double
    public let sampleHeadlines: [String]
    public let lastSeenAt: Date

    public init(
        term: String,
        score: Double,
        shortCount: Int,
        baselineCount: Int,
        uniqueSources: Int,
        burstZ: Double,
        burstRatio: Double,
        titleShare: Double,
        sampleHeadlines: [String],
        lastSeenAt: Date
    ) {
        self.term = term
        self.score = score
        self.shortCount = shortCount
        self.baselineCount = baselineCount
        self.uniqueSources = uniqueSources
        self.burstZ = burstZ
        self.burstRatio = burstRatio
        self.titleShare = titleShare
        self.sampleHeadlines = sampleHeadlines
        self.lastSeenAt = lastSeenAt
    }
}

public actor TrendDetector {
    public struct Configuration: Sendable, Codable, Hashable {
        public struct Weights: Sendable, Codable, Hashable {
            public var burstWeight: Double
            public var countWeight: Double
            public var accelWeight: Double
            public var sourceWeight: Double
            public var titleCaseWeight: Double
            public var allCapsWeight: Double
            public var titleTokenWeight: Double
            public var bodyTokenWeight: Double

            public init(
                burstWeight: Double = 1.0,
                countWeight: Double = 0.6,
                accelWeight: Double = 0.2,
                sourceWeight: Double = 0.3,
                titleCaseWeight: Double = 1.25,
                allCapsWeight: Double = 1.5,
                titleTokenWeight: Double = 1.0,
                bodyTokenWeight: Double = 0.6
            ) {
                self.burstWeight = burstWeight
                self.countWeight = countWeight
                self.accelWeight = accelWeight
                self.sourceWeight = sourceWeight
                self.titleCaseWeight = titleCaseWeight
                self.allCapsWeight = allCapsWeight
                self.titleTokenWeight = titleTokenWeight
                self.bodyTokenWeight = bodyTokenWeight
            }
        }

        public var shortWindow: TimeInterval
        public var baselineWindow: TimeInterval
        public var bucketSize: TimeInterval
        public var maxTermsPerItem: Int
        public var selectTopTermsPerItem: Bool
        public var minTokenLength: Int
        public var enableBigrams: Bool
        public var enableTrigrams: Bool
        public var enableTitleCasePhrases: Bool
        public var allowNumericTokens: Bool
        public var summaryMaxLength: Int

        public var stopwords: Set<String>
        public var bannedTerms: Set<String>
        public var aliasMap: [String: String]
        public var filterStopwordsInPhrases: Bool
        public var phraseStopwords: Set<String>

        public var enableDedupe: Bool
        public var maxItemsPerSourcePerBucket: Int?

        public var minShortCount: Int
        public var minUniqueSources: Int
        public var lowercaseMinShortCount: Int
        public var lowercaseMinUniqueSources: Int

        public var enableDynamicStopwords: Bool
        public var dynamicStopwordBaselineMin: Int
        public var dynamicStopwordBurstZMax: Double
        public var dynamicStopwordBurstRatioMax: Double
        public var dynamicStopwordMinSources: Int

        public var weights: Weights
        public var topicLimit: Int
        public var sampleHeadlineLimit: Int

        public init(
            shortWindow: TimeInterval = 15 * 60,
            baselineWindow: TimeInterval = 6 * 60 * 60,
            bucketSize: TimeInterval = 60,
            maxTermsPerItem: Int = 40,
            selectTopTermsPerItem: Bool = false,
            minTokenLength: Int = 3,
            enableBigrams: Bool = true,
            enableTrigrams: Bool = false,
            enableTitleCasePhrases: Bool = true,
            allowNumericTokens: Bool = false,
            summaryMaxLength: Int = 500,
            stopwords: Set<String> = Configuration.defaultStopwords,
            bannedTerms: Set<String> = [],
            aliasMap: [String: String] = [:],
            filterStopwordsInPhrases: Bool = false,
            phraseStopwords: Set<String> = Configuration.defaultPhraseStopwords,
            enableDedupe: Bool = true,
            maxItemsPerSourcePerBucket: Int? = 5,
            minShortCount: Int = 2,
            minUniqueSources: Int = 2,
            lowercaseMinShortCount: Int? = nil,
            lowercaseMinUniqueSources: Int? = nil,
            enableDynamicStopwords: Bool = true,
            dynamicStopwordBaselineMin: Int = 12,
            dynamicStopwordBurstZMax: Double = 0.35,
            dynamicStopwordBurstRatioMax: Double = 1.2,
            dynamicStopwordMinSources: Int = 3,
            weights: Weights = Weights(),
            topicLimit: Int = 30,
            sampleHeadlineLimit: Int = 5
        ) {
            self.shortWindow = shortWindow
            self.baselineWindow = baselineWindow
            self.bucketSize = bucketSize
            self.maxTermsPerItem = maxTermsPerItem
            self.selectTopTermsPerItem = selectTopTermsPerItem
            self.minTokenLength = minTokenLength
            self.enableBigrams = enableBigrams
            self.enableTrigrams = enableTrigrams
            self.enableTitleCasePhrases = enableTitleCasePhrases
            self.allowNumericTokens = allowNumericTokens
            self.summaryMaxLength = summaryMaxLength
            self.stopwords = stopwords
            self.bannedTerms = bannedTerms
            self.aliasMap = aliasMap
            self.filterStopwordsInPhrases = filterStopwordsInPhrases
            self.phraseStopwords = phraseStopwords
            self.enableDedupe = enableDedupe
            self.maxItemsPerSourcePerBucket = maxItemsPerSourcePerBucket
            self.minShortCount = minShortCount
            self.minUniqueSources = minUniqueSources
            self.lowercaseMinShortCount = lowercaseMinShortCount ?? max(minShortCount, 3)
            self.lowercaseMinUniqueSources = lowercaseMinUniqueSources ?? minUniqueSources
            self.enableDynamicStopwords = enableDynamicStopwords
            self.dynamicStopwordBaselineMin = dynamicStopwordBaselineMin
            self.dynamicStopwordBurstZMax = dynamicStopwordBurstZMax
            self.dynamicStopwordBurstRatioMax = dynamicStopwordBurstRatioMax
            self.dynamicStopwordMinSources = dynamicStopwordMinSources
            self.weights = weights
            self.topicLimit = topicLimit
            self.sampleHeadlineLimit = sampleHeadlineLimit
        }

        public static let defaultStopwords: Set<String> = [
            "a", "about", "after", "against", "all", "amid", "an", "and", "are", "as",
            "at", "be", "been", "before", "being", "breaking", "but", "by", "for",
            "from", "has", "have", "he", "her", "his", "how", "if", "in", "into",
            "is", "it", "its", "latest", "live", "me", "more", "my", "new", "no",
            "not", "of", "off", "on", "one", "opinion", "or", "our", "out", "over",
            "says", "said", "she", "so", "than", "that", "the", "their", "them",
            "then", "there", "these", "they", "this", "those", "to", "under", "up",
            "update", "us", "was", "watch", "we", "were", "what", "when", "where",
            "which", "who", "why", "with", "you", "your",
            "near", "people",
            "can", "cant", "could", "couldnt", "may", "might", "must", "should",
            "shouldnt", "will", "wont", "would", "wouldnt", "dont", "doesnt",
            "didnt", "isnt", "arent", "wasnt", "werent", "hasnt", "havent", "hadnt",
            "day", "days", "week", "weeks", "month", "months", "year", "years",
            "today", "tonight", "yesterday", "tomorrow",
            "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
            "january", "february", "march", "april", "may", "june", "july", "august",
            "september", "october", "november", "december",
            "file", "files", "video", "videos"
        ]

        public static let defaultPhraseStopwords: Set<String> = [
            "guard", "guards", "guarded", "guarding",
            "says", "said", "say", "saying",
            "warn", "warns", "warned", "warning",
            "calls", "called", "calling",
            "backs", "backed", "backing",
            "reports", "reported", "reporting",
            "sees", "saw", "seeing"
        ]
    }

    private struct Bucket {
        var termCounts: [String: Int] = [:]
        var termTitleCounts: [String: Int] = [:]
        var termSources: [String: Set<String>] = [:]
        var sourceItemCounts: [String: Int] = [:]
    }

    private enum CaseStyle: Int, Comparable {
        case normal = 0
        case titleCase = 1
        case allCaps = 2

        static func < (lhs: CaseStyle, rhs: CaseStyle) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }

    private struct Token {
        let value: String
        let caseStyle: CaseStyle
    }

    private struct TermCandidate {
        let term: String
        let caseStyle: CaseStyle
        let isTitle: Bool
    }

    private struct HeadlineSample: Hashable {
        let headline: String
        let sourceID: String
        let publishedAt: Date
    }

    private var configuration: Configuration
    private var buckets: [Int: Bucket] = [:]
    private var termSamples: [String: [HeadlineSample]] = [:]
    private var lastSeenAt: [String: Date] = [:]
    private var termCaseStyles: [String: CaseStyle] = [:]
    private var dynamicStopwords: [String: Date] = [:]
    private var recentURLSeen: [String: Date] = [:]
    private var recentTitleSeen: [String: Date] = [:]

    public init(configuration: Configuration = Configuration()) {
        var normalizedConfig = configuration
        if !configuration.aliasMap.isEmpty {
            var normalized: [String: String] = [:]
            normalized.reserveCapacity(configuration.aliasMap.count)
            for (key, value) in configuration.aliasMap {
                normalized[key.lowercased()] = value.lowercased()
            }
            normalizedConfig.aliasMap = normalized
        }
        self.configuration = normalizedConfig
    }

    public func ingest(_ item: NewsItem) {
        ingest([item])
    }

    public func ingest(_ items: [NewsItem]) {
        guard !items.isEmpty else { return }
        for item in items {
            ingestSingle(item)
        }
        let cutoff = Date().addingTimeInterval(-configuration.baselineWindow)
        expireBuckets(olderThan: cutoff)
        expireDedupe(olderThan: cutoff)
        expireDynamicStopwords(olderThan: cutoff)
    }

    public func trending(now: Date = .init()) -> [TrendingTopic] {
        let cutoff = now.addingTimeInterval(-configuration.baselineWindow)
        expireBuckets(olderThan: cutoff)
        expireDedupe(olderThan: cutoff)
        expireDynamicStopwords(olderThan: cutoff)
        pruneSamples(olderThan: cutoff)

        let shortStart = now.addingTimeInterval(-configuration.shortWindow)
        let prevShortStart = now.addingTimeInterval(-configuration.shortWindow * 2)
        let baselineStart = now.addingTimeInterval(-configuration.baselineWindow)

        var shortCounts: [String: Int] = [:]
        var prevShortCounts: [String: Int] = [:]
        var baselineCounts: [String: Int] = [:]
        var shortTitleCounts: [String: Int] = [:]
        var prevShortTitleCounts: [String: Int] = [:]
        var baselineTitleCounts: [String: Int] = [:]
        var shortSources: [String: Set<String>] = [:]

        for (bucketKey, bucket) in buckets {
            let bucketStart = date(forBucketKey: bucketKey)
            if bucketStart < baselineStart {
                continue
            }
            let inShort = bucketStart >= shortStart
            let inPrevShort = bucketStart >= prevShortStart && bucketStart < shortStart

            for (term, count) in bucket.termCounts {
                let titleCount = bucket.termTitleCounts[term, default: 0]
                baselineCounts[term, default: 0] += count
                baselineTitleCounts[term, default: 0] += titleCount
                if inShort {
                    shortCounts[term, default: 0] += count
                    shortTitleCounts[term, default: 0] += titleCount
                } else if inPrevShort {
                    prevShortCounts[term, default: 0] += count
                    prevShortTitleCounts[term, default: 0] += titleCount
                }
            }

            if inShort {
                for (term, sources) in bucket.termSources {
                    if shortSources[term] == nil {
                        shortSources[term] = sources
                    } else {
                        shortSources[term]?.formUnion(sources)
                    }
                }
            }
        }

        var topics: [TrendingTopic] = []
        topics.reserveCapacity(shortCounts.count)

        for (term, shortCount) in shortCounts {
            let uniqueSources = shortSources[term]?.count ?? 0
            let baselineCount = baselineCounts[term, default: 0]
            let prevShortCount = prevShortCounts[term, default: 0]

            let shortTitleCount = shortTitleCounts[term, default: 0]
            let prevShortTitleCount = prevShortTitleCounts[term, default: 0]
            let baselineTitleCount = baselineTitleCounts[term, default: 0]

            let weightedShort = weightedCount(total: shortCount, title: shortTitleCount)
            let weightedPrevShort = weightedCount(total: prevShortCount, title: prevShortTitleCount)
            let weightedBaseline = weightedCount(total: baselineCount, title: baselineTitleCount)
            let expectedShort = weightedBaseline * (configuration.shortWindow / max(1, configuration.baselineWindow))

            let caseStyle = termCaseStyles[term] ?? .normal
            let minShort = caseStyle == .normal ? configuration.lowercaseMinShortCount : configuration.minShortCount
            let minSources = caseStyle == .normal ? configuration.lowercaseMinUniqueSources : configuration.minUniqueSources

            if weightedShort < Double(minShort) {
                continue
            }
            if uniqueSources < minSources {
                continue
            }

            let burstZ = (weightedShort - expectedShort) / sqrt(expectedShort + 1.0)
            let burstRatio = weightedShort / max(1.0, expectedShort)
            let weightedAcceleration = weightedShort - weightedPrevShort

            if shouldAutoStopword(
                term: term,
                caseStyle: caseStyle,
                baselineCount: baselineCount,
                uniqueSources: uniqueSources,
                burstZ: burstZ,
                burstRatio: burstRatio,
                now: now
            ) {
                continue
            }

            let countFactor = configuration.weights.countWeight * log(1 + weightedShort)
            let accelFactor = configuration.weights.accelWeight * max(0, weightedAcceleration)
            let sourceFactor = 1 + configuration.weights.sourceWeight * max(0, Double(uniqueSources - 1))
            let score = (configuration.weights.burstWeight * max(0, burstZ) + countFactor + accelFactor)
                * sourceFactor
                * caseWeight(for: term)

            let titleShare = Double(shortTitleCount) / Double(max(1, shortCount))
            let samples = sampleHeadlines(for: term, limit: configuration.sampleHeadlineLimit)
            let lastSeen = lastSeenAt[term] ?? now

            topics.append(
                TrendingTopic(
                    term: term,
                    score: score,
                    shortCount: shortCount,
                    baselineCount: baselineCount,
                    uniqueSources: uniqueSources,
                    burstZ: burstZ,
                    burstRatio: burstRatio,
                    titleShare: titleShare,
                    sampleHeadlines: samples,
                    lastSeenAt: lastSeen
                )
            )
        }

        topics.sort {
            if $0.score != $1.score {
                return $0.score > $1.score
            }
            if $0.shortCount != $1.shortCount {
                return $0.shortCount > $1.shortCount
            }
            return $0.lastSeenAt > $1.lastSeenAt
        }

        if topics.count > configuration.topicLimit {
            return Array(topics.prefix(configuration.topicLimit))
        }
        return topics
    }

    public func reset() {
        buckets.removeAll()
        termSamples.removeAll()
        lastSeenAt.removeAll()
        termCaseStyles.removeAll()
        dynamicStopwords.removeAll()
        recentURLSeen.removeAll()
        recentTitleSeen.removeAll()
    }

    public func updateAliasMap(_ aliasMap: [String: String]) {
        var normalized: [String: String] = [:]
        normalized.reserveCapacity(aliasMap.count)
        for (key, value) in aliasMap {
            normalized[key.lowercased()] = value.lowercased()
        }
        configuration.aliasMap = normalized
    }

    public func addAliasMappings(_ mappings: [String: String]) {
        for (key, value) in mappings {
            configuration.aliasMap[key.lowercased()] = value.lowercased()
        }
    }

    public func addStopwords(_ stopwords: [String]) {
        for word in stopwords {
            configuration.stopwords.insert(word.lowercased())
        }
    }

    public func removeStopwords(_ stopwords: [String]) {
        for word in stopwords {
            configuration.stopwords.remove(word.lowercased())
        }
    }

    public func addPhraseStopwords(_ stopwords: [String]) {
        for word in stopwords {
            configuration.phraseStopwords.insert(word.lowercased())
        }
    }

    public func removePhraseStopwords(_ stopwords: [String]) {
        for word in stopwords {
            configuration.phraseStopwords.remove(word.lowercased())
        }
    }

    public func clearDynamicStopwords() {
        dynamicStopwords.removeAll()
    }

    private func ingestSingle(_ item: NewsItem) {
        let timestamp = item.publishedAt
        let cutoff = timestamp.addingTimeInterval(-configuration.baselineWindow)
        if configuration.enableDedupe {
            if isDuplicate(item, cutoff: cutoff) {
                return
            }
            recordDedupe(item, timestamp: timestamp)
        }

        let bucketKey = bucketKey(for: timestamp)
        var bucket = buckets[bucketKey, default: Bucket()]

        let sourceID = item.source
        if let cap = configuration.maxItemsPerSourcePerBucket, cap > 0 {
            let count = bucket.sourceItemCounts[sourceID, default: 0]
            if count >= cap {
                buckets[bucketKey] = bucket
                return
            }
            bucket.sourceItemCounts[sourceID] = count + 1
        }

        let terms = extractTerms(from: item)
        guard !terms.isEmpty else {
            buckets[bucketKey] = bucket
            return
        }

        for candidate in terms {
            let term = candidate.term
            bucket.termCounts[term, default: 0] += 1
            if candidate.isTitle {
                bucket.termTitleCounts[term, default: 0] += 1
            }
            if bucket.termSources[term] == nil {
                bucket.termSources[term] = [sourceID]
            } else {
                bucket.termSources[term]?.insert(sourceID)
            }
            addSample(term: term, headline: item.title, sourceID: sourceID, publishedAt: timestamp)
            recordCaseStyle(term: term, caseStyle: candidate.caseStyle)
            if let existing = lastSeenAt[term] {
                if timestamp > existing {
                    lastSeenAt[term] = timestamp
                }
            } else {
                lastSeenAt[term] = timestamp
            }
        }

        buckets[bucketKey] = bucket
    }

    private func extractTerms(from item: NewsItem) -> [TermCandidate] {
        let titleText = stripURLSubstrings(from: item.title)
        let summary = item.body.map { String($0.prefix(configuration.summaryMaxLength)) } ?? ""
        let bodyText = stripURLSubstrings(from: summary)

        let titleTokens = normalizedTokens(from: titleText)
        let bodyTokens = normalizedTokens(from: bodyText)

        var terms: [TermCandidate] = []
        var seen: [String: Int] = [:]

        func addTerm(_ term: String, caseStyle: CaseStyle, isTitle: Bool) {
            if !configuration.selectTopTermsPerItem {
                guard terms.count < configuration.maxTermsPerItem else { return }
            }
            if let index = seen[term] {
                let current = terms[index]
                let mergedCase = max(current.caseStyle, caseStyle)
                let mergedTitle = current.isTitle || isTitle
                if mergedCase != current.caseStyle || mergedTitle != current.isTitle {
                    terms[index] = TermCandidate(term: term, caseStyle: mergedCase, isTitle: mergedTitle)
                }
                return
            }
            seen[term] = terms.count
            terms.append(TermCandidate(term: term, caseStyle: caseStyle, isTitle: isTitle))
        }

        func addTokens(_ tokens: [Token], isTitle: Bool) {
            for token in tokens {
                addTerm(canonicalize(term: token.value), caseStyle: token.caseStyle, isTitle: isTitle)
            }

            if configuration.enableBigrams && tokens.count >= 2 {
                for index in 0..<(tokens.count - 1) {
                    let phrase = tokens[index].value + " " + tokens[index + 1].value
                    if shouldFilterPhrase(phrase, includeStopwords: configuration.filterStopwordsInPhrases) {
                        continue
                    }
                    let phraseCase = caseStyle(for: [tokens[index], tokens[index + 1]])
                    addTerm(canonicalize(term: phrase), caseStyle: phraseCase, isTitle: isTitle)
                }
            }

            if configuration.enableTrigrams && tokens.count >= 3 {
                for index in 0..<(tokens.count - 2) {
                    let phrase = tokens[index].value + " " + tokens[index + 1].value + " " + tokens[index + 2].value
                    if shouldFilterPhrase(phrase, includeStopwords: configuration.filterStopwordsInPhrases) {
                        continue
                    }
                    let phraseCase = caseStyle(for: [tokens[index], tokens[index + 1], tokens[index + 2]])
                    addTerm(canonicalize(term: phrase), caseStyle: phraseCase, isTitle: isTitle)
                }
            }
        }

        addTokens(titleTokens, isTitle: true)
        addTokens(bodyTokens, isTitle: false)

        if configuration.enableTitleCasePhrases {
            let phrases = titleCasePhrases(from: item.title)
            for phrase in phrases {
                addTerm(canonicalize(term: phrase), caseStyle: .titleCase, isTitle: true)
            }
        }

        if configuration.selectTopTermsPerItem {
            return selectTopTerms(from: terms)
        }
        return terms
    }

    private func normalizedTokens(from text: String) -> [Token] {
        var buffer: [String] = []
        buffer.reserveCapacity(64)
        var current = ""
        current.reserveCapacity(16)

        for scalar in text.unicodeScalars {
            if CharacterSet.alphanumerics.contains(scalar) {
                current.unicodeScalars.append(scalar)
            } else if isApostropheScalar(scalar) {
                if !current.isEmpty {
                    current.unicodeScalars.append(scalar)
                }
            } else {
                if !current.isEmpty {
                    buffer.append(current)
                    current.removeAll(keepingCapacity: true)
                }
            }
        }
        if !current.isEmpty {
            buffer.append(current)
        }

        var tokens: [Token] = []
        tokens.reserveCapacity(buffer.count)
        for raw in buffer {
            let stripped = stripApostrophes(from: raw)
            if stripped.isEmpty { continue }

            let isAllCaps = isAllCapsToken(stripped)
            let isTitleCase = !isAllCaps && isTitleCaseToken(stripped)
            let isShort = stripped.count < configuration.minTokenLength
            let allowShort = isShort && (isAllCaps || isShortProperNoun(stripped))

            if isShort && !allowShort { continue }

            let normalized = stripped.lowercased()
            if configuration.bannedTerms.contains(normalized) { continue }
            if configuration.stopwords.contains(normalized) && !isAllCaps { continue }
            if configuration.enableDynamicStopwords,
               dynamicStopwords[normalized] != nil,
               !isAllCaps {
                continue
            }
            if !configuration.allowNumericTokens && normalized.unicodeScalars.allSatisfy({ CharacterSet.decimalDigits.contains($0) }) {
                continue
            }

            let tokenValue = isAllCaps ? stripped.uppercased() : normalized
            let caseStyle: CaseStyle = isAllCaps ? .allCaps : (isTitleCase ? .titleCase : .normal)
            tokens.append(Token(value: tokenValue, caseStyle: caseStyle))
        }
        return tokens
    }

    private func titleCasePhrases(from title: String) -> [String] {
        let rawTokens = title.split { $0.isWhitespace }
        let tokens = rawTokens.map { trimNonAlnum(String($0)) }.filter { !$0.isEmpty }
        guard tokens.count >= 2 else { return [] }

        var phrases: [String] = []
        var current: [String] = []

        func flushCurrent() {
            if current.count >= 2 {
                let phrase = current.joined(separator: " ")
                phrases.append(phrase)
            }
            current.removeAll(keepingCapacity: true)
        }

        for token in tokens {
            if isTitleCaseToken(token) || isAllCapsToken(token) {
                current.append(token)
            } else {
                flushCurrent()
            }
        }
        flushCurrent()

        return phrases
    }

    private func caseStyle(for tokens: [Token]) -> CaseStyle {
        guard !tokens.isEmpty else { return .normal }
        if tokens.allSatisfy({ $0.caseStyle == .allCaps }) {
            return .allCaps
        }
        if tokens.allSatisfy({ $0.caseStyle == .titleCase || $0.caseStyle == .allCaps }) {
            return .titleCase
        }
        return .normal
    }

    private func isTitleCaseToken(_ token: String) -> Bool {
        guard let firstScalar = token.unicodeScalars.first else { return false }
        if !CharacterSet.uppercaseLetters.contains(firstScalar) {
            return false
        }
        let hasLowercase = token.unicodeScalars.contains { CharacterSet.lowercaseLetters.contains($0) }
        return hasLowercase || token == token.uppercased()
    }

    private func trimNonAlnum(_ value: String) -> String {
        value.trimmingCharacters(in: CharacterSet.alphanumerics.inverted)
    }

    private func shouldFilterPhrase(_ phrase: String, includeStopwords: Bool) -> Bool {
        let parts = phrase.split(separator: " ")
        for part in parts {
            let token = part.lowercased()
            if includeStopwords && configuration.stopwords.contains(token) { return true }
            if configuration.bannedTerms.contains(token) { return true }
            if configuration.phraseStopwords.contains(token) { return true }
        }
        return false
    }

    private func canonicalize(term: String) -> String {
        let lowered = term.lowercased()
        if let mapped = configuration.aliasMap[lowered] {
            return mapped
        }
        if isAllCapsToken(term) {
            return term.uppercased()
        }
        return lowered
    }

    private func recordCaseStyle(term: String, caseStyle: CaseStyle) {
        guard caseStyle != .normal else { return }
        if let existing = termCaseStyles[term] {
            if caseStyle > existing {
                termCaseStyles[term] = caseStyle
            }
        } else {
            termCaseStyles[term] = caseStyle
        }
    }

    private func caseWeight(for term: String) -> Double {
        let style = termCaseStyles[term] ?? .normal
        switch style {
        case .allCaps:
            return configuration.weights.allCapsWeight
        case .titleCase:
            return configuration.weights.titleCaseWeight
        case .normal:
            return 1.0
        }
    }

    private func candidateScore(_ candidate: TermCandidate) -> Double {
        let caseWeight: Double
        switch candidate.caseStyle {
        case .allCaps:
            caseWeight = configuration.weights.allCapsWeight
        case .titleCase:
            caseWeight = configuration.weights.titleCaseWeight
        case .normal:
            caseWeight = 1.0
        }
        let titleWeight = candidate.isTitle ? configuration.weights.titleTokenWeight : configuration.weights.bodyTokenWeight
        let tokenCount = max(1, candidate.term.split(separator: " ").count)
        let length = candidate.term.replacingOccurrences(of: " ", with: "").count
        let phraseBoost = 1.0 + Double(tokenCount - 1) * 0.35
        let lengthBoost = 1.0 + min(20.0, Double(length)) / 20.0 * 0.15
        return caseWeight * titleWeight * phraseBoost * lengthBoost
    }

    private func selectTopTerms(from terms: [TermCandidate]) -> [TermCandidate] {
        let limit = configuration.maxTermsPerItem
        guard limit > 0 else { return [] }
        let sorted = terms.sorted {
            let leftScore = candidateScore($0)
            let rightScore = candidateScore($1)
            if leftScore != rightScore {
                return leftScore > rightScore
            }
            if $0.caseStyle != $1.caseStyle {
                return $0.caseStyle > $1.caseStyle
            }
            if $0.isTitle != $1.isTitle {
                return $0.isTitle && !$1.isTitle
            }
            if $0.term.count != $1.term.count {
                return $0.term.count > $1.term.count
            }
            return $0.term < $1.term
        }
        return Array(sorted.prefix(limit))
    }

    private func weightedCount(total: Int, title: Int) -> Double {
        let titleCount = max(0, title)
        let bodyCount = max(0, total - titleCount)
        return Double(titleCount) * configuration.weights.titleTokenWeight
            + Double(bodyCount) * configuration.weights.bodyTokenWeight
    }

    private func shouldAutoStopword(
        term: String,
        caseStyle: CaseStyle,
        baselineCount: Int,
        uniqueSources: Int,
        burstZ: Double,
        burstRatio: Double,
        now: Date
    ) -> Bool {
        guard configuration.enableDynamicStopwords else { return false }
        guard caseStyle == .normal else { return false }
        guard baselineCount >= configuration.dynamicStopwordBaselineMin else { return false }
        guard uniqueSources >= configuration.dynamicStopwordMinSources else { return false }
        if burstZ <= configuration.dynamicStopwordBurstZMax && burstRatio <= configuration.dynamicStopwordBurstRatioMax {
            dynamicStopwords[term.lowercased()] = now
            return true
        }
        return false
    }

    private func isDuplicate(_ item: NewsItem, cutoff: Date) -> Bool {
        let urlKey = item.url.absoluteString.lowercased()
        if let seen = recentURLSeen[urlKey], seen >= cutoff {
            return true
        }

        let titleKey = normalizeTitleForDedupe(item.title)
        if let seen = recentTitleSeen[titleKey], seen >= cutoff {
            return true
        }

        return false
    }

    private func recordDedupe(_ item: NewsItem, timestamp: Date) {
        let urlKey = item.url.absoluteString.lowercased()
        recentURLSeen[urlKey] = timestamp
        let titleKey = normalizeTitleForDedupe(item.title)
        recentTitleSeen[titleKey] = timestamp
    }

    private func expireDedupe(olderThan cutoff: Date) {
        recentURLSeen.keys.filter { (recentURLSeen[$0] ?? cutoff) < cutoff }.forEach {
            recentURLSeen.removeValue(forKey: $0)
        }
        recentTitleSeen.keys.filter { (recentTitleSeen[$0] ?? cutoff) < cutoff }.forEach {
            recentTitleSeen.removeValue(forKey: $0)
        }
    }

    private func expireDynamicStopwords(olderThan cutoff: Date) {
        for (term, timestamp) in dynamicStopwords where timestamp < cutoff {
            dynamicStopwords.removeValue(forKey: term)
        }
    }

    private func normalizeTitleForDedupe(_ title: String) -> String {
        title.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private func stripApostrophes(from value: String) -> String {
        var result = String.UnicodeScalarView()
        result.reserveCapacity(value.unicodeScalars.count)
        for scalar in value.unicodeScalars where !isApostropheScalar(scalar) {
            result.append(scalar)
        }
        return String(result)
    }

    private func isAllCapsToken(_ token: String) -> Bool {
        var letterCount = 0
        for scalar in token.unicodeScalars {
            if CharacterSet.letters.contains(scalar) {
                letterCount += 1
                if !CharacterSet.uppercaseLetters.contains(scalar) {
                    return false
                }
            }
        }
        return letterCount >= 2
    }

    private func isShortProperNoun(_ token: String) -> Bool {
        guard token.count >= 2, token.count < configuration.minTokenLength else { return false }
        guard let first = token.unicodeScalars.first, CharacterSet.uppercaseLetters.contains(first) else {
            return false
        }
        for scalar in token.unicodeScalars.dropFirst() {
            if !CharacterSet.lowercaseLetters.contains(scalar) {
                return false
            }
        }
        return true
    }

    private func isApostropheScalar(_ scalar: Unicode.Scalar) -> Bool {
        switch scalar.value {
        case 0x27, 0x2018, 0x2019:
            return true
        default:
            return false
        }
    }

    private func addSample(term: String, headline: String, sourceID: String, publishedAt: Date) {
        guard configuration.sampleHeadlineLimit > 0 else { return }
        let sample = HeadlineSample(headline: headline, sourceID: sourceID, publishedAt: publishedAt)
        var samples = termSamples[term, default: []]

        if let index = samples.firstIndex(where: { $0.headline == headline }) {
            if publishedAt > samples[index].publishedAt {
                samples[index] = sample
            }
        } else {
            samples.append(sample)
        }

        samples.sort { $0.publishedAt > $1.publishedAt }
        let cap = max(1, configuration.sampleHeadlineLimit * 3)
        if samples.count > cap {
            samples = Array(samples.prefix(cap))
        }
        termSamples[term] = samples
    }

    private func sampleHeadlines(for term: String, limit: Int) -> [String] {
        guard limit > 0 else { return [] }
        guard let samples = termSamples[term], !samples.isEmpty else { return [] }

        var selected: [String] = []
        var usedSources: Set<String> = []
        for sample in samples {
            if selected.count >= limit { break }
            if usedSources.contains(sample.sourceID) { continue }
            selected.append(sample.headline)
            usedSources.insert(sample.sourceID)
        }
        if selected.count < limit {
            for sample in samples {
                if selected.count >= limit { break }
                if selected.contains(sample.headline) { continue }
                selected.append(sample.headline)
            }
        }
        return selected
    }

    private func bucketKey(for date: Date) -> Int {
        let interval = date.timeIntervalSince1970
        let size = max(1.0, configuration.bucketSize)
        return Int(floor(interval / size))
    }

    private func date(forBucketKey key: Int) -> Date {
        let size = max(1.0, configuration.bucketSize)
        return Date(timeIntervalSince1970: TimeInterval(key) * size)
    }

    private func expireBuckets(olderThan cutoff: Date) {
        let minKey = bucketKey(for: cutoff)
        buckets.keys.filter { $0 < minKey }.forEach { buckets.removeValue(forKey: $0) }
    }

    private func pruneSamples(olderThan cutoff: Date) {
        for (term, samples) in termSamples {
            let filtered = samples.filter { $0.publishedAt >= cutoff }
            if filtered.isEmpty {
                termSamples.removeValue(forKey: term)
            } else {
                termSamples[term] = filtered
            }
        }
        for (term, lastSeen) in lastSeenAt where lastSeen < cutoff {
            lastSeenAt.removeValue(forKey: term)
            termCaseStyles.removeValue(forKey: term)
        }
    }

    private func stripURLSubstrings(from text: String) -> String {
        let parts = text.split(whereSeparator: { $0.isWhitespace || $0.isNewline })
        guard !parts.isEmpty else { return text }

        var cleaned: [Substring] = []
        cleaned.reserveCapacity(parts.count)
        for part in parts {
            let token = String(part)
            if isURLLikeSegment(token) {
                continue
            }
            cleaned.append(part)
        }
        return cleaned.joined(separator: " ")
    }

    private func isURLLikeSegment(_ token: String) -> Bool {
        let lowered = token.lowercased()
        if lowered.hasPrefix("http://") || lowered.hasPrefix("https://") || lowered.hasPrefix("www.") {
            return true
        }
        if lowered.contains("://") {
            return true
        }
        if lowered.contains("/") && lowered.contains(".") {
            return true
        }
        if lowered.contains("@") && lowered.contains(".") {
            return true
        }
        let parts = lowered.split(separator: ".")
        if parts.count >= 2, let last = parts.last {
            return urlTlds.contains(String(last))
        }
        return false
    }

    private let urlTlds: Set<String> = [
        "com", "org", "net", "gov", "edu", "io", "co", "uk", "us", "ca", "de",
        "fr", "it", "es", "ru", "cn", "info", "biz", "me", "tv", "ai"
    ]
}
