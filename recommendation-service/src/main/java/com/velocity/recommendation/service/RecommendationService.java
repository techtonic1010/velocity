package com.velocity.recommendation.service;

import com.velocity.recommendation.client.VectorHasherClient;
import com.velocity.recommendation.dto.InteractionType;
import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.dto.RecommendationItem;
import com.velocity.recommendation.dto.RecommendationResponse;
import com.velocity.recommendation.repository.EntityHistoryLookupRepository;
import com.velocity.recommendation.repository.EntityLookupRepository;
import com.velocity.recommendation.repository.EntityLookupRepository.EntitySummary;
import com.velocity.recommendation.service.CandidateRanker.RankedCandidate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
// GET /recommendations/{userId}
//             │
//             ▼
// 1. Resolve seed entities
//    (Redis lastEntities → Postgres fallback)
//             │
//             ▼
// 2. Fetch neighbors for each seed
//    (parallel HTTP calls to vector-hasher)
//             │
//             ▼
// 3. Resolve interaction signals
//    (Redis signals → Postgres fallback)
//             │
//             ▼
// 4. Rank candidates
//    (CandidateRanker)
//             │
//             ▼
// 5. Remove already-seen entities
//    (Bloom filter + Postgres confirmation)
//             │
//             ▼
// 6. Keep Top-K recommendations
//             │
//             ▼
// 7. Fetch title/category
//    (EntityLookupRepository)
//             │
//             ▼
// 8. Return RecommendationResponse
// Orchestrates GET /recommendations: seeds (with Postgres fallback) -> parallel neighbor fetch ->
// merge/rank (CandidateRanker) -> Bloom-gated batched filter -> top-K -> entity lookup -> response.
// See MILESTONE_5_PLAN.md for the full reasoning behind each step.
@Service
public class RecommendationService {

    private final StringRedisTemplate redisTemplate;
    private final EntityHistoryLookupRepository entityHistoryLookupRepository;
    private final VectorHasherClient vectorHasherClient;
    private final BloomFilterReadService bloomFilterReadService;
    private final EntityLookupRepository entityLookupRepository;
    private final int lastNSize;
    private final int topK;

    public RecommendationService(
            StringRedisTemplate redisTemplate,
            EntityHistoryLookupRepository entityHistoryLookupRepository,
            VectorHasherClient vectorHasherClient,
            BloomFilterReadService bloomFilterReadService,
            EntityLookupRepository entityLookupRepository,
            @Value("${redis.last-n-size}") int lastNSize,
            @Value("${recommendation.top-k}") int topK) {
        this.redisTemplate = redisTemplate;
        this.entityHistoryLookupRepository = entityHistoryLookupRepository;
        this.vectorHasherClient = vectorHasherClient;
        this.bloomFilterReadService = bloomFilterReadService;
        this.entityLookupRepository = entityLookupRepository;
        this.lastNSize = lastNSize;
        this.topK = topK;
    }

    public RecommendationResponse getRecommendations(String userId) {
        List<String> seeds = resolveSeeds(userId);
        if (seeds.isEmpty()) {
            return new RecommendationResponse(userId, List.of());
        }

        Map<String, List<NeighborEntry>> neighborsBySeed = fetchNeighborsInParallel(seeds);
        Map<String, InteractionType> signalsBySeed = resolveSignals(userId, seeds);

        List<RankedCandidate> ranked = CandidateRanker.rankCandidates(neighborsBySeed, signalsBySeed);
        List<RankedCandidate> survivors = filterSeenAndTruncate(userId, ranked);

        return new RecommendationResponse(userId, buildItems(survivors));
    }

    // Step 1: an empty Redis result is ambiguous (cold-start vs. lost state), so it falls back to
    // the durable record rather than being read as "no history."
    private List<String> resolveSeeds(String userId) {
        List<String> lastEntities = redisTemplate.opsForList().range(lastEntitiesKey(userId), 0, -1);
        if (lastEntities != null && !lastEntities.isEmpty()) {
            return lastEntities;
        }
        return entityHistoryLookupRepository.findRecentEntityIds(userId, lastNSize);
    }

    // Step 2: one HTTP call per seed, run concurrently — at most lastNSize calls, cheap enough for
    // the default common pool rather than a dedicated executor.
    private Map<String, List<NeighborEntry>> fetchNeighborsInParallel(List<String> seeds) {
        Map<String, CompletableFuture<Optional<List<NeighborEntry>>>> futuresBySeed = new LinkedHashMap<>();
        for (String seedId : seeds) {
            futuresBySeed.put(seedId, CompletableFuture.supplyAsync(() -> vectorHasherClient.fetchNeighbors(seedId)));
        }
        CompletableFuture.allOf(futuresBySeed.values().toArray(new CompletableFuture[0])).join();

        Map<String, List<NeighborEntry>> neighborsBySeed = new LinkedHashMap<>();
        futuresBySeed.forEach((seedId, future) -> future.join().ifPresent(neighbors -> neighborsBySeed.put(seedId, neighbors)));
        return neighborsBySeed;
    }

    // Step 4: same existence-gated fallback shape as step 1 — a missing signals key means Redis
    // lost it (every processed entity gets one unconditionally), not that it never existed.
    private Map<String, InteractionType> resolveSignals(String userId, List<String> seeds) {
        String key = signalsKey(userId);
        if (!Boolean.TRUE.equals(redisTemplate.hasKey(key))) {
            return entityHistoryLookupRepository.findLatestInteractionTypes(userId, seeds);
        }

        HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
        List<String> values = hashOps.multiGet(key, seeds);
        Map<String, InteractionType> signalsBySeed = new LinkedHashMap<>();
        for (int i = 0; i < seeds.size(); i++) {
            String value = values.get(i);
            if (value != null) {
                signalsBySeed.put(seeds.get(i), InteractionType.valueOf(value));
            }
        }
        return signalsBySeed;
    }

    // Step 5: gate on the Bloom filter's own existence first (a missing key must not be read as
    // "every bit is 0, so nothing is seen"), then resolve every candidate that needs confirming with
    // one batched query — never a per-candidate round trip. Filtering the already-sorted `ranked`
    // list in a single pass (rather than splitting into separate free/needs-confirming lists and
    // recombining) is what keeps the final order correct without any extra re-sorting.
    private List<RankedCandidate> filterSeenAndTruncate(String userId, List<RankedCandidate> ranked) {
        boolean bloomTrustworthy = bloomFilterReadService.exists(userId);

        List<String> needsConfirming = new ArrayList<>();
        for (RankedCandidate candidate : ranked) {
            boolean possiblySeen = !bloomTrustworthy || bloomFilterReadService.mightContain(userId, candidate.entityId());
            if (possiblySeen) {
                needsConfirming.add(candidate.entityId());
            }
        }

        Set<String> trulySeen = entityHistoryLookupRepository.findSeenEntityIds(userId, needsConfirming);

        return ranked.stream()
                .filter(candidate -> !trulySeen.contains(candidate.entityId()))
                .limit(topK)
                .toList();
    }

    // Step 6: batch-fetch title/category for just the survivors, then reassemble in the survivors'
    // score order — EntityLookupRepository doesn't guarantee its result rows match input order.
    private List<RecommendationItem> buildItems(List<RankedCandidate> survivors) {
        if (survivors.isEmpty()) {
            return List.of();
        }
        List<String> survivorIds = survivors.stream().map(RankedCandidate::entityId).toList();
        Map<String, EntitySummary> summariesById = entityLookupRepository.findByIds(survivorIds).stream()
                .collect(Collectors.toMap(EntitySummary::entityId, summary -> summary));

        List<RecommendationItem> items = new ArrayList<>();
        for (RankedCandidate candidate : survivors) {
            EntitySummary summary = summariesById.get(candidate.entityId());
            if (summary != null) {
                items.add(new RecommendationItem(candidate.entityId(), summary.title(), summary.category(), candidate.score()));
            }
        }
        return items;
    }

    private String lastEntitiesKey(String userId) {
        return "user:" + userId + ":lastEntities";
    }

    private String signalsKey(String userId) {
        return "user:" + userId + ":signals";
    }
}

// Responsibilities of each step
// Step 1 — Resolve Seeds

// Input: userId

// Gets the user's recent entities.

// Prefer Redis (lastEntities)
// If Redis lost data → PostgreSQL fallback

// Output:

// [A, B, C, D, E]
// Step 2 — Fetch Neighbors

// For every seed:

// A → neighbors
// B → neighbors
// C → neighbors

// Uses parallel HTTP requests to the vector-hasher service.

// Output:

// Map<Seed, List<Neighbor>>
// Step 3 — Resolve Signals

// Gets whether each seed was

// LIKE
// VIEW
// DISLIKE

// Again:

// Redis first
// PostgreSQL fallback

// Output:

// A -> LIKE
// B -> VIEW
// C -> DISLIKE
// Step 4 — Rank Candidates

// Hands both maps to CandidateRanker.

// It:

// merges
// deduplicates
// scores
// sorts

// Output:

// RankedCandidate[]
// Step 5 — Filter Seen

// Removes recommendations the user already saw.

// Uses:

// Bloom Filter
//        +
// Postgres confirmation

// Output:

// Only unseen candidates
// Step 6 — Build Response

// Currently we only have IDs.

// Example:

// M123
// M456
// M789

// This step fetches

// Title
// Category

// and builds

// RecommendationItem
