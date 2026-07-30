package com.velocity.recommendation.service;

import com.velocity.recommendation.dto.InteractionType;
import com.velocity.recommendation.dto.NeighborEntry;

import java.sql.Time;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Merge
// ↓
// Remove duplicates
// ↓
// Apply signal weights
// ↓
// Compute scores
// ↓
// Sort
// ↓
// Return recommendations
// Pure logic, no I/O: merges every seed's neighbor list into one deduped, ranked candidate list.
// Mirrors UserTimelineBuilder's role in entity-interaction-service — the one piece of real
// algorithmic complexity here, kept free of Redis/Postgres/HTTP so it's cheap to test exhaustively.
public final class CandidateRanker {

    // Placeholder ranking constants (same spirit as Milestone 4's 10%/5% LIKE/DISLIKE simulation
    // ratio) — deliberate, tunable demo choices, not derived values. Logged in PROJECT_SPEC.md §10.
    private static final double LIKE_DISTANCE_MULTIPLIER = 0.8;
    private static final double DISLIKE_DISTANCE_MULTIPLIER = 1.5;

    private CandidateRanker() {
    }

    /**
     * Seed A

        A
        ├── X
        ├── Y
        └── Z
        A -> LIKE

        B -> VIEW

        C -> DISLIKE
     * @param neighborsBySeed vector-hasher's response per seed entityId (seeds that 404'd are
     *                        simply absent, not included with an empty list)
     * @param signalsBySeed   each seed's own interaction signal (Redis, or the entity_history
     *                        fallback if Redis's signals hash was missing) — never the candidate's
     *                        own signal, since a candidate with its own signal would always also be
     *                        Bloom/history-"seen" and excluded downstream anyway
     * @return candidates deduped by entityId (min distance wins, ties broken by first-seen),
     * scored, sorted descending by score (best first)
     */
    public static List<RankedCandidate> rankCandidates(
            Map<String, List<NeighborEntry>> neighborsBySeed,
            Map<String, InteractionType> signalsBySeed) {

        Map<String, RankedCandidate> bestByEntityId = new HashMap<>();
//         Time Complexity

// Suppose

// S = number of seed entities.
// Each seed returns K neighbors.

// Then:

// Scanning & deduplicating: O(S × K) (one pass through all neighbors, with HashMap lookups).
// Sorting: If there are N unique candidates, O(N log N).

// Overall:

// O(S × K + N log N)

// Memory usage is O(N) for the bestByEntityId map.
        for (Map.Entry<String, List<NeighborEntry>> seedEntry : neighborsBySeed.entrySet()) {
            String seedId = seedEntry.getKey();
            for (NeighborEntry neighbor : seedEntry.getValue()) {
                RankedCandidate current = bestByEntityId.get(neighbor.entityId());
                // "Keep min distance" is decided on the raw distance, before any signal scaling —
                // the scaling reflects the winning seed's signal, it doesn't influence which seed wins.
                if (current == null || neighbor.distance() < current.distance()) {
                    double adjustedDistance = scale(neighbor.distance(), signalsBySeed.get(seedId));
                    double score = 1.0 / (1.0 + adjustedDistance);
                    bestByEntityId.put(neighbor.entityId(),
                            new RankedCandidate(neighbor.entityId(), seedId, neighbor.distance(), score));
                }
            }
        }

        return bestByEntityId.values().stream()
                .sorted(Comparator.comparingDouble(RankedCandidate::score).reversed())
                .toList();
    }

    private static double scale(double distance, InteractionType sourceSeedSignal) {
        if (sourceSeedSignal == InteractionType.LIKE) {
            return distance * LIKE_DISTANCE_MULTIPLIER;
        }
        if (sourceSeedSignal == InteractionType.DISLIKE) {
            return distance * DISLIKE_DISTANCE_MULTIPLIER;
        }
        return distance;
    }

    public record RankedCandidate(String entityId, String sourceSeedId, double distance, double score) {
    }
}
// Sure. In plain English, the loop does this:

// Take one seed entity (something the user previously interacted with).
// Look at all of its similar neighbors returned by the vector search.
// For each neighbor:
// Check if we've already seen this candidate.
// If we haven't, add it.
// If we have, keep whichever version has the smaller (better) vector distance.
// Use the seed's interaction type (LIKE, VIEW, or DISLIKE) to slightly adjust the candidate's distance.
// Convert that adjusted distance into a score (higher score = better recommendation).
// Store the candidate with its score.
// After all seeds have been processed, sort all unique candidates by score (highest first) and return the l