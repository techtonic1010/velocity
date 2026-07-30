package com.velocity.recommendation.service;

import com.velocity.recommendation.dto.InteractionType;
import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.service.CandidateRanker.RankedCandidate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class CandidateRankerTest {

    @Test
    void mergesNonOverlappingNeighborsFromMultipleSeeds() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of(
                "SEED-A", List.of(new NeighborEntry("N1", 0.2)),
                "SEED-B", List.of(new NeighborEntry("N2", 0.3)));

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());

        assertThat(result).extracting(RankedCandidate::entityId).containsExactlyInAnyOrder("N1", "N2");
    }

    @Test
    void dedupesByEntityIdKeepingTheMinDistanceAndItsProvenance() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of(
                "SEED-A", List.of(new NeighborEntry("N1", 0.5)),
                "SEED-B", List.of(new NeighborEntry("N1", 0.2)));

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());

        assertThat(result).hasSize(1);
        assertThat(result.get(0).entityId()).isEqualTo("N1");
        assertThat(result.get(0).distance()).isEqualTo(0.2);
        assertThat(result.get(0).sourceSeedId()).isEqualTo("SEED-B");
    }

    @Test
    void likeSignalOnTheSourceSeedBoostsTheScore() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of("SEED-A", List.of(new NeighborEntry("N1", 0.5)));

        List<RankedCandidate> neutral = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());
        List<RankedCandidate> liked = CandidateRanker.rankCandidates(
                neighborsBySeed, Map.of("SEED-A", InteractionType.LIKE));

        assertThat(liked.get(0).score()).isGreaterThan(neutral.get(0).score());
    }

    @Test
    void dislikeSignalOnTheSourceSeedPenalizesTheScore() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of("SEED-A", List.of(new NeighborEntry("N1", 0.5)));

        List<RankedCandidate> neutral = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());
        List<RankedCandidate> disliked = CandidateRanker.rankCandidates(
                neighborsBySeed, Map.of("SEED-A", InteractionType.DISLIKE));

        assertThat(disliked.get(0).score()).isLessThan(neutral.get(0).score());
    }

    @Test
    void clickSignalIsTreatedAsNeutral() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of("SEED-A", List.of(new NeighborEntry("N1", 0.5)));

        List<RankedCandidate> noSignal = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());
        List<RankedCandidate> clickSignal = CandidateRanker.rankCandidates(
                neighborsBySeed, Map.of("SEED-A", InteractionType.CLICK));

        assertThat(clickSignal.get(0).score()).isEqualTo(noSignal.get(0).score());
    }

    @Test
    void missingSignalEntryForASeedIsTreatedAsNeutral() {
        // SEED-A has no entry at all in signalsBySeed (e.g. it was never a real click, only History).
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of("SEED-A", List.of(new NeighborEntry("N1", 0.5)));

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, Map.of("SEED-B", InteractionType.LIKE));

        // score = 1 / (1 + 0.5) = 0.6666...
        assertThat(result.get(0).score()).isCloseTo(1.0 / 1.5, within(1e-9));
    }

    @Test
    void resultsAreSortedByScoreDescending() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of(
                "SEED-A", List.of(new NeighborEntry("FAR", 0.9), new NeighborEntry("CLOSE", 0.1)));

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());

        assertThat(result).extracting(RankedCandidate::entityId).containsExactly("CLOSE", "FAR");
    }

    @Test
    void emptyNeighborsProduceEmptyResult() {
        assertThat(CandidateRanker.rankCandidates(Map.of(), Map.of())).isEmpty();
    }

    @Test
    void minDistanceWinsProvenanceEvenWhenAFartherSeedWasLiked() {
        // Documented simplification: a candidate referenced by a closer neutral seed AND a farther
        // LIKE seed uses the closer/neutral seed's provenance — no boost applied in this case.
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of(
                "NEUTRAL-CLOSER", List.of(new NeighborEntry("N1", 0.2)),
                "LIKED-FARTHER", List.of(new NeighborEntry("N1", 0.6)));
        Map<String, InteractionType> signals = Map.of("LIKED-FARTHER", InteractionType.LIKE);

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, signals);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).sourceSeedId()).isEqualTo("NEUTRAL-CLOSER");
        // No LIKE boost applied, since the winning (closer) seed was neutral: score = 1 / (1 + 0.2).
        assertThat(result.get(0).score()).isCloseTo(1.0 / 1.2, within(1e-9));
    }
}
