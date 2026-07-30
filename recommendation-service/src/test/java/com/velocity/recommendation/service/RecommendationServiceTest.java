package com.velocity.recommendation.service;

import com.velocity.recommendation.client.VectorHasherClient;
import com.velocity.recommendation.dto.InteractionType;
import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.dto.RecommendationItem;
import com.velocity.recommendation.dto.RecommendationResponse;
import com.velocity.recommendation.repository.EntityHistoryLookupRepository;
import com.velocity.recommendation.repository.EntityLookupRepository;
import com.velocity.recommendation.repository.EntityLookupRepository.EntitySummary;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.*;

class RecommendationServiceTest {

    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    private final ListOperations<String, String> listOps = mock(ListOperations.class);
    private final HashOperations<String, String, String> hashOps = mock(HashOperations.class);
    private final EntityHistoryLookupRepository entityHistoryLookupRepository = mock(EntityHistoryLookupRepository.class);
    private final VectorHasherClient vectorHasherClient = mock(VectorHasherClient.class);
    private final BloomFilterReadService bloomFilterReadService = mock(BloomFilterReadService.class);
    private final EntityLookupRepository entityLookupRepository = mock(EntityLookupRepository.class);

    private RecommendationService newService(int lastNSize, int topK) {
        when(redisTemplate.opsForList()).thenReturn(listOps);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        return new RecommendationService(redisTemplate, entityHistoryLookupRepository, vectorHasherClient,
                bloomFilterReadService, entityLookupRepository, lastNSize, topK);
    }

    @Test
    void coldStartUserWithNoHistoryAnywhereReturnsEmptyAndShortCircuits() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of());
        when(entityHistoryLookupRepository.findRecentEntityIds("U1", 5)).thenReturn(List.of());
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        assertThat(response).isEqualTo(new RecommendationResponse("U1", List.of()));
        verifyNoInteractions(vectorHasherClient, bloomFilterReadService, entityLookupRepository);
    }

    @Test
    void emptyRedisLastNFallsBackToPostgresRecentEntities() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of());
        when(entityHistoryLookupRepository.findRecentEntityIds("U1", 5)).thenReturn(List.of("SEED-A"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.of(List.of(new NeighborEntry("N1", 0.2))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(false);
        when(entityHistoryLookupRepository.findLatestInteractionTypes("U1", List.of("SEED-A"))).thenReturn(Map.of());
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N1")).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds("U1", List.of())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(List.of("N1")))
                .thenReturn(List.of(new EntitySummary("N1", "Title 1", "sports")));
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        verify(entityHistoryLookupRepository).findRecentEntityIds("U1", 5);
        assertThat(response.recommendations()).extracting(RecommendationItem::entityId).containsExactly("N1");
    }

    @Test
    void signalsKeyMissingFallsBackToPostgresInsteadOfHashOps() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.of(List.of(new NeighborEntry("N1", 0.5))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(false);
        when(entityHistoryLookupRepository.findLatestInteractionTypes("U1", List.of("SEED-A")))
                .thenReturn(Map.of("SEED-A", InteractionType.LIKE));
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N1")).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds("U1", List.of())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(List.of("N1")))
                .thenReturn(List.of(new EntitySummary("N1", "Title 1", "sports")));
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        verifyNoInteractions(hashOps);
        verify(entityHistoryLookupRepository).findLatestInteractionTypes("U1", List.of("SEED-A"));
        // LIKE scaling applied: distance 0.5 * 0.8 = 0.4 -> score = 1/1.4
        assertThat(response.recommendations().get(0).score()).isCloseTo(1.0 / 1.4, within(1e-9));
    }

    @Test
    void bloomFilterKeyMissingConfirmsEveryCandidateNotJustBloomPositives() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A"));
        when(vectorHasherClient.fetchNeighbors("SEED-A"))
                .thenReturn(Optional.of(List.of(new NeighborEntry("N1", 0.2), new NeighborEntry("N2", 0.3))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(true);
        when(hashOps.multiGet("user:U1:signals", List.of("SEED-A"))).thenReturn(java.util.Collections.singletonList(null));
        when(bloomFilterReadService.exists("U1")).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds(eq("U1"), any())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(any())).thenReturn(List.of(
                new EntitySummary("N1", "Title 1", "sports"), new EntitySummary("N2", "Title 2", "news")));
        RecommendationService service = newService(5, 10);

        service.getRecommendations("U1");

        verify(bloomFilterReadService, never()).mightContain(anyString(), anyString());
        verify(entityHistoryLookupRepository).findSeenEntityIds(eq("U1"), argThat(ids ->
                ids.containsAll(List.of("N1", "N2")) && ids.size() == 2));
    }

    @Test
    void aFourOhFourFromVectorHasherSkipsThatSeedWithoutFailingTheRequest() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A", "SEED-B"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.empty());
        when(vectorHasherClient.fetchNeighbors("SEED-B")).thenReturn(Optional.of(List.of(new NeighborEntry("N1", 0.3))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(false);
        when(entityHistoryLookupRepository.findLatestInteractionTypes(eq("U1"), any())).thenReturn(Map.of());
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N1")).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds("U1", List.of())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(List.of("N1")))
                .thenReturn(List.of(new EntitySummary("N1", "Title 1", "sports")));
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        assertThat(response.recommendations()).extracting(RecommendationItem::entityId).containsExactly("N1");
    }

    @Test
    void survivorsAreTruncatedToTopKAfterSeenFilteringAndKeepScoreOrder() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.of(List.of(
                new NeighborEntry("CLOSE", 0.1), new NeighborEntry("MID", 0.5), new NeighborEntry("FAR", 0.9))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(false);
        when(entityHistoryLookupRepository.findLatestInteractionTypes(eq("U1"), any())).thenReturn(Map.of());
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain(eq("U1"), anyString())).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds("U1", List.of())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(any())).thenReturn(List.of(
                new EntitySummary("CLOSE", "Close Title", "c"),
                new EntitySummary("MID", "Mid Title", "c")));
        RecommendationService service = newService(5, 2);

        RecommendationResponse response = service.getRecommendations("U1");

        assertThat(response.recommendations()).extracting(RecommendationItem::entityId)
                .containsExactly("CLOSE", "MID");
    }

    @Test
    void finalResponseOrderMatchesScoreOrderNotEntityLookupReturnOrder() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A", "SEED-B"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.of(List.of(
                new NeighborEntry("N1", 0.2), new NeighborEntry("N2", 0.5))));
        when(vectorHasherClient.fetchNeighbors("SEED-B")).thenReturn(Optional.of(List.of(new NeighborEntry("N3", 0.3))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(true);
        when(hashOps.multiGet("user:U1:signals", List.of("SEED-A", "SEED-B")))
                .thenReturn(List.of("LIKE", "CLICK"));
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N1")).thenReturn(false);
        when(bloomFilterReadService.mightContain("U1", "N2")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N3")).thenReturn(true);
        // N2 is truly seen (excluded); N3 is a Bloom false positive (kept).
        when(entityHistoryLookupRepository.findSeenEntityIds(eq("U1"), argThat(ids ->
                ids.containsAll(List.of("N2", "N3")) && ids.size() == 2)))
                .thenReturn(Set.of("N2"));
        // Deliberately returned in the opposite order of the expected score ranking.
        when(entityLookupRepository.findByIds(any())).thenReturn(List.of(
                new EntitySummary("N3", "Title 3", "news"),
                new EntitySummary("N1", "Title 1", "sports")));
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        // N1: dist 0.2, LIKE seed -> adjusted 0.16 -> score ~0.862
        // N3: dist 0.3, neutral (CLICK) seed -> score ~0.769
        // N2 excluded (truly seen).
        assertThat(response.recommendations()).extracting(RecommendationItem::entityId).containsExactly("N1", "N3");
    }
}
