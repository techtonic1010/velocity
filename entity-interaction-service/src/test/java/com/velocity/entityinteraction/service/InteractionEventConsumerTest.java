package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.repository.EntityHistoryRepository;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.*;

class InteractionEventConsumerTest {

    private static InteractionEvent event(String userId, String entityId, InteractionType type, String sourceId) {
        return new InteractionEvent(userId, entityId, type, Instant.parse("2019-11-13T08:36:57Z"), sourceId);
    }

    @Test
    void eachEventUpdatesAllThreeRedisServicesThenTheBatchIsWrittenOnceToPostgres() {
        LastNCacheService lastNCacheService = mock(LastNCacheService.class);
        BloomFilterService bloomFilterService = mock(BloomFilterService.class);
        SignalsService signalsService = mock(SignalsService.class);
        EntityHistoryRepository entityHistoryRepository = mock(EntityHistoryRepository.class);
        InteractionEventConsumer consumer = new InteractionEventConsumer(
                lastNCacheService, bloomFilterService, signalsService, entityHistoryRepository, 8);

        InteractionEvent first = event("U1", "N1", InteractionType.CLICK, "TRAIN-1");
        InteractionEvent second = event("U1", "N2", InteractionType.LIKE, "TRAIN-2");
        List<InteractionEvent> batch = List.of(first, second);

        consumer.consume(batch);

        verify(lastNCacheService).recordClick("U1", "N1");
        verify(lastNCacheService).recordClick("U1", "N2");
        verify(bloomFilterService).add("U1", "N1");
        verify(bloomFilterService).add("U1", "N2");
        verify(signalsService).record("U1", "N1", InteractionType.CLICK);
        verify(signalsService).record("U1", "N2", InteractionType.LIKE);
        verify(entityHistoryRepository, times(1)).upsertBatch(batch, 8);
    }

    @Test
    void eventsAreProcessedInBatchOrderAndPostgresWriteHappensAfterAllRedisUpdates() {
        LastNCacheService lastNCacheService = mock(LastNCacheService.class);
        BloomFilterService bloomFilterService = mock(BloomFilterService.class);
        SignalsService signalsService = mock(SignalsService.class);
        EntityHistoryRepository entityHistoryRepository = mock(EntityHistoryRepository.class);
        InteractionEventConsumer consumer = new InteractionEventConsumer(
                lastNCacheService, bloomFilterService, signalsService, entityHistoryRepository, 8);

        InteractionEvent first = event("U1", "N1", InteractionType.CLICK, "TRAIN-1");
        InteractionEvent second = event("U2", "N2", InteractionType.CLICK, "TRAIN-2");
        List<InteractionEvent> batch = List.of(first, second);

        consumer.consume(batch);

        InOrder order = inOrder(lastNCacheService, bloomFilterService, signalsService, entityHistoryRepository);
        order.verify(lastNCacheService).recordClick("U1", "N1");
        order.verify(bloomFilterService).add("U1", "N1");
        order.verify(signalsService).record("U1", "N1", InteractionType.CLICK);
        order.verify(lastNCacheService).recordClick("U2", "N2");
        order.verify(bloomFilterService).add("U2", "N2");
        order.verify(signalsService).record("U2", "N2", InteractionType.CLICK);
        order.verify(entityHistoryRepository).upsertBatch(batch, 8);
    }

    @Test
    void emptyBatchStillCallsUpsertBatchOnceWithEmptyList() {
        LastNCacheService lastNCacheService = mock(LastNCacheService.class);
        BloomFilterService bloomFilterService = mock(BloomFilterService.class);
        SignalsService signalsService = mock(SignalsService.class);
        EntityHistoryRepository entityHistoryRepository = mock(EntityHistoryRepository.class);
        InteractionEventConsumer consumer = new InteractionEventConsumer(
                lastNCacheService, bloomFilterService, signalsService, entityHistoryRepository, 8);

        consumer.consume(List.of());

        verifyNoInteractions(lastNCacheService, bloomFilterService, signalsService);
        verify(entityHistoryRepository).upsertBatch(List.of(), 8);
    }
}
