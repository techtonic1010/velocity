package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.repository.EntityHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

// Single consumer doing both jobs: per-event Redis updates (idempotent, safe to repeat), then one
// batched Postgres write. If the batch write throws, Spring Kafka redelivers the whole batch —
// redoing the already-idempotent Redis updates on redelivery is harmless.
@Service
public class InteractionEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(InteractionEventConsumer.class);

    private final LastNCacheService lastNCacheService;
    private final BloomFilterService bloomFilterService;
    private final SignalsService signalsService;
    private final EntityHistoryRepository entityHistoryRepository;
    private final int redisShardCount;

    public InteractionEventConsumer(
            LastNCacheService lastNCacheService,
            BloomFilterService bloomFilterService,
            SignalsService signalsService,
            EntityHistoryRepository entityHistoryRepository,
            @Value("${redis.shard-count}") int redisShardCount) {
        this.lastNCacheService = lastNCacheService;
        this.bloomFilterService = bloomFilterService;
        this.signalsService = signalsService;
        this.entityHistoryRepository = entityHistoryRepository;
        this.redisShardCount = redisShardCount;
    }

    @KafkaListener(topics = "${interaction.kafka-topic}", containerFactory = "batchFactory")
    public void consume(List<InteractionEvent> batch) {
        // Iterate in received order — a Kafka partition preserves per-key (per-user) order, so
        // processing the batch in order keeps one user's events chronologically correct.
        for (InteractionEvent event : batch) {
            lastNCacheService.recordClick(event.userId(), event.entityId());
            bloomFilterService.add(event.userId(), event.entityId());
            signalsService.record(event.userId(), event.entityId(), event.interactionType());
        }
        entityHistoryRepository.upsertBatch(batch, redisShardCount);
        log.info("Consumed batch of {} interaction events", batch.size());
    }
}

// InteractionEventConsumer.consume(batch)
//         │
//         ├─ for each event in the batch, in order:
//         │       lastNCacheService.recordClick(userId, entityId)   → runs the Lua script
//         │       bloomFilterService.add(userId, entityId)          → 7x SETBIT
//         │       signalsService.record(userId, entityId, type)     → 1x HSET
//         │
//         └─ once, after the loop:
//                 entityHistoryRepository.upsertBatch(batch, redisShardCount)  → 1 JDBC batch INSERT

// Nothing calls InteractionEventConsumer directly in your code — 
// Spring Kafka's container (built from batchFactory) invokes .consume(...) 
// automatically whenever new messages show up on the topic, because of the
//  @KafkaListener annotation on that method.

