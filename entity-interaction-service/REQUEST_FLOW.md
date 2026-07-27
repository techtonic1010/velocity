# Entity Interaction Service — Request Flow

One HTTP endpoint (`POST /replay`) that ends up writing to two independent stores (Postgres, Redis), with Kafka as the hand-off point between the "produce" half and the "consume" half of the system. Those two halves know nothing about each other directly — only Kafka connects them.

## End-to-end flow

```
                              HTTP
                               │
                               ▼
                 ┌──────────────────────────┐
                 │     ReplayController        │   POST /replay?userId=... | ?limit=...
                 └──────────────┬─────────────┘
                                 │ userId / limit
                                 ▼
                 ┌──────────────────────────┐
                 │       ReplayService          │   reads both TSV files, groups rows by user
                 └──────────────┬─────────────┘
                                 │ List<BehaviorRow>   (all of one user's rows)
                                 ▼
                 ┌──────────────────────────┐
                 │    UserTimelineBuilder       │   sort → History events → real-click events
                 └──────────────┬─────────────┘
                                │ List<InteractionEvent>   (chronologically ordered)
                                ▼
                 ┌──────────────────────────┐
                 │  InteractionEventProducer    │   kafkaTemplate.send(topic, key=userId, event)
                 └──────────────┬─────────────┘
                                 │
                  ══════════════╪══════════════   Kafka topic "interaction-events"  (key = userId)
                                 │
                                 ▼
                 ┌──────────────────────────     ┐
                 │  InteractionEventConsumer     │   @KafkaListener, batch mode
                 └───┬───────┬───────┬────────┬─ ┘
       (per event) ┌─┘       │       │        └─┐ (once, after the loop over the whole batch)
                    ▼         ▼       ▼          ▼
           LastNCacheService  BloomFilterService  SignalsService   EntityHistoryRepository
                    │         │       │                            │
                    ▼         ▼       ▼                            ▼
                 Redis      Redis   Redis                       Postgres
             (lastEntities)(bloomfilter)(signals)             (entity_history)
```

## Classes: inputs, outputs, who calls whom

| # | Class | Trigger / input | Calls | Output |
|---|---|---|---|---|
| 1 | `ReplayController` | HTTP `POST /replay?userId=` or `?limit=` | `ReplayService.replayUser(userId)` or `.replayAll(limit)` | `ReplayResult` → JSON response body |
| 2 | `ReplayService` | `userId: String` or `limit: Integer` | `BehaviorsTsvParser.parse(...)`, `UserTimelineBuilder.build(...)`, `InteractionEventProducer.publish(...)` per event | `ReplayResult(usersReplayed, eventsPublished)` |
| 3 | `BehaviorsTsvParser` | raw TSV `Reader` + split label | — (pure parsing) | `Stream<BehaviorRow>` |
| 4 | `UserTimelineBuilder` | `userId`, `List<BehaviorRow>` | `MurmurHash3.hash32(...)` (LIKE/DISLIKE classification) | `List<InteractionEvent>`, ordered |
| 5 | `InteractionEventProducer` | one `InteractionEvent` | `KafkaTemplate.send(topic, userId, event)` | none — fire-and-forget |
| 6 | `InteractionEventConsumer` | Kafka delivers a batch on `interaction-events` | `LastNCacheService`, `BloomFilterService`, `SignalsService` per event; `EntityHistoryRepository.upsertBatch(...)` once per batch | none — side effects only |
| 7 | `LastNCacheService` | `userId, entityId` | one atomic Lua script (`LREM`→`LPUSH`→`LTRIM`) | new list length (unused) |
| 8 | `BloomFilterService` | `userId, entityId` | `MurmurHash3.hash32(...)` ×2, then 7× `SETBIT` | none |
| 9 | `SignalsService` | `userId, entityId, interactionType` | one `HSET` | none |
| 10 | `EntityHistoryRepository` | `List<InteractionEvent>`, `redisShardCount` | `ShardUtil.shardFor(...)` per row, one JDBC batch `INSERT ... ON CONFLICT DO NOTHING` | none |

## Data shapes that actually cross a boundary

**`InteractionEvent`** — the one shape that crosses every boundary in the system: it's the Kafka message value, what `UserTimelineBuilder` produces, and the row shape written to Postgres.
```
{ userId, entityId, interactionType: CLICK|LIKE|DISLIKE, timestamp, sourceId }
```

**`BehaviorRow`** — internal only, never crosses Kafka, never leaves the producer side.
```
{ impressionId, split: TRAIN|DEV, userId, time, historyEntityIds: [...], impressions: [{entityId, clicked}, ...] }
```

**`ReplayResult`** — the only shape that ever reaches the HTTP caller.
```
{ usersReplayed: int, eventsPublished: int }
```

## The one thing worth drawing explicitly

Everything left of Kafka (`ReplayController` → `InteractionEventProducer`) has zero knowledge of Postgres or Redis. Everything right of Kafka (`InteractionEventConsumer` → the four downstream services) has zero knowledge of TSV files or replay logic. Kafka is the only link — which is what makes the write side (Redis + Postgres) able to fall behind or restart without losing anything: every published event sits on the topic until a consumer is there to read it.
