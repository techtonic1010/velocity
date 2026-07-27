package com.velocity.entityinteraction.dto;

import java.time.Instant;

// This is the message shape that goes onto the Kafka interaction-events topic, keyed by userId.
// It's also the exact JSON body POST /interactions expects — same record, no separate
// request DTO, since the wire shape and the Kafka payload are identical.
//
// sourceId is what Postgres's entity_history PRIMARY KEY (source_id, entity_id) relies on for
// idempotency, so it must be assigned per the replay's rules, not left to the caller's discretion:
//   - real click (from an impression row)  -> "{SPLIT}-{impressionId}"  e.g. "TRAIN-8821", "DEV-341"
//   - synthetic click (from History replay) -> "HIST-{userId}-{index}"  e.g. "HIST-U13740-0"
// Split-prefixing avoids train/dev impressionId collisions; per-item History indices keep genuine
// repeated History entries as distinct permanent rows instead of upserting over each other.
//
// Example:
// {
//   "userId": "U131",
//   "entityId": "N12345",
//   "interactionType": "CLICK",
//   "timestamp": "2019-11-13T08:36:57Z",
//   "sourceId": "TRAIN-8821"
// }
public record InteractionEvent(
        String userId,
        String entityId,
        InteractionType interactionType,
        Instant timestamp,
        String sourceId) {
}
// add the sample DTO class for the interaction event, 
// which will be used to send the event to Kafka.


