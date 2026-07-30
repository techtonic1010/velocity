package com.velocity.recommendation.dto;

// Duplicated from entity-interaction-service (no shared library between services) — must match
// entity_history.interaction_type (VARCHAR(10)) and Redis's user:{userId}:signals values exactly,
// since this service only ever reads values written by entity-interaction-service, never writes them.
public enum InteractionType {
    CLICK,
    LIKE,
    DISLIKE
}
