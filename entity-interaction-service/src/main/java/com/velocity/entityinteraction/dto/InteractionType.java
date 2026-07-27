package com.velocity.entityinteraction.dto;

// Matches entity_history.interaction_type (VARCHAR(10)) in 003_entity_history_schema.sql.
// Jackson serializes/deserializes this as the plain enum name — e.g. "interactionType": "CLICK"
// in JSON — with zero extra annotations, and that same string is what lands in the DB column.
public enum InteractionType {
    CLICK,
    LIKE,
    DISLIKE
}
