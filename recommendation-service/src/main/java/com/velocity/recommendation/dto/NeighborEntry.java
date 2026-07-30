package com.velocity.recommendation.dto;

// Mirrors vector-hasher's GET /neighbors/{entityId} response shape: { neighbors: [{entityId, distance}, ...] }.
public record NeighborEntry(String entityId, double distance) {
}
