package com.velocity.recommendation.dto;

import java.util.List;

// Mirrors vector-hasher's GET /neighbors/{entityId} response body exactly.
public record NeighborsResponse(String entityId, List<NeighborEntry> neighbors) {
}
// used in the VectorHasherClient to deserialize the response from the 
// vector-hasher service when fetching neighbors for a given entityId.