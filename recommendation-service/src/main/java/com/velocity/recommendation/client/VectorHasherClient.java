package com.velocity.recommendation.client;

import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.dto.NeighborsResponse;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Optional;
// The VectorHasherClient ultimately reads from the neighbor_index_read table (through the vector-hasher service).

// That table is your read-optimized, mostly static cache/table built specifically for fast neighbor lookups.

// Single class responsible for calling vector-hasher's GET /neighbors/{entityId} — mirrors
// entity-upload-service's EmbeddingCreatorClient (RestClient injected via RestClientConfig, not
// built here). Reuses vector-hasher's own read-side cache and DB access rather than this service
// querying neighbor_index_read directly (see MILESTONE_5_PLAN.md decision 1).
public class VectorHasherClient {

    private final RestClient restClient;

    public VectorHasherClient(RestClient restClient) {
        this.restClient = restClient;
    }

    // Empty (not an exception) on 404 — an entity not yet present in neighbor_index_read just means
    // "skip this seed," not a request failure. Any other non-2xx still propagates as an exception.
    public Optional<List<NeighborEntry>> fetchNeighbors(String entityId) {
        try {
            NeighborsResponse response = restClient.get()
                    .uri("/neighbors/{entityId}", entityId)
                    .retrieve()
                    .body(NeighborsResponse.class);
            return Optional.ofNullable(response).map(NeighborsResponse::neighbors);
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        }
    }
}
