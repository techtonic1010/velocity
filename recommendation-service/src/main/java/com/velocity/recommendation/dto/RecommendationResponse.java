package com.velocity.recommendation.dto;

import java.util.List;

// GET /recommendations's full response body — matches PROJECT_SPEC.md §5.5's shape.
public record RecommendationResponse(String userId, List<RecommendationItem> recommendations) {
}
