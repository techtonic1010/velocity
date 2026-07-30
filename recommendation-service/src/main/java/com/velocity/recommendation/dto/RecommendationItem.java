package com.velocity.recommendation.dto;

// One entry in GET /recommendations's response body — matches PROJECT_SPEC.md §5.5's shape.
public record RecommendationItem(String entityId, String title, String category, double score) {
}
