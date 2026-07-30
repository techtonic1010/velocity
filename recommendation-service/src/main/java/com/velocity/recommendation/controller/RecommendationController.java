package com.velocity.recommendation.controller;

import com.velocity.recommendation.dto.RecommendationResponse;
import com.velocity.recommendation.service.RecommendationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RecommendationController {

    private final RecommendationService recommendationService;

    public RecommendationController(RecommendationService recommendationService) {
        this.recommendationService = recommendationService;
    }

    @GetMapping("/recommendations")
    public RecommendationResponse recommend(@RequestParam String userId) {
        return recommendationService.getRecommendations(userId);
    }
}
