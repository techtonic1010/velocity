package com.velocity.recommendation.controller;

import com.velocity.recommendation.dto.RecommendationResponse;
import com.velocity.recommendation.service.RecommendationService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecommendationControllerTest {

    @Test
    void delegatesToRecommendationServiceWithTheGivenUserId() {
        RecommendationService recommendationService = mock(RecommendationService.class);
        RecommendationResponse expected = new RecommendationResponse("U131", List.of());
        when(recommendationService.getRecommendations("U131")).thenReturn(expected);
        RecommendationController controller = new RecommendationController(recommendationService);

        RecommendationResponse result = controller.recommend("U131");

        assertThat(result).isEqualTo(expected);
    }
}
