package com.velocity.recommendation.controller;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HealthControllerTest {

    @Test
    void healthCheckReturnsAHealthyMessage() {
        assertThat(new HealthController().healthCheck()).isEqualTo("Recommendation Service is healthy!");
    }
}
