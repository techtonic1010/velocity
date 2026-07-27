package com.velocity.entityinteraction.controller;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HealthControllerTest {

    @Test
    void healthCheckReturnsAHealthyMessage() {
        assertThat(new HealthController().healthCheck()).isEqualTo("Entity Interaction Service is healthy!");
    }
}
