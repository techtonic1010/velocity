package com.velocity.entityinteraction.config;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerConfigTest {

    @Test
    void backOffRetriesForeverWithExponentialDelayCappedAtThirtySeconds() {
        ExponentialBackOff backOff = KafkaConsumerConfig.interactionEventBackOff();

        assertThat(backOff.getInitialInterval()).isEqualTo(1_000L);
        assertThat(backOff.getMultiplier()).isEqualTo(2.0);
        assertThat(backOff.getMaxInterval()).isEqualTo(30_000L);
        // Neither maxElapsedTime nor maxAttempts is overridden, so both stay at
        // ExponentialBackOff's own unlimited defaults -> retries never exhaust.
        assertThat(backOff.getMaxElapsedTime()).isEqualTo(Long.MAX_VALUE);
        assertThat(backOff.getMaxAttempts()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void errorHandlerBeanIsBuiltFromThatBackOff() {
        DefaultErrorHandler errorHandler = new KafkaConsumerConfig().interactionEventErrorHandler();

        assertThat(errorHandler).isNotNull();
    }
}
