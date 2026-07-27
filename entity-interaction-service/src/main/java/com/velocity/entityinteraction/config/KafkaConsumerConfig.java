package com.velocity.entityinteraction.config;

import com.velocity.entityinteraction.dto.InteractionEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

import javax.swing.Spring;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    // Every operation InteractionEventConsumer performs per batch (Redis ring-buffer/Bloom/signals
    // updates, the Postgres ON CONFLICT DO NOTHING upsert) is idempotent — see that class's own
    // comment — so there's no "poison message" risk here, only transient dependency outages
    // (Redis/Postgres briefly down). Retrying forever with a capped backoff is therefore safe and
    // correct; giving up early (Spring Kafka's out-of-the-box default) silently drops real data.
    private static final long RETRY_INITIAL_INTERVAL_MS = 1_000L;
    private static final double RETRY_MULTIPLIER = 2.0;
    private static final long RETRY_MAX_INTERVAL_MS = 30_000L;
    // Creates a ConsumerFactory, which acts as a blueprint for creating Kafka consumers.
    @Bean
    public ConsumerFactory<String, InteractionEvent> interactionEventConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
                // only from the specified package for security.
        JsonDeserializer<InteractionEvent> valueDeserializer = new JsonDeserializer<>(InteractionEvent.class);
        valueDeserializer.addTrustedPackages("com.velocity.entityinteraction.dto");

        Map<String, Object> configProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "entity-interaction-service",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(configProps, new StringDeserializer(), valueDeserializer);
    }

    // No maxElapsedTime is set, so this backs off exponentially up to RETRY_MAX_INTERVAL_MS and
    // then keeps retrying forever at that interval — it never gives up and never needs a
    // recoverer, since a recoverer only runs once retries are exhausted.
    static ExponentialBackOff interactionEventBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff(RETRY_INITIAL_INTERVAL_MS, RETRY_MULTIPLIER);
        backOff.setMaxInterval(RETRY_MAX_INTERVAL_MS);
        return backOff;
    }

    @Bean
    public DefaultErrorHandler interactionEventErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(interactionEventBackOff());
        // RetryListener has two failedDelivery overloads: a single-ConsumerRecord one (the only
        // abstract method, so the only one a lambda can implement) and a default no-op
        // ConsumerRecords (plural) one. batchFactory is a *batch* listener, so
        // ErrorHandlingUtils.retryBatch always calls the plural overload — a lambda here would
        // silently never log anything. Must override it explicitly.
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                // Never called for a batch listener; present only to satisfy the interface.
            }

            @Override
            public void failedDelivery(ConsumerRecords<?, ?> records, Exception ex, int deliveryAttempt) {
                log.warn("Retrying interaction-events batch of {} (attempt {}) after: {}",
                        records.count(), deliveryAttempt, ex.getMessage());
            }
        });
        return errorHandler;
    }

    // Batch listener: InteractionEventConsumer processes a whole poll's worth of records at once —
    // Redis updates happen per-event, the Postgres write happens once per batch.

    // ConcurrentKafkaListenerContainerFactory → A factory that creates listener containers.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InteractionEvent> batchFactory(
            ConsumerFactory<String, InteractionEvent> interactionEventConsumerFactory,
            DefaultErrorHandler interactionEventErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, InteractionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
         // Whenever you need a KafkaConsumer, create it using this ConsumerFactory
        factory.setConsumerFactory(interactionEventConsumerFactory);
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(interactionEventErrorHandler);
        // Spring calls it only once:

            // consume(
            //     [event1,
            //      event2,
            //      ...
            //      event100]
            // )

            // Much faster for bulk processing.
            // Instead of:

// 100 INSERT statements

// you can do:

// 1 bulk INSERT
        return factory;
    }
}
