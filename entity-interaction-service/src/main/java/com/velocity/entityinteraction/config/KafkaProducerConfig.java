package com.velocity.entityinteraction.config;

import com.velocity.entityinteraction.dto.InteractionEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    // Spring Boot's default value serializer is StringSerializer, which can't handle a record —
    // JsonSerializer is what lets KafkaTemplate<String, InteractionEvent> actually serialize the payload.

//     String: The data type of the message key (usually an ID or partition key).
// InteractionEvent: The data type of the message value (your Java object/record containing the payload).
    @Bean
    public ProducerFactory<String, InteractionEvent> interactionEventProducerFactory(
        // / 1. Spring creates the Factory Bean
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                // Uses StringSerializer. In Kafka, message keys are used for partitioning and are usually simple strings (like a user ID or event ID).
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                // Uses Spring's JsonSerializer (from org.springframework.kafka.support.serializer). This is the key piece—it takes your InteractionEvent Java object/record and serializes it into a JSON byte array before sending it over the wire.
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, InteractionEvent> interactionEventKafkaTemplate(
            ProducerFactory<String, InteractionEvent> interactionEventProducerFactory) {
        return new KafkaTemplate<>(interactionEventProducerFactory);
    }
}
