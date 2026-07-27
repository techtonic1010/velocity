package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InteractionEventProducer {
// class fields (variables) are private to encapsulate the state of the class 
// and prevent external modification. This is a fundamental principle of
//  object-oriented programming that promotes data integrity and security.
    private final KafkaTemplate<String, InteractionEvent> kafkaTemplate;
    private final String topic;

//     KafkaTemplate is reusable across different topics

// Imagine your app later needs to publish UserRegistrationEvent or AuditLogEvent.
   
public InteractionEventProducer(
    // 1. Spring checks its memory: "Do I have a bean matching KafkaTemplate<String, InteractionEvent>?"
            KafkaTemplate<String, InteractionEvent> interactionEventKafkaTemplate,
            @Value("${interaction.kafka-topic}") String topic) {
        this.kafkaTemplate = interactionEventKafkaTemplate;
        this.topic = topic;
    }
// This automatic passing of dependencies from the configuration class into your service class is called Dependency Injection

    // Keyed by userId so Kafka preserves per-user ordering across the topic's partitions.
    public void publish(InteractionEvent event) {
        kafkaTemplate.send(topic, event.userId(), event);
    }
}
