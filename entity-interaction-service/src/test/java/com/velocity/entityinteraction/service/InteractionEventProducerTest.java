package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;

import static org.mockito.Mockito.*;

class InteractionEventProducerTest {

    @Test
    void publishSendsToConfiguredTopicKeyedByUserId() {
        KafkaTemplate<String, InteractionEvent> kafkaTemplate = mock(KafkaTemplate.class);
        InteractionEventProducer producer = new InteractionEventProducer(kafkaTemplate, "interaction-events");
        InteractionEvent event = new InteractionEvent(
                "U131", "N45", InteractionType.CLICK, Instant.parse("2019-11-13T08:36:57Z"), "TRAIN-1");

        producer.publish(event);

        verify(kafkaTemplate).send("interaction-events", "U131", event);
    }
}
