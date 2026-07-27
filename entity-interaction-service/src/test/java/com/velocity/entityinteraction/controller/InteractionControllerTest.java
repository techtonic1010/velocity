package com.velocity.entityinteraction.controller;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.service.InteractionEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class InteractionControllerTest {

    @Test
    void submitPublishesTheEventAndReturns202Accepted() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        InteractionController controller = new InteractionController(producer);
        InteractionEvent event = new InteractionEvent(
                "U131", "N45", InteractionType.CLICK, Instant.parse("2019-11-13T08:36:57Z"), "TRAIN-1");

        ResponseEntity<Void> response = controller.submit(event);

        verify(producer).publish(event);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNull();
    }
}
