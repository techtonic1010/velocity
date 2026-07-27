package com.velocity.entityinteraction.controller;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.service.InteractionEventProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InteractionController {

    private final InteractionEventProducer producer;

    public InteractionController(InteractionEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/interactions")
    public ResponseEntity<Void> submit(@RequestBody InteractionEvent event) {
        producer.publish(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
