package com.velocity.entityupload.controller;

import com.velocity.entityupload.service.IngestionService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IngestController {

    private final IngestionService ingestionService;

    public IngestController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/ingest")
    public IngestionService.IngestionResult ingest(
            @RequestParam(required = false) Integer limit) {
        return ingestionService.ingest(limit);
    }
}

// What is IngestionResult?

// Inside IngestionService

// public record IngestionResult(
//     int total,
//     int succeeded,
//     int failed,
//     List<String> failedIds
// ) {}

// This is just a normal Java object.

// Their job is not to do business logic. They simply:

// Accept the HTTP request.
// Extract input (query params, path params, request body).
// Call a service.
// Return whatever the service gives back.