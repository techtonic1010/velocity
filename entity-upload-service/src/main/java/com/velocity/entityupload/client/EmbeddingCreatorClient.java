package com.velocity.entityupload.client;

import com.velocity.entityupload.dto.EmbedRequest;
import com.velocity.entityupload.dto.EmbedResponse;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

// this is single class responsible for communicating with the embedding-creator service
public class EmbeddingCreatorClient {

    private static final int MAX_ATTEMPTS = 2;
    private static final long RETRY_BACKOFF_MILLIS = 500;

    private final RestClient restClient;

    public EmbeddingCreatorClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public EmbedResponse embed(String entityId, String text) {
        RestClientException lastError = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return restClient.post()
                        .uri("/embed")
                        .body(new EmbedRequest(entityId, text))
                        .retrieve()
                        .body(EmbedResponse.class);
            } catch (RestClientException e) {
                lastError = e;
                if (attempt < MAX_ATTEMPTS) {
                    sleepBeforeRetry();
                }
            }
        }
        throw lastError;
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying embed call", e);
        }
    }
}

// Why does EmbeddingCreatorClient exist?

// It is the single class responsible for communicating with the embedding-creator service.

// Instead of IngestionService doing this:

// restClient.post(...)

// it simply does:

// embeddingCreatorClient.embed(...)

// This keeps responsibilities separate.

// EmbeddingCreatorClient → Knows how to call the embedding service (HTTP request, response parsing, retries, error handling).
// IngestionService → Knows what workflow to execute (read 50k articles, batch them, call the client, save results).

// This follows the Single Responsibility Principle (SRP).

// Option 2: Spring creates the RestClient

// Instead:

// public class EmbeddingCreatorClient {

//     private final RestClient restClient;

//     public EmbeddingCreatorClient(RestClient restClient) {
//         this.restClient = restClient;
//     }
// }

// Notice:

// EmbeddingCreatorClient never creates a RestClient.

// It simply says:

// "Someone give me one."

// Who gives it?

// Spring.

// Somewhere else (RestClientConfig)
// @Configuration
// public class RestClientConfig {

//     @Bean
//     RestClient restClient() {
//         return RestClient.builder()
//                 .baseUrl("http://localhost:8000")
//                 .build();
//     }
// }

// Spring executes this once.

// It creates the object.

// Then it injects it into

// EmbeddingCreatorClient

// automatically.