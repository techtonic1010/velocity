package com.velocity.entityupload.config;

import com.velocity.entityupload.client.EmbeddingCreatorClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.ClientHttpRequestFactories;
import org.springframework.boot.web.client.ClientHttpRequestFactorySettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class RestClientConfig {

    @Bean
    public RestClient embeddingCreatorRestClient(
            @Value("${embedding-creator.base-url}") String baseUrl) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.DEFAULTS
                .withConnectTimeout(Duration.ofSeconds(5))
                .withReadTimeout(Duration.ofSeconds(10));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactories.get(settings);

        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
    }

    @Bean
    public EmbeddingCreatorClient embeddingCreatorClient(RestClient embeddingCreatorRestClient) {
        return new EmbeddingCreatorClient(embeddingCreatorRestClient);
    }
}

// First, look at the constructor of EmbeddingCreatorClient

// It probably looks like this:

// public class EmbeddingCreatorClient {

//     private final RestClient restClient;

//     public EmbeddingCreatorClient(RestClient restClient) {
//         this.restClient = restClient;
//     }
// }

// Notice that EmbeddingCreatorClient needs a RestClient to work.

// Why?

// Because EmbeddingCreatorClient itself does not know how to send HTTP requests.

// It uses RestClient whenever you call:

// embeddingCreatorClient.embed(id, text);

// Internally, it does something like:

// restClient.post()
//           .uri("/embed")
//           .body(...)
//           .retrieve()
//           .body(...);

// So,

// EmbeddingCreatorClient
//         │
//         ▼
//    RestClient
//         │
//         ▼
// Python Embedding Service

// Without a RestClient, EmbeddingCreatorClient cannot make HTTP calls.
//////////////////////////////////////////////////////////////////////////////
// Bean 1: RestClient
// @Bean
// public RestClient embeddingCreatorRestClient(...) {
//     ...
// }

// This creates a configured HTTP client.

// Think of it as:

// RestClient
// --------------
// Base URL = http://embedding-creator:8000

// Connect Timeout = 5 sec

// Read Timeout = 10 sec

// This is the configuration.

// Why not inject RestClient directly into IngestionService???

// You could, but then IngestionService would have to know how to make HTTP calls.

// Instead, you keep responsibilities separate:

// IngestionService
//         │
//         ▼
// EmbeddingCreatorClient
//         │
//         ▼
// RestClient
//         │
//         ▼
// Python Service

// Now:

// IngestionService knows "I need an embedding."
// EmbeddingCreatorClient knows "How do I call the Python API?"
// RestClient knows "How do I send HTTP requests?"

// Each class has a single responsibility.