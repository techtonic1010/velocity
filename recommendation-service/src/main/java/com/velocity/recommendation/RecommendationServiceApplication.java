package com.velocity.recommendation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RecommendationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(RecommendationServiceApplication.class, args);
    }
}

// redis fallback 
// if the redis is down, the signals can be read from Postgres 
// (entity_history.interaction_type) since they are already persisted there. 
// This ensures that the recommendation service can still function and provide 
// recommendations even if Redis is temporarily unavailable.

// entity_history.interaction_type already carries exactly this data, 
// written in the same batch as the Redis signal (per InteractionEventConsumer.consume()),
//  so nothing new needs to be built, just one more read path added to RecommendationService 
//  for the missing-signal case.

// redis fall back is not needed for the signals themselves, since they are already persisted in Postgres (entity_history.interaction_type) and can be read from there if Redis is down.
// signals computation 
