package com.velocity.entityinteraction.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @Override
    public Health health() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            int nodeCount = adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS).size();
            return Health.up().withDetail("nodes", nodeCount).build();
        } catch (Exception e) {
            return Health.down(e).build();
        }
    }
}
// /home/parth-ratnaparkhi/Desktop/Velocity/entity-interaction-service/src/main/java/com/velocity/entityinteraction/config/KafkaHealthIndicator.java