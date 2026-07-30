package com.velocity.recommendation.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
// This repository is not a Redis fallback.

// Its purpose is:

// Given a list of entityIds, fetch their metadata (title, category) from PostgreSQL.
// First SELECT-based repository in this codebase — every other repository so far (entity-upload
// -service's EntityRepository, entity-interaction-service's EntityHistoryRepository) is upsert-only.
@Repository
public class EntityLookupRepository {

    private static final String SELECT_SQL =
            "SELECT entity_id, title, category FROM entities WHERE entity_id = ANY(?)";

    private final JdbcTemplate jdbcTemplate;

    public EntityLookupRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<EntitySummary> findByIds(List<String> entityIds) {
        if (entityIds.isEmpty()) {
            return List.of();
        }
        return jdbcTemplate.query(SELECT_SQL,
                ps -> ps.setArray(1, ps.getConnection().createArrayOf("varchar", entityIds.toArray())),
                (rs, rowNum) -> new EntitySummary(
                        rs.getString("entity_id"), rs.getString("title"), rs.getString("category")));
    }

    public record EntitySummary(String entityId, String title, String category) {
    }
}
