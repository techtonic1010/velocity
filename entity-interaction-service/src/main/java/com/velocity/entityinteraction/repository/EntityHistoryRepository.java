package com.velocity.entityinteraction.repository;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.util.ShardUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

@Repository
public class EntityHistoryRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO entity_history (source_id, entity_id, user_id, interaction_type, event_timestamp, shard_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (source_id, entity_id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public EntityHistoryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // entity_history is an PAST historical-fact table — a redelivered/ replayed identical event
    // has identical field values, so DO NOTHING (first write wins) is correct, not DO UPDATE.
    public void upsertBatch(List<InteractionEvent> events, int redisShardCount) {
        jdbcTemplate.batchUpdate(UPSERT_SQL, events, events.size(), (ps, event) -> {
            ps.setString(1, event.sourceId());
            ps.setString(2, event.entityId());
            ps.setString(3, event.userId());
            ps.setString(4, event.interactionType().name());
            ps.setTimestamp(5, Timestamp.from(event.timestamp()));
            ps.setInt(6, ShardUtil.shardFor(event.userId(), redisShardCount));
        });
    }
}
