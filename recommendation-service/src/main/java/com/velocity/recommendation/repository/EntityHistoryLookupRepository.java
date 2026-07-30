package com.velocity.recommendation.repository;

import com.velocity.recommendation.dto.InteractionType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
// PostgreSQL fallback for user interaction history when Redis data is missing.

// Read-only access to entity_history (owned/written by entity-interaction-service) — the durable
// fallback behind all three Redis-state-loss gaps found while designing this service: last-N
// (step 1), the Bloom filter (step 5), and the signals hash (step 4). One consistent shape: check
// Redis existence first, fall back to a query here, let Redis self-heal on the user's next real
// interaction. Every query here is batched over a small (<=15, capped by NEIGHBOR_INDEX_K) set of
// ids, never a per-candidate round trip.
@Repository
public class EntityHistoryLookupRepository {

    private final JdbcTemplate jdbcTemplate;

    public EntityHistoryLookupRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Step 5: resolves the Bloom-positive-confirm set AND the Bloom-missing "confirm everyone" set
    // with the same query — a candidate is "seen" if it has any row at all for this user, regardless
    // of how many source_ids/interaction_types it has across repeats.
    public Set<String> findSeenEntityIds(String userId, List<String> candidateIds) {
        if (candidateIds.isEmpty()) {
            return Set.of();
        }
        String sql = "SELECT DISTINCT entity_id FROM entity_history WHERE user_id = ? AND entity_id = ANY(?)";
        List<String> seen = jdbcTemplate.query(sql,
                ps -> {
                    ps.setString(1, userId);
                    ps.setArray(2, ps.getConnection().createArrayOf("varchar", candidateIds.toArray()));
                },
                (rs, rowNum) -> rs.getString("entity_id"));
        return new HashSet<>(seen);
    }

    // Step 1: Redis's lastEntities list, rebuilt from the durable record. entity_history can hold
    // more than one row per entity_id (History + real-click repeats are intentionally not collapsed,
    // per PROJECT_SPEC.md §10), so this resolves the latest timestamp per distinct entity_id first,
    // then takes the most-recent `limit` of those — matching Redis's ring buffer semantics (each
    // entityId appears at most once), not just "the last `limit` rows".
    public List<String> findRecentEntityIds(String userId, int limit) {
        String sql = """
                SELECT entity_id FROM (
                    SELECT DISTINCT ON (entity_id) entity_id, event_timestamp
                    FROM entity_history
                    WHERE user_id = ?
                    ORDER BY entity_id, event_timestamp DESC
                ) AS latest_per_entity
                ORDER BY event_timestamp DESC
                LIMIT ?
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("entity_id"), userId, limit);
    }

    // Step 4: the signals hash, rebuilt for just the (<=5) seed entityIds that need it. Same
    // "latest wins" resolution as findRecentEntityIds, for the same reason (repeats aren't collapsed
    // in entity_history the way they are in Redis).
    public Map<String, InteractionType> findLatestInteractionTypes(String userId, List<String> seedEntityIds) {
        if (seedEntityIds.isEmpty()) {
            return Map.of();
        }
        String sql = """
                SELECT DISTINCT ON (entity_id) entity_id, interaction_type
                FROM entity_history
                WHERE user_id = ? AND entity_id = ANY(?)
                ORDER BY entity_id, event_timestamp DESC
                """;
        Map<String, InteractionType> result = new HashMap<>();

        jdbcTemplate.query(sql,
                ps -> {
                    ps.setString(1, userId);
                    ps.setArray(2, ps.getConnection().createArrayOf("varchar", seedEntityIds.toArray()));
                },
                (RowCallbackHandler) rs ->
                        result.put(rs.getString("entity_id"), InteractionType.valueOf(rs.getString("interaction_type"))));
        return result;
    }
}
// The Bloom filter says: ===> The method runs:

// A → maybe seen              SELECT DISTINCT entity_id
// B → maybe seen                FROM entity_history
                    //   =====>      
// C → unseen                   WHERE user_id = ?
                            
// D → maybe seen               AND entity_id = ANY(?)
// where

// candidateIds = [A, B, D]
// Suppose the database contains:

// user1
// ------
// A
// D

// The query returns:

// [A, D]