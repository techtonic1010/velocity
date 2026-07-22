package com.velocity.entityupload.repository;

import com.velocity.entityupload.model.NewsArticle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

// @Repository + constructor-injected JdbcTemplate
@Repository
public class EntityRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO entities (entity_id, title, abstract, category, subcategory, vector)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (entity_id) DO UPDATE SET
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                category = EXCLUDED.category,
                subcategory = EXCLUDED.subcategory,
                vector = EXCLUDED.vector
            """;

    private final JdbcTemplate jdbcTemplate;

    public EntityRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

//     With batching:

// Java
//    │
// 100 rows
//    │
//    ▼
// Database
    public void upsertBatch(List<EntityRow> rows) {
        jdbcTemplate.batchUpdate(UPSERT_SQL, rows, rows.size(), (ps, row) -> {
            ps.setString(1, row.article().newsId());
            ps.setString(2, row.article().title());
            ps.setString(3, row.article().abstractText());
            ps.setString(4, row.article().category());
            ps.setString(5, row.article().subcategory());
            ps.setBytes(6, row.vectorBytes());
        });
    }

    /**
     * Packs a float vector as little-endian float32 bytes — the contract Milestone 2's
     * Python LSH hasher reads back with {@code np.frombuffer(raw, dtype='<f4')}.
     */
    public static byte[] packVector(List<Double> vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.size() * Float.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        for (Double value : vector) {
            buffer.putFloat(value.floatValue());
        }
        return buffer.array();
    }

    public record EntityRow(NewsArticle article, byte[] vectorBytes) {
    }
}

// 1. Why JdbcTemplate instead of JPA?

// Because this class has only one SQL query.

// INSERT ... ON CONFLICT DO UPDATE

// JdbcTemplate lets you execute that query directly:

// jdbcTemplate.batchUpdate(sql, ...)

// With JPA, you'd need:

// @Entity
// @Id
// JpaRepository
// understand save()/merge()

// That's a lot of setup just to run one SQL statement.

// Rule: If you already know the exact SQL, use JdbcTemplate.