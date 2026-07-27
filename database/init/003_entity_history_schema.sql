-- Milestone 4: Entity History schema (durable record of user clicks/likes/dislikes).
-- Auto-executed by Postgres on first boot (docker-entrypoint-initdb.d), see docker-compose.yml.
-- NOTE: only runs against a fresh postgres-data volume, same caveat as 001_schema.sql /
-- 002_neighbor_index_schema.sql — this project's Postgres has already run once, so this
-- file also needs to be applied by hand (psql) against the live database.

CREATE TABLE IF NOT EXISTS entity_history (
    source_id         VARCHAR(64)  NOT NULL,   -- real MIND impression ID (sitting clicks) or a
                                                 -- made-up marker (background-history clicks) —
                                                 -- identifies exactly which real record this came from
    entity_id         VARCHAR(32)  NOT NULL,
    user_id           VARCHAR(32)  NOT NULL,
    interaction_type  VARCHAR(10)  NOT NULL,   -- CLICK | LIKE | DISLIKE
    event_timestamp   TIMESTAMPTZ  NOT NULL,
    shard_id          INTEGER      NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- (source_id, entity_id) together identify one real event — replaying the same
    -- event twice (redelivery, re-run) upserts the existing row instead of duplicating it.
    PRIMARY KEY (source_id, entity_id)
);

-- PK (source_id, entity_id) only gives a fast path when source_id is known.
-- These cover the two other lookup patterns Milestone 5 needs: by article, and by time.
CREATE INDEX IF NOT EXISTS idx_entity_history_entity_id ON entity_history (entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_history_event_timestamp ON entity_history (event_timestamp);
