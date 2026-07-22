-- Milestone 3: Neighbor Index write-side + read-side schema.
-- Auto-executed by Postgres on first boot (docker-entrypoint-initdb.d), see docker-compose.yml.
-- NOTE: only runs against a fresh postgres-data volume, same caveat as 001_schema.sql.

-- Write side: updated directly by vector-hasher whenever it indexes an entity
-- (builds/evicts its max-heap). Never read by the serving path directly.
CREATE TABLE IF NOT EXISTS neighbor_index_write (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    shard_id    INTEGER      NOT NULL,
    -- Flattened, closest-first: [{"entityId": "...", "distance": 0.42}, ...]
    neighbors   JSONB        NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- compute (search candidates → cosine distance → heap eviction) 
-- → save to neighbor_index_write (what you just described) → 
-- produce to Kafka → consumer picks it up → 
-- saves to neighbor_index_read (the separate table GET /neighbors and the cache actually read from) 
-- → cache invalidated for that entity.

-- So neighbor_index_write is the landing spot right after computation, 
-- and the only way data ever reaches neighbor_index_read is through Kafka 
-- — never a direct write. That separation is deliberate, 
-- matching the "write-side heap maintenance is never read directly by serving" 
-- idea from earlier.

-- Read side: only ever written by the Kafka consumer, overwriting the row
-- wholesale on every message (full list, not a diff — idempotent, safe to replay).
CREATE TABLE IF NOT EXISTS neighbor_index_read (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    neighbors   JSONB        NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
