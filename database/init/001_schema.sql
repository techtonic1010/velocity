-- Milestone 1: Entity DB schema.
-- Auto-executed by Postgres on first boot (docker-entrypoint-initdb.d), see docker-compose.yml.
-- NOTE: only runs against a fresh postgres-data volume. If this table is missing after a
-- previous `docker-compose up`, the volume already existed — run `docker-compose down -v`
-- and re-up, or apply this file manually.

CREATE TABLE IF NOT EXISTS entities (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    title       TEXT         NOT NULL,
    abstract    TEXT,
    category    VARCHAR(100),
    subcategory VARCHAR(100),
    -- 384 x 4-byte little-endian float32, packed, no header (numpy/ByteBuffer contract).
    vector      BYTEA        NOT NULL,
    -- Populated by Milestone 2's LSH hasher; unused (always 0) until then.
    shard_id    INTEGER      NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- entity_id	N12345
-- title	   49ers trade for Deshaun Watson in stunning offseason move
-- abstract	   San Francisco sends two first-round picks as part of a blockbuster deal that reshapes the NFC playoff picture.
-- category	   sports
-- subcategory	football_nfl
-- vector	   \x8a3f4c3e... (1,536 raw bytes — see below)
-- shard_id	    7
-- created_at	2026-07-20 14:32:11.402+00