from html import entities

import numpy as np
import psycopg
from psycopg.types.json import Jsonb

# to get all the vectors from the database , to compute the shard ids 
# for each vector and then update the shard ids in the database.
def fetch_all_vectors(dsn: str) -> list[tuple[str, np.ndarray]]:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT entity_id, vector FROM entities")
            rows = cur.fetchall()
    return [ (entity_id, np.frombuffer(raw, dtype="<f4")) for entity_id, raw in rows]

# assingning shardIDs to each vectoe in the database , and add it in database . 
def update_shard_ids(dsn: str, assignments: list[tuple[str, int]]) -> None:
    if not assignments:
        return

    values_clause = ", ".join(["(%s::varchar, %s::integer)"] * len(assignments))
    params = [value for pair in assignments for value in pair]
    query = f"""
        UPDATE entities AS e
        SET shard_id = data.shard_id
        FROM (VALUES {values_clause}) AS data(entity_id, shard_id)
        WHERE e.entity_id = data.entity_id
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)

# // Fetch the shard ID for a given entity.
def fetch_shard_id(dsn: str, entity_id: str) -> int | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT shard_id FROM entities WHERE entity_id = %s", (entity_id,)
            )
            row = cur.fetchone()
    return row[0] if row else None

# // // Fetch the embedding vector for a given entity.
def fetch_vector(dsn: str, entity_id: str) -> np.ndarray | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT vector FROM entities WHERE entity_id = %s", (entity_id,)
            )
            row = cur.fetchone()
    if row is None:
        return None
    return np.frombuffer(row[0], dtype="<f4")

# /// get the similar vectors from the same shard and adjacent shards , excluding the current entity id.
def fetch_candidates(
    dsn: str, shard_ids: list[int], exclude_entity_id: str
) -> list[dict[str, str]]:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, title FROM entities
                WHERE shard_id = ANY(%s) AND entity_id != %s
                """,
                (shard_ids, exclude_entity_id),
            )
            rows = cur.fetchall()
    return [{"entityId": entity_id, "title": title} for entity_id, title in rows]

# ///////////returns the vectors , from the same shard and adjacent shards , excluding the current entity id.
def fetch_candidates_with_vectors(
    dsn: str, shard_ids: list[int], exclude_entity_id: str
) -> list[tuple[str, np.ndarray]]:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, vector FROM entities
                WHERE shard_id = ANY(%s) AND entity_id != %s
                """,
                (shard_ids, exclude_entity_id),
            )
            rows = cur.fetchall()
    return [(entity_id, np.frombuffer(raw, dtype="<f4")) for entity_id, raw in rows]

# Retrieve the precomputed neighbors for a given entity from the database.
def fetch_neighbors_write(dsn: str, entity_id: str) -> list[dict[str, object]] | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT neighbors FROM neighbor_index_write WHERE entity_id = %s",
                (entity_id,),
            )
            row = cur.fetchone()
    return row[0] if row else None
# OUTPUT:
# row = (
#     [
#         {"entityId":"B2","distance":0.12},
#         {"entityId":"C9","distance":0.21}
#     ],
# )

# Save the computed neighbors of one entity into PostgreSQL.
# neighbors = [
#     (0.15, "A21"),
#     (0.27, "B44"),
#     (0.42, "C90")
# ]
def save_neighbors(
    dsn: str, entity_id: str, shard_id: int, neighbors: list[tuple[float, str]]
) -> None:
    neighbors_json = [
        {"entityId": neighbor_id, "distance": distance}
        for distance, neighbor_id in neighbors
    ]
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO neighbor_index_write (entity_id, shard_id, neighbors)
                VALUES (%s, %s, %s)
                ON CONFLICT (entity_id) DO UPDATE
                SET shard_id = EXCLUDED.shard_id,
                    neighbors = EXCLUDED.neighbors,
                    updated_at = now()
                """,
                (entity_id, shard_id, Jsonb(neighbors_json)),
            )


# Called by the Kafka consumer only. Unlike save_neighbors, the neighbors
# list here already arrived as JSON off the Kafka message — it's already
# shaped as [{"entityId": ..., "distance": ...}, ...], so no tuple-to-dict
# conversion is needed here, just wrap and store as-is.
def save_neighbors_read(
    dsn: str, entity_id: str, neighbors: list[dict[str, object]]
) -> None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO neighbor_index_read (entity_id, neighbors)
                VALUES (%s, %s)
                ON CONFLICT (entity_id) DO UPDATE
                SET neighbors = EXCLUDED.neighbors,
                    updated_at = now()
                """,
                (entity_id, Jsonb(neighbors)),
            )


def fetch_neighbors_read(dsn: str, entity_id: str) -> list[dict[str, object]] | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT neighbors FROM neighbor_index_read WHERE entity_id = %s",
                (entity_id,),
            )
            row = cur.fetchone()
    return row[0] if row else None

# ### `fetch_all_vectors()`

# Reads every `entity_id` and embedding vector from the `entities` table.  
# `np.frombuffer(raw, dtype="<f4")` directly converts the stored `BYTEA` bytes into a 384-dimensional `float32` NumPy array. It is the Python counterpart of Java's `EntityRepository.packVector()`.

# ---

# ### `update_shard_ids()`

# Bulk-updates the computed `shard_id` values in a **single SQL query** using `UPDATE ... FROM (VALUES ...)`, avoiding one `UPDATE` per row. Explicit `::varchar` and `::integer` casts ensure PostgreSQL correctly infers parameter types.

# ---

# ### `fetch_shard_id()`

# Fetches the current `shard_id` for a given `entity_id`. Used to determine which shard(s) should be searched for recommendations.

# ---

# ### `fetch_candidates()`

# Retrieves articles whose `shard_id` belongs to the given list (e.g., current and adjacent shards), excluding the current article. `psycopg` automatically converts the Python `list[int]` into a PostgreSQL array for the `ANY(...)` clause.

# ============================================================
# HOW "SIMILAR VECTORS" ACTUALLY GET FOUND — FLOW MAP
# ============================================================
#
#   Milestone 2 (already done)
#   ┌───────────────────────────────────────────────┐
#   │ Every entity already has a shard_id saved.     │
#   │ Similar vectors USUALLY (not always) share     │
#   │ the same shard_id. Not guaranteed — see        │
#   │ resolve_candidate_shard_ids's adjacent-shard    │
#   │ fallback in lsh.py for the "not always" case.  │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 1 — Coarse filter (cheap, plain SQL, no math yet)
#   ┌───────────────────────────────────────────────┐
#   │ fetch_candidates_with_vectors(shard_ids=[7])   │
#   │   SELECT entity_id, vector FROM entities       │
#   │   WHERE shard_id = ANY([7]) AND id != '18'     │
#   │                                                 │
#   │ Returns a GROUP, not "the similar ones" yet:   │
#   │   [("10", raw_bytes), ("16", raw_bytes),       │
#   │    ("121", raw_bytes)]                         │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 2 — Decode raw bytes back into real numbers
#   ┌───────────────────────────────────────────────┐
#   │ np.frombuffer(raw, dtype="<f4")                │
#   │ raw_bytes  →  array([0.023, -0.118, ...])      │
#   │ (already done automatically inside Step 1)     │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 3 — Get the ONE target vector to compare against
#   ┌───────────────────────────────────────────────┐
#   │ fetch_vector(dsn, "18")  →  entity 18's own    │
#   │ vector, decoded the same way as Step 2.        │
#   │ (this is the function still left to write)     │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 4 — NOW compute real distance, one pair at a time
#            (the expensive step — but only across the SMALL
#            group Step 1 already narrowed things down to)
#   ┌───────────────────────────────────────────────┐
#   │ for each (id, vec) in candidates:              │
#   │     distance = cosine_distance(target, vec)    │
#   │                                                 │
#   │ ("10", 0.15)   ("16", 0.42)   ("121", 0.08)    │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 5 — Sort ascending, keep the closest K
#   ┌───────────────────────────────────────────────┐
#   │ sorted by distance → [("121", 0.08),           │
#   │                        ("10", 0.15),           │
#   │                        ("16", 0.42)]           │
#   │                                                 │
#   │ THIS is what "similar vectors" means here —    │
#   │ a result you CALCULATE, never a property the   │
#   │ database already knew before you asked.        │
#   └───────────────────────────────────────────────┘
#
#   THE TWO-TIER DESIGN, in one line:
#   shard_id filter  = cheap & coarse   (a plain SQL WHERE clause)
#   cosine_distance  = expensive & precise (run ONLY on the small
#                      filtered group from Step 1 — never the whole table)
# ============================================================