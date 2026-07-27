from kafka import KafkaProducer

from app import db, heap, kafka_client, lsh

# ============================================================
# index_entity(entity_id) — FLOW MAP, both halves
# ============================================================
#
# INPUT: entity_id (the entity being indexed — brand new, or re-indexed)
#
#   SETUP (runs once, before either half)
#   ┌─────────────────────────────────────────────────────┐
#   │ db.fetch_shard_id(entity_id)      → shard_id         │
#   │ db.fetch_vector(entity_id)        → vector           │
#   │ db.fetch_candidates_with_vectors  → same_bucket       │
#   │ lsh.resolve_candidate_shard_ids   → shard_ids to use  │
#   │   (re-fetch with fetch_candidates_with_vectors if     │
#   │    the widened list differs from [shard_id])          │
#   │ → candidates: [(candidate_id, candidate_vector), ...] │
#   └─────────────────────────────────────────────────────┘
#                             │
#            ┌────────────────┴────────────────┐
#            ▼              
#                     ▼
#   HALF 1 — entity's OWN list          HALF 2 — reverse check
#   (always runs, always saves)         (per candidate, may do nothing)
#   ┌─────────────────────────┐         ┌──────────────────────────────┐
#   │ for each candidate:      │         │ for each candidate:           │
#   │  lsh.cosine_distance     │         │  db.fetch_neighbors_write     │
#   │   (vector, cand_vector)  │         │   (candidate_id)              │
#   │   → distance             │         │   → existing (candidate's     │
#   │  heap.push_with_eviction │         │      CURRENT saved list)      │
#   │   (own_heap,             │         │  _rebuild_heap(existing, k)   │
#   │    (distance, cand_id))  │         │   → candidate_heap            │
#   │                          │         │  heap.push_with_eviction      │
#   │ heap.flatten_sorted      │         │   (candidate_heap,            │
#   │  (own_heap)              │         │    (distance, entity_id))     │
#   │  → own_neighbors         │         │   → True/False                │
#   │                          │         │                                │
#   │ db.save_neighbors(...)   │         │  False → STOP, nothing saved  │
#   │ kafka_client.            │         │  for this candidate            │
#   │  send_neighbor_update    │         │                                │
#   │  (..., entity_id, ...)   │         │  True → db.fetch_shard_id      │
#   │                          │         │   (candidate_id)               │
#   │ ALWAYS runs — this list  │         │   heap.flatten_sorted(...)     │
#   │ is brand new             │         │   db.save_neighbors(...)       │
#   │                          │         │   kafka_client.                │
#   │                          │         │    send_neighbor_update        │
#   │                          │         │    (..., candidate_id, ...)    │
#   └───────────────────────── ┘          └──────────────────────────────┘
# 
#   Note: distance is computed ONCE in Half 1 and reused in Half 2 for the
#   same candidate — cosine distance is symmetric, so distance(A, B) ==
#   distance(B, A). No need to call cosine_distance a second time.
#
# OUTPUT: changed_entity_ids — entity_id (always) + any candidate_ids whose
# list actually got evicted-into (only the ones where push_with_eviction
# returned True in Half 2).
# ============================================================


def index_entity(
    dsn: str,
    producer: KafkaProducer,
    entity_id: str,
    k: int,
    num_shards: int,
    min_candidates: int,
    topic: str,
) -> list[str]:
    shard_id = db.fetch_shard_id(dsn, entity_id)
    vector = db.fetch_vector(dsn, entity_id)
    if shard_id is None or vector is None:
        raise ValueError(f"Unknown entityId: {entity_id}")

    # Same-bucket-first, widen-if-too-few — same fallback /candidates uses.
    same_bucket = db.fetch_candidates_with_vectors(dsn, [shard_id], entity_id)
    shard_ids = lsh.resolve_candidate_shard_ids(
        shard_id, len(same_bucket), num_shards, min_candidates
    )
    candidates = (
        same_bucket
        if shard_ids == [shard_id]
        else db.fetch_candidates_with_vectors(dsn, shard_ids, entity_id)
    )

    changed_entity_ids: list[str] = []

    # --- Half 1: build the new entity's own list. ---
    # cosine_distance's output feeds straight into push_with_eviction; the
    # same distance value gets reused below in the reverse check (distance
    # is symmetric, so no need to compute it twice per candidate).
    own_heap: list[tuple[float, str]] = []
    distances: dict[str, float] = {}

    for candidate_id, candidate_vector in candidates:
        distance = lsh.cosine_distance(vector, candidate_vector)
        distances[candidate_id] = distance
        heap.push_with_eviction(own_heap, (distance, candidate_id), k)

    # Always save + publish — this list is brand new, so it always changed.
    own_neighbors = heap.flatten_sorted(own_heap)
    db.save_neighbors(dsn, entity_id, shard_id, own_neighbors)
    kafka_client.send_neighbor_update(producer, topic, entity_id, _to_json(own_neighbors))
    changed_entity_ids.append(entity_id)

# without it, only the brand-new entity would ever get an updated list
# — existing entities would never find out a closer neighbor just showed up.

    # --- Half 2: reverse check — does the new entity now belong in each
    # candidate's own existing list? ---
    
    for candidate_id, _ in candidates:
        existing = db.fetch_neighbors_write(dsn, candidate_id)
        candidate_heap = _rebuild_heap(existing, k)

        changed = heap.push_with_eviction(
            candidate_heap, (distances[candidate_id], entity_id), k
        )
        if not changed:
            continue

        candidate_shard_id = db.fetch_shard_id(dsn, candidate_id)
        candidate_neighbors = heap.flatten_sorted(candidate_heap)
        db.save_neighbors(dsn, candidate_id, candidate_shard_id, candidate_neighbors)
        kafka_client.send_neighbor_update(
            producer, topic, candidate_id, _to_json(candidate_neighbors)
        )
        changed_entity_ids.append(candidate_id)

    return changed_entity_ids


# Reloads an already-persisted (already-within-k) list back into heap form.
# No eviction actually happens here — push_with_eviction is just reused as
# the insertion primitive, since heap.py doesn't expose a separate "load
# without eviction" function.
def _rebuild_heap(existing: list[dict[str, object]] | None, k: int) -> list[tuple[float, str]]:
    rebuilt: list[tuple[float, str]] = []
    if existing is None:
        return rebuilt
    for item in existing:
        heap.push_with_eviction(rebuilt, (item["distance"], item["entityId"]), k)
    return rebuilt


# flatten_sorted's tuples -> the {"entityId", "distance"} shape
# kafka_client.send_neighbor_update (and, on the consumer side,
# db.save_neighbors_read) expect on the wire.
def _to_json(neighbors: list[tuple[float, str]]) -> list[dict[str, object]]:
    return [{"entityId": entity_id, "distance": distance} for distance, entity_id in neighbors]
