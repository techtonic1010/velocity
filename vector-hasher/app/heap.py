import heapq

# vector-hasher/db.py/fetch_candidates_with_vectors gives the candidates with their vectors , 
# so that we can compute the distance between the new entity 
# and each candidate and build a heap of the closest candidates.

# INPUT — called from neighbor_index.py, in two different loops:
#   1) building a new entity's own list: heap starts as [], entry's distance
#      comes from lsh.cosine_distance(target_vector, candidate_vector).
#   2) the reverse eviction check on an existing candidate: heap is rebuilt
#      from db.fetch_neighbors_write(dsn, candidate_id)'s existing list,
#      entry is (same distance, new_entity_id).
#   k comes from the NEIGHBOR_INDEX_K config value either way.
# OUTPUT — the True/False return tells neighbor_index.py whether to bother
# calling db.save_neighbors (+ kafka_client.send_neighbor_update) for this
# entity at all. False means nothing changed — do nothing further.
def push_with_eviction(
    heap: list[tuple[float, str]], entry: tuple[float, str], k: int
) -> bool:
    distance, entity_id = entry
# //Extract the distance and entity ID from the incoming candidate

    # An entity already sitting in the heap (e.g. reloaded via a rebuild from
    # its persisted list) must not be treated as a new candidate — distance
    # is deterministic for a given pair, so a repeat push is never a genuine
    # update, only a duplicate waiting to happen.
    if any(existing_entity_id == entity_id for _, existing_entity_id in heap):
        return False

    if len(heap) < k:
        heapq.heappush(heap, (-distance, entity_id))
        return True

    farthest_distance = -heap[0][0]
    if distance < farthest_distance:
        heapq.heapreplace(heap, (-distance, entity_id))
        return True

    return False


# INPUT — called from neighbor_index.py, once per entity whose heap actually
# changed (i.e. at least one push_with_eviction call on it returned True).
# OUTPUT — the closest-first list gets handed to db.save_neighbors(...) /
# db.save_neighbors_read(...) (write-side, read-side) and to
# kafka_client.send_neighbor_update(...) — this is the final shape that
# ends up stored and published, nothing further transforms it.
def flatten_sorted(heap: list[tuple[float, str]]) -> list[tuple[float, str]]:
    return sorted((-neg_distance, entity_id) for neg_distance, entity_id in heap)


# pick 10 , build heap , checke if we can insert the nex entity ? 

# Here's the full sequence for new entity 18 arriving:

# Find candidates — LSH gives you entity 18's nearby bucket-mates, say [10, 16, 121].
# Build entity 18's own heap — compute distance(18, each candidate), build a fresh heap for 18's own brand-new list. This one's simple since 18 has no prior list.
# For each candidate separately, do the reverse check — this is the part worth being precise about:
# Fetch entity 10's existing list from Postgres → rebuild its own heap → check if 18 should evict 10's current farthest → if yes, write it back.
# Fetch entity 16's existing list from Postgres → rebuild its own separate heap → same check → maybe write back, maybe not.
# Fetch entity 121's list → same thing again.

# So it's not one heap serving the whole operation — it's N+1 heaps,
# one per entity touched (the new entity plus each candidate), 
# each built fresh, each checked and discarded independently.