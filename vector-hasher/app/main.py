import os
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import numpy as np
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel, ConfigDict, Field

from app import db, kafka_client, lsh, neighbor_index
from app.cache import LRUCache
# Reads the PostgreSQL connection string.
DATABASE_URL = os.environ["DATABASE_URL"]
LSH_SEED = int(os.environ.get("LSH_SEED", "42"))
LSH_NUM_HYPERPLANES = int(os.environ.get("LSH_NUM_HYPERPLANES", "20"))
NUM_SHARDS = int(os.environ.get("NUM_SHARDS", "8"))
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "384"))
MIN_CANDIDATES = int(os.environ.get("MIN_CANDIDATES", "5"))
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
NEIGHBOR_INDEX_K = int(os.environ.get("NEIGHBOR_INDEX_K", "3"))
KAFKA_NEIGHBOR_TOPIC = os.environ.get("KAFKA_NEIGHBOR_TOPIC", "neighbor-updates")
NEIGHBOR_CACHE_SIZE = int(os.environ.get("NEIGHBOR_CACHE_SIZE", "32"))

_hyperplanes: np.ndarray | None = None
_kafka_producer: KafkaProducer | None = None
_neighbor_cache: LRUCache | None = None

# On startup: Generates the hyperplane matrix (fixed random vectors used for all hashing operations),
# creates the Kafka producer, creates the neighbor cache, and starts the Kafka
# consumer loop on a background thread (daemon=True — it never returns on its
# own, so it must not block process shutdown).
# On shutdown: Cleans up the global references.
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
#     When FastAPI starts, it generates the 20 random hyperplanes.
# These remain fixed throughout the lifetime of the application.
    global _hyperplanes, _kafka_producer, _neighbor_cache
    _hyperplanes = lsh.generate_hyperplanes(LSH_NUM_HYPERPLANES, EMBEDDING_DIM, LSH_SEED)
    _kafka_producer = kafka_client.create_producer(KAFKA_BOOTSTRAP_SERVERS)
    _neighbor_cache = LRUCache(NEIGHBOR_CACHE_SIZE)

    threading.Thread(
        target=kafka_client.run_consumer,
        args=(DATABASE_URL, _neighbor_cache, KAFKA_NEIGHBOR_TOPIC, KAFKA_BOOTSTRAP_SERVERS),
        daemon=True,
    ).start()

    yield

    _hyperplanes = None
    if _kafka_producer is not None:
        _kafka_producer.close()
    _kafka_producer = None
    _neighbor_cache = None


app = FastAPI(title="Vector Hasher", lifespan=lifespan)

# These classes describe what JSON the API accepts and returns.
class HashRequest(BaseModel):
    vector: list[float]


class HashResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    shard_id: int = Field(alias="shardId")
    hash_bits: list[int] = Field(alias="hashBits")


class AssignShardsResponse(BaseModel):
    total: int
    updated: int


class Candidate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    title: str


class CandidatesResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    shard_id: int = Field(alias="shardId")
    candidates: list[Candidate]


class NeighborEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    distance: float


class IndexResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    changed: list[str]


class IndexAllResponse(BaseModel):
    total: int
    changed: int


class NeighborsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    neighbors: list[NeighborEntry]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

# Takes a vector (list of floats)
# Validates it has correct dimensionality
# Computes its LSH hash (bit signature + shard ID)
# Returns which shard the vector belongs to and its hash bits

# Use case: Client-side hashing or immediate shard lookup.
# /hash — Hash one vector

# Imagine a client sends one embedding.
@app.post("/hash", response_model=HashResponse)
def hash_vector(request: HashRequest) -> HashResponse:
    if _hyperplanes is None:
        raise RuntimeError("Hyperplanes are not initialized")
    if len(request.vector) != EMBEDDING_DIM:
        raise HTTPException(
            status_code=400,
            detail=f"Expected a {EMBEDDING_DIM}-dim vector, got {len(request.vector)}",
        )
# Compute the hash and shard ID for a single embedding vector.
    vector = np.array(request.vector, dtype="<f4")
    result = lsh.compute_hash(vector, _hyperplanes, NUM_SHARDS)
    # which FastAPI converts into JSON.
    return HashResponse(shard_id=result.shard_id, hash_bits=result.hash_bits)

# Fetches all vectors from the database
# Computes their shard IDs using LSH
# Writes shard assignments back to the database
# Returns total processed count

# /assign-shards — Hash every vector in the database

# Instead of one vector, it processes all stored vectors.
@app.post("/assign-shards", response_model=AssignShardsResponse)
def assign_shards() -> AssignShardsResponse:
    if _hyperplanes is None:
        raise RuntimeError("Hyperplanes are not initialized")

    rows = db.fetch_all_vectors(DATABASE_URL)
    assignments = [
        (entity_id, lsh.compute_hash(vector, _hyperplanes, NUM_SHARDS).shard_id)
        for entity_id, vector in rows
    ]
    db.update_shard_ids(DATABASE_URL, assignments)
    return AssignShardsResponse(total=len(rows), updated=len(assignments))

# It first checks how many articles are in the same shard as the entity.
# If that's enough (>= MIN_CANDIDATES), it only searches that one shard.
# If not enough, it widens the search to that shard's neighbors too (using adjacent_shard_ids, already in lsh.py).
# Find which shard the given article belongs to.
@app.get("/candidates/{entity_id}", response_model=CandidatesResponse)
def candidates(entity_id: str) -> CandidatesResponse:
    shard_id = db.fetch_shard_id(DATABASE_URL, entity_id)
    if shard_id is None:
        raise HTTPException(status_code=404, detail=f"Unknown entityId: {entity_id}")
# Retrieve candidate articles from the same shard (excluding the current article).
    same_bucket = db.fetch_candidates(DATABASE_URL, [shard_id], entity_id)
    shard_ids = lsh.resolve_candidate_shard_ids(
        shard_id, len(same_bucket), NUM_SHARDS, MIN_CANDIDATES
    )
    results = (
        same_bucket
        if shard_ids == [shard_id]
        else db.fetch_candidates(DATABASE_URL, shard_ids, entity_id)
    )

    return CandidatesResponse(
        entity_id=entity_id,
        shard_id=shard_id,
        candidates=[Candidate(entity_id=c["entityId"], title=c["title"]) for c in results],
    )


# Runs the full index_entity pipeline for one entity: build its own list,
# reverse-check every candidate, save + publish whatever changed.
@app.post("/neighbors/{entity_id}/index", response_model=IndexResponse)
def index_neighbor(entity_id: str) -> IndexResponse:
    if _kafka_producer is None:
        raise RuntimeError("Kafka producer is not initialized")
    try:
        changed = neighbor_index.index_entity(
            DATABASE_URL,
            _kafka_producer,
            entity_id,
            NEIGHBOR_INDEX_K,
            NUM_SHARDS,
            MIN_CANDIDATES,
            KAFKA_NEIGHBOR_TOPIC,
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Unknown entityId: {entity_id}")

    return IndexResponse(entity_id=entity_id, changed=changed)


# Bootstraps the whole current dataset — same role /assign-shards played in
# Milestone 2. Runs index_entity once per entity currently in `entities`.
@app.post("/neighbors/index-all", response_model=IndexAllResponse)
def index_all_neighbors() -> IndexAllResponse:
    if _kafka_producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    entity_ids = [entity_id for entity_id, _ in db.fetch_all_vectors(DATABASE_URL)]
    total_changed = 0
    for entity_id in entity_ids:
        changed = neighbor_index.index_entity(
            DATABASE_URL,
            _kafka_producer,
            entity_id,
            NEIGHBOR_INDEX_K,
            NUM_SHARDS,
            MIN_CANDIDATES,
            KAFKA_NEIGHBOR_TOPIC,
        )
        total_changed += len(changed)

    return IndexAllResponse(total=len(entity_ids), changed=total_changed)


# Read-side lookup: cache first, neighbor_index_read on a miss (and re-caches
# the result so the next read for the same entity is a hit).
@app.get("/neighbors/{entity_id}", response_model=NeighborsResponse)
def get_neighbors(entity_id: str) -> NeighborsResponse:
    if _neighbor_cache is None:
        raise RuntimeError("Neighbor cache is not initialized")

    neighbors = _neighbor_cache.get(entity_id)
    if neighbors is None:
        neighbors = db.fetch_neighbors_read(DATABASE_URL, entity_id)
        if neighbors is None:
            raise HTTPException(status_code=404, detail=f"Unknown entityId: {entity_id}")
        _neighbor_cache.set(entity_id, neighbors)

    return NeighborsResponse(
        entity_id=entity_id,
        neighbors=[
            NeighborEntry(entity_id=n["entityId"], distance=n["distance"]) for n in neighbors
        ],
    )
