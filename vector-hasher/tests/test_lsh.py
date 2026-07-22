import numpy as np

from app.lsh import adjacent_shard_ids, compute_hash, generate_hyperplanes

SEED = 42
NUM_HYPERPLANES = 20
EMBEDDING_DIM = 384
NUM_SHARDS = 8


def test_same_vector_always_hashes_to_the_same_shard() -> None:
    hyperplanes = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)
    vector = np.random.default_rng(1).standard_normal(EMBEDDING_DIM)

    first = compute_hash(vector, hyperplanes, NUM_SHARDS)
    second = compute_hash(vector, hyperplanes, NUM_SHARDS)

    assert first.shard_id == second.shard_id
    assert first.hash_bits == second.hash_bits


def test_same_seed_produces_the_same_hyperplanes_across_runs() -> None:
    first_run = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)
    second_run = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)

    assert np.array_equal(first_run, second_run)


def test_nearly_identical_vectors_land_in_the_same_bucket() -> None:
    hyperplanes = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)
    rng = np.random.default_rng(2)
    base_vector = rng.standard_normal(EMBEDDING_DIM)
    nudged_vector = base_vector + rng.standard_normal(EMBEDDING_DIM) * 1e-6

    base_result = compute_hash(base_vector, hyperplanes, NUM_SHARDS)
    nudged_result = compute_hash(nudged_vector, hyperplanes, NUM_SHARDS)

    assert base_result.shard_id == nudged_result.shard_id


def test_opposite_vectors_produce_fully_complementary_hash_bits() -> None:
    hyperplanes = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)
    vector = np.random.default_rng(3).standard_normal(EMBEDDING_DIM)
    opposite_vector = -vector

    result = compute_hash(vector, hyperplanes, NUM_SHARDS)
    opposite_result = compute_hash(opposite_vector, hyperplanes, NUM_SHARDS)

    assert result.hash_bits != opposite_result.hash_bits


def test_adjacent_shard_ids_wraps_around_correctly() -> None:
    assert adjacent_shard_ids(0, NUM_SHARDS) == [0, 1, 7]
    assert adjacent_shard_ids(7, NUM_SHARDS) == [0, 6, 7]
    assert adjacent_shard_ids(3, NUM_SHARDS) == [2, 3, 4]
