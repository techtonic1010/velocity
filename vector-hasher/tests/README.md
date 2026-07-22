# vector-hasher tests

## How to run

```
python3 -m venv .venv
source .venv/bin/activate
pip install numpy pytest
python -m pytest -v
```

No database, no Docker, no other services required — every test here exercises
`app/lsh.py` only (pure functions, zero I/O).

## Last run

```
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
collecting ... collected 5 items

tests/test_lsh.py::test_same_vector_always_hashes_to_the_same_shard PASSED [ 20%]
tests/test_lsh.py::test_same_seed_produces_the_same_hyperplanes_across_runs PASSED [ 40%]
tests/test_lsh.py::test_nearly_identical_vectors_land_in_the_same_bucket PASSED [ 60%]
tests/test_lsh.py::test_opposite_vectors_produce_fully_complementary_hash_bits PASSED [ 80%]
tests/test_lsh.py::test_adjacent_shard_ids_wraps_around_correctly PASSED [100%]

============================== 5 passed in 0.13s ===============================
```

## What each test checks, and why

### `test_same_vector_always_hashes_to_the_same_shard`
Hashes the same vector twice against the same hyperplanes and asserts both the
`shard_id` and the raw `hash_bits` are identical both times. Basic determinism
check — guards against any accidental randomness inside `compute_hash` itself.

### `test_same_seed_produces_the_same_hyperplanes_across_runs`
Calls `generate_hyperplanes` twice, independently, with the same seed, and
asserts the two resulting matrices are exactly equal (`np.array_equal`).

This is the single most important property in the whole service: if the
hyperplanes were re-randomized every time the process started, every
previously-assigned `shard_id` in Postgres would become meaningless the moment
the container restarted, since the same vector would hash differently before
and after. This test proves the fixed-seed design actually delivers that
stability, rather than just asserting it in a comment.

### `test_nearly_identical_vectors_land_in_the_same_bucket`
Builds a base vector, then a second vector nudged by noise roughly six orders
of magnitude smaller than the base vector's own values, and asserts both land
in the same `shard_id`. This is the core LSH property Milestone 2 exists to
demonstrate: similar input -> same bucket. Because the random seeds used to
build the test vectors are fixed, this test is fully deterministic — it is not
a "usually passes" test despite what it sounds like conceptually.

### `test_opposite_vectors_produce_fully_complementary_hash_bits`
Hashes a vector and its exact negation, and asserts the two `hash_bits` lists
differ. A vector and its negation are guaranteed to flip the sign of every
single hyperplane projection, so `hash_bits` is certain to differ in every
position — this isn't a probabilistic check.

Deliberately asserts on `hash_bits`, not `shard_id`. `shard_id = hash_value %
8` only depends on 3 of the 20 hash bits, so it's a lossy compression: two
fully-opposite hash patterns (all-0s vs all-1s) can still reduce to the *same*
`shard_id`, since 2^20 is itself divisible by 8. Asserting `shard_id` differs
here would be a false assumption; asserting `hash_bits` differ is the property
that's actually always true.

### `test_adjacent_shard_ids_wraps_around_correctly`
Three hand-verified cases for the "adjacent bucket" fallback used by
`/candidates`: `shard_id=0`'s neighbors are `7` and `1` (wraps around the low
end), `shard_id=7`'s neighbors are `6` and `0` (wraps around the high end),
and `shard_id=3` (an ordinary middle case) gives `2` and `4`. Directly tests
the modular wraparound arithmetic, the kind of boundary logic that's easy to
get subtly wrong without an explicit test.
