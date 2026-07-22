from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class HashResult:
    hash_bits: list[int]
    shard_id: int


def generate_hyperplanes(
    num_hyperplanes: int, embedding_dim: int, seed: int
) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal((num_hyperplanes, embedding_dim))


def compute_hash(
    vector: np.ndarray, hyperplanes: np.ndarray, num_shards: int
) -> HashResult:
    projections = hyperplanes @ vector
    hash_bits = [1 if value >= 0 else 0 for value in projections]

    hash_value = 0
    for bit in hash_bits:
        hash_value = (hash_value << 1) | bit

    return HashResult(hash_bits=hash_bits, shard_id=hash_value % num_shards)


def adjacent_shard_ids(shard_id: int, num_shards: int) -> list[int]:
    return sorted({(shard_id - 1) % num_shards, shard_id, (shard_id + 1) % num_shards})

# It only picks which shard numbers to look in — 
# e.g. it returns something like [3] or [2, 3, 4]. It does not fetch any actual articles.
def resolve_candidate_shard_ids(
    shard_id: int, same_bucket_count: int, num_shards: int, min_candidates: int
) -> list[int]:
    if same_bucket_count >= min_candidates:
        return [shard_id]
    return adjacent_shard_ids(shard_id, num_shards)

# /////////////////////what did we save in the milestone 2 . because the same vectors , lie in the same shard . ?? isnt it ?? ans short
# that's the goal of LSH: similar vectors usually land in the same shard. 
# But not guaranteed — this project's own notes (PROGRESS.md) found a real '
# 'case where two very similar articles landed in different, non-adjacent shards, '
# 'just because of one unlucky bit. That's exactly why the "widen to adjacent shards"
# " if not enough candidates" fallback exists
def cosine_distance(vec1: np.ndarray, vec2: np.ndarray) -> float:
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    return 1 - (dot_product / (norm_vec1 * norm_vec2))


#             384 values
#         ┌──────────────────────┐
# HP1     │ • • • • • • • • • •  │
# HP2     │ • • • • • • • • • •  │
# HP3     │ • • • • • • • • • •  │
# ...     │                      │
# HP20    │ • • • • • • • • • •  │
#         └──────────────────────┘

# 20 Hyperplanes

# Hyperplane 1 → 384 numbers

# Hyperplane 2 → 384 numbers

# ...

# Hyperplane 20 → 384 numbers

# The dot product tells you which side of the hyperplane the vector lies on.

# Suppose one hyperplane is represented by a 384-dimensional vector h, and your embedding is another 384-dimensional vector v.

# You compute:

# projection = h · v

# Now interpret the sign:

# projection > 0
#         │
#         ▼
# One side of the hyperplane
# (bit = 1)


# One honest correction to my own plan text: I described these as
# "random unit vectors" — that's not quite what this does.
# standard_normal gives vectors of random magnitude, 
# not normalized to length 1. That's actually fine and deliberate: '
# 'for sign(dot(vector, r)), scaling r by any positive number never '
# 'changes the sign of the dot product — so whether r has length 1 or '
# 'length 7 makes zero difference to the resulting hash bit. Normalizing '
# 'would cost a sqrt + division per hyperplane for a result that's 
# mathematically identical either way, so skipping it isn't cutting '
# 'a corner — it's just not doing unnecessary work.