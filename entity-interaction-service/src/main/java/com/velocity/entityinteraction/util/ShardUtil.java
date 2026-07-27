package com.velocity.entityinteraction.util;

// Logical sharding only (per PROJECT_SPEC.md §2's scope decision) — there's one Redis container in
// docker-compose.yml, this just computes/records which shard would own a user in a real deployment.
// A distinct concept from vector-hasher's NUM_SHARDS (LSH bucket count) — don't conflate the two.
public final class ShardUtil {

    private ShardUtil() {
    }

    public static int shardFor(String userId, int numShards) {
        return Math.floorMod(userId.hashCode(), numShards);
    }
}
