package com.velocity.entityinteraction.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ShardUtilTest {

    @Test
    void sameUserIdAlwaysMapsToSameShard() {
        assertThat(ShardUtil.shardFor("U131", 8)).isEqualTo(ShardUtil.shardFor("U131", 8));
    }

    @Test
    void resultIsAlwaysWithinBounds() {
        String[] userIds = {"U1", "U131", "U9999", "U0", "Uabcxyz", "", "U-negative-hashcode-candidate"};
        int numShards = 8;

        for (String userId : userIds) {
            int shard = ShardUtil.shardFor(userId, numShards);
            assertThat(shard).isGreaterThanOrEqualTo(0).isLessThan(numShards);
        }
    }

    @Test
    void negativeStringHashCodeStillProducesNonNegativeShard() {
        // "polygenelubricants" has a well-known negative String.hashCode(); floorMod (not %) is
        // what guarantees a non-negative shard even when hashCode() is negative.
        int shard = ShardUtil.shardFor("polygenelubricants", 8);
        assertThat(shard).isGreaterThanOrEqualTo(0).isLessThan(8);
    }
}
