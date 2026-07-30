package com.velocity.recommendation.service;

import com.velocity.recommendation.util.MurmurHash3;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

// BloomFilterReadService is a read-only service that checks whether a user 
// has probably already seen a particular entity by reading that user's Bloom
//  filter stored in Redis.

// Its responsibilities are:

// Build the Redis key for the user's Bloom filter.
// Compute the 7 hash positions for a (userId, entityId) pair.
// Read those 7 bits from Redis.
// Return:
// false → definitely not seen.
// true → probably seen (Bloom filters can have false positives).

// Read-only mirror of entity-interaction-service's BloomFilterService (no add() — only that
// service's consumer ever writes these bits). BIT_SIZE/HASH_COUNT/the hash formula must match
// exactly, or the bit positions checked here wouldn't line up with the ones set on write.
@Service
public class BloomFilterReadService {
//     More hashes:

// ✅ Lower false positives (up to a point)
// user45|entity123
    private static final int BIT_SIZE = 960;
    private static final int HASH_COUNT = 7;

    private final StringRedisTemplate redisTemplate;

    public BloomFilterReadService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // A missing key means "never built or wiped" — callers must not read that as "every bit is 0,
    // so nothing is seen." GETBIT on a missing key returns 0 for every position, indistinguishable
    // from a genuinely unseen entity, so this check has to happen before trusting mightContain at all.
    // Bloom filter exists, entity not seen ✅
    public boolean exists(String userId) {
        Boolean exists = redisTemplate.hasKey(key(userId));
        return Boolean.TRUE.equals(exists);
    }

    public boolean mightContain(String userId, String entityId) {
        String key = key(userId);
        for (int bitPosition : bitPositions(userId, entityId)) {
            if (!Boolean.TRUE.equals(redisTemplate.opsForValue().getBit(key, bitPosition))) {
                return false;
            }
        }
        return true;
    }

    private String key(String userId) {
        return "user:" + userId + ":bloomfilter";
    }

    private int[] bitPositions(String userId, String entityId) {
        byte[] data = (userId + "|" + entityId).getBytes(StandardCharsets.UTF_8);
        int h1 = MurmurHash3.hash32(data, 0);
        int h2 = MurmurHash3.hash32(data, 1);

        int[] positions = new int[HASH_COUNT];
        for (int i = 0; i < HASH_COUNT; i++) {
            positions[i] = Math.floorMod(h1 + i * h2, BIT_SIZE);
        }
        return positions;
    }
}
