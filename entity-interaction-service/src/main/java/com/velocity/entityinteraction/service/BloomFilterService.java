package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.util.MurmurHash3;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class BloomFilterService {

    // Sized for n=100 expected entities/user, p=0.01 false-positive rate:
    // m = -(n * ln p) / (ln 2)^2 ~= 960 bits, k = (m/n) * ln 2 ~= 7 hash functions.
    // Real data shows p99=137 entities/user, which exceeds this design capacity for the heaviest
    // users — false-positive rate creeps up for them, which is safe (fallback to DB) but worth
    // tracking; not fixed here (see PROJECT_SPEC.md #10).
    private static final int BIT_SIZE = 960;
    private static final int HASH_COUNT = 7;

    private final StringRedisTemplate redisTemplate;

    public BloomFilterService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void add(String userId, String entityId) {
        String key = "user:" + userId + ":bloomfilter";
        for (int bitPosition : bitPositions(userId, entityId)) {
            redisTemplate.opsForValue().setBit(key, bitPosition, true);
        }
    }

    public boolean mightContain(String userId, String entityId) {
        String key = "user:" + userId + ":bloomfilter";
        for (int bitPosition : bitPositions(userId, entityId)) {
            if (!Boolean.TRUE.equals(redisTemplate.opsForValue().getBit(key, bitPosition))) {
                return false;
            }
        }
        return true;
    }

    // Kirsch-Mitzenmacher double hashing: derive k bit positions from just 2 real hash calls
    // instead of k separate ones — h_i(x) = h1(x) + i*h2(x) mod m, statistically as good as k
    // independent hashes for Bloom filter purposes.
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
