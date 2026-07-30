package com.velocity.recommendation.service;

import com.velocity.recommendation.util.MurmurHash3;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
// User ID + Entity ID
//         │
//         ▼
// Generate 7 hash positions
//         │
//         ▼
// Check those 7 bits in Redis
//         │
//         ├── Any bit = 0 ?
//         │      ▼
//         │    Definitely NOT seen
//         │
//         └── All bits = 1
//                ▼
//         Probably seen
class BloomFilterReadServiceTest {

    private static final int BIT_SIZE = 960;
    private static final int HASH_COUNT = 7;

    // Mirrors BloomFilterReadService's private bitPositions() formula, using the same public
    // MurmurHash3 utility, so tests can assert exact bit positions instead of "some bit got checked".
    private static int[] computeBitPositions(String userId, String entityId) {
        byte[] data = (userId + "|" + entityId).getBytes(StandardCharsets.UTF_8);
        int h1 = MurmurHash3.hash32(data, 0);
        int h2 = MurmurHash3.hash32(data, 1);
        int[] positions = new int[HASH_COUNT];
        for (int i = 0; i < HASH_COUNT; i++) {
            positions[i] = Math.floorMod(h1 + i * h2, BIT_SIZE);
        }
        return positions;
    }

    @Test
    void existsReturnsTrueWhenTheKeyIsPresent() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.hasKey("user:U131:bloomfilter")).thenReturn(true);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.exists("U131")).isTrue();
    }

    @Test
    void existsReturnsFalseWhenTheKeyIsMissing() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.hasKey("user:U131:bloomfilter")).thenReturn(false);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.exists("U131")).isFalse();
    }

    @Test
    void existsReturnsFalseWhenRedisReturnsNull() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.hasKey("user:U131:bloomfilter")).thenReturn(null);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.exists("U131")).isFalse();
    }

    @Test
    void mightContainReturnsTrueWhenAllSevenBitsAreSet() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(true);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isTrue();
    }

    @Test
    void mightContainChecksTheExactExpectedBitPositions() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(true);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        service.mightContain("U131", "N45");

        int[] expectedPositions = computeBitPositions("U131", "N45");
        for (int position : expectedPositions) {
            verify(valueOps).getBit("user:U131:bloomfilter", position);
        }
    }

    @Test
    void mightContainReturnsFalseAndShortCircuitsOnFirstUnsetBit() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(false, true, true, true, true, true, true);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isFalse();
        verify(valueOps, times(1)).getBit(anyString(), anyLong());
    }

    @Test
    void mightContainTreatsNullBitAsUnset() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(null);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isFalse();
    }
}
