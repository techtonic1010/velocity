package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.util.MurmurHash3;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class BloomFilterServiceTest {

    private static final int BIT_SIZE = 960;
    private static final int HASH_COUNT = 7;

    // Mirrors BloomFilterService's private bitPositions() formula (Kirsch-Mitzenmacher double
    // hashing) using the same public MurmurHash3 utility, so the test can assert exact bit
    // positions instead of just "some bits got set".
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
    void addSetsExactlySevenBitsAtTheKirschMitzenmacherPositions() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        BloomFilterService service = new BloomFilterService(redisTemplate);

        service.add("U131", "N45");

        String expectedKey = "user:U131:bloomfilter";
        int[] expectedPositions = computeBitPositions("U131", "N45");

        ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);
        verify(valueOps, times(HASH_COUNT)).setBit(eq(expectedKey), offsetCaptor.capture(), eq(true));

        List<Long> capturedOffsets = offsetCaptor.getAllValues();
        assertThat(capturedOffsets).hasSize(HASH_COUNT);
        for (int i = 0; i < HASH_COUNT; i++) {
            assertThat(capturedOffsets.get(i)).isEqualTo((long) expectedPositions[i]);
        }
    }

    @Test
    void mightContainReturnsTrueWhenAllSevenBitsAreSet() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(true);
        BloomFilterService service = new BloomFilterService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isTrue();
    }

    @Test
    void mightContainReturnsFalseAndShortCircuitsOnFirstUnsetBit() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        // First position checked (i=0) comes back unset; every other position would be "set" but
        // must never be reached because the loop returns as soon as it hits a false.
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(false, true, true, true, true, true, true);
        BloomFilterService service = new BloomFilterService(redisTemplate);

        boolean result = service.mightContain("U131", "N45");

        assertThat(result).isFalse();
        verify(valueOps, times(1)).getBit(anyString(), anyLong());
    }

    @Test
    void mightContainTreatsNullBitAsUnset() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(null);
        BloomFilterService service = new BloomFilterService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isFalse();
    }
}
