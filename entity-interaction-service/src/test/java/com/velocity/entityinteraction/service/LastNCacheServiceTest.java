package com.velocity.entityinteraction.service;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

import static org.mockito.Mockito.*;

class LastNCacheServiceTest {

    @Test
    void recordClickRunsTheRingBufferScriptWithKeyEntityIdAndSizeMinusOne() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        RedisScript<Long> script = mock(RedisScript.class);
        LastNCacheService service = new LastNCacheService(redisTemplate, script, 5);

        service.recordClick("U131", "N45");

        // lastNSize=5 -> LTRIM upper bound is size-1=4, since the buffer is a 0-indexed max size.
        verify(redisTemplate).execute(script, List.of("user:U131:lastEntities"), "N45", "4");
    }

    @Test
    void differentUsersGetIndependentKeys() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        RedisScript<Long> script = mock(RedisScript.class);
        LastNCacheService service = new LastNCacheService(redisTemplate, script, 5);

        service.recordClick("U1", "N1");
        service.recordClick("U2", "N1");

        verify(redisTemplate).execute(script, List.of("user:U1:lastEntities"), "N1", "4");
        verify(redisTemplate).execute(script, List.of("user:U2:lastEntities"), "N1", "4");
    }
}
