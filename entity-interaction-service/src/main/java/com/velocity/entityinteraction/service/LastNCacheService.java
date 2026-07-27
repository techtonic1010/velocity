package com.velocity.entityinteraction.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class LastNCacheService {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> lastNRingBufferScript;
    private final int lastNSize;

    public LastNCacheService(
            StringRedisTemplate redisTemplate,
            RedisScript<Long> lastNRingBufferScript,
            @Value("${redis.last-n-size}") int lastNSize) {
        this.redisTemplate = redisTemplate;
        this.lastNRingBufferScript = lastNRingBufferScript;
        this.lastNSize = lastNSize;
    }

    public void recordClick(String userId, String entityId) {
        String key = "user:" + userId + ":lastEntities";
        redisTemplate.execute(lastNRingBufferScript, List.of(key), entityId, String.valueOf(lastNSize - 1));
    }
}

// You need LastNCacheService because it encapsulates all Redis "last N entities" logic in one place.

// Instead of every class doing this:

// String key = "user:" + userId + ":lastEntities";
// redisTemplate.execute(script, ...);

// they simply call:

// lastNCacheService.recordClick(userId, entityId);
