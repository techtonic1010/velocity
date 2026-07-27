package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class SignalsService {

    private final StringRedisTemplate redisTemplate;

    public SignalsService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // One entry per (user, entity) pair. Includes CLICK, not just LIKE/DISLIKE — deviates from
    // PROJECT_SPEC.md #5.4's literal shape, per this session's explicit decision.
    public void record(String userId, String entityId, InteractionType type) {
        String key = "user:" + userId + ":signals";
        redisTemplate.opsForHash().put(key, entityId, type.name());
    }
}
