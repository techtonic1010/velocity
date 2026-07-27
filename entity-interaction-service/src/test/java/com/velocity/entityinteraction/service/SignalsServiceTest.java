package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionType;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.mockito.Mockito.*;

class SignalsServiceTest {

    @Test
    void recordStoresInteractionTypeNameUnderTheUsersSignalsHash() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        SignalsService service = new SignalsService(redisTemplate);

        service.record("U131", "N45", InteractionType.LIKE);

        verify(hashOps).put("user:U131:signals", "N45", "LIKE");
    }

    @Test
    void clickAndDislikeAreAlsoStoredNotJustLikeDislike() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        SignalsService service = new SignalsService(redisTemplate);

        service.record("U131", "N45", InteractionType.CLICK);
        service.record("U131", "N46", InteractionType.DISLIKE);

        verify(hashOps).put("user:U131:signals", "N45", "CLICK");
        verify(hashOps).put("user:U131:signals", "N46", "DISLIKE");
    }
}
