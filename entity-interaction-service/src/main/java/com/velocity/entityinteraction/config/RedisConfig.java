package com.velocity.entityinteraction.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

@Configuration
public class RedisConfig {

    // LREM (remove any existing copy) + LPUSH (push to front) + LTRIM (trim to size) as ONE Lua
    // script, so Redis runs all three atomically — no other update to the same key can interleave
    // between them. Returns the list's final length (LLEN) so the caller gets a real Long back,
    // since LTRIM's own reply ("OK") isn't a usable return type here.

    // So LLEN is mainly there to provide a numeric return value,
    //  not because the ring buffer logic depends on it.
    @Bean
    public RedisScript<Long> lastNRingBufferScript() {
        String script = """
                redis.call('LREM', KEYS[1], 0, ARGV[1])
                redis.call('LPUSH', KEYS[1], ARGV[1])
                redis.call('LTRIM', KEYS[1], 0, ARGV[2])
                return redis.call('LLEN', KEYS[1])
                """;
        return new DefaultRedisScript<>(script, Long.class);
    }
}
