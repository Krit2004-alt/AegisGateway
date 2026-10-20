package com.aegis.gateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

/**
 * Configuration class that registers Redis Lua scripts as Spring beans.
 */
@Configuration
public class RedisScriptConfig {

    /**
     * Registers the sliding window counter Lua script returning a List.
     *
     * @return RedisScript producing a List of numeric elements
     */
    @Bean
    @SuppressWarnings("rawtypes")
    public RedisScript<List> slidingWindowRedisScript() {
        ClassPathResource resource = new ClassPathResource("scripts/sliding_window_counter.lua");
        return RedisScript.of(resource, List.class);
    }
}
