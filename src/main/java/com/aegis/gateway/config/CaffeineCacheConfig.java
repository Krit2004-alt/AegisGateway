package com.aegis.gateway.config;

import com.aegis.gateway.model.LocalTokenLease;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configuration for the L1 in-memory Caffeine cache utilizing 2-second expiration
 * and Window TinyLFU admission/eviction policy.
 */
@Configuration
public class CaffeineCacheConfig {

    public static final long DEFAULT_MAX_CACHE_SIZE = 50_000L;

    /**
     * Configures the L1 LocalTokenLease Caffeine cache bean.
     * Window TinyLFU eviction is activated by setting maximumSize.
     *
     * @return Cache instance holding LocalTokenLease by tenant/client key
     */
    @Bean
    public Cache<String, LocalTokenLease> localTokenLeaseCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(2))
                .maximumSize(DEFAULT_MAX_CACHE_SIZE)
                .recordStats()
                .build();
    }
}
