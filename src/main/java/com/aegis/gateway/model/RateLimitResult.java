package com.aegis.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Encapsulates the evaluation outcome of a rate limiting check across caches or distributed stores.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitResult {

    private boolean allowed;
    private long remainingTokens;
    private long retryAfterSeconds;
    private long retryAfterMillis;
    private long currentLimit;
    private String source;

    public static RateLimitResult allowed(long remainingTokens, long currentLimit, String source) {
        return RateLimitResult.builder()
                .allowed(true)
                .remainingTokens(remainingTokens)
                .retryAfterSeconds(0L)
                .retryAfterMillis(0L)
                .currentLimit(currentLimit)
                .source(source)
                .build();
    }

    public static RateLimitResult denied(long retryAfterSeconds, long currentLimit, String source) {
        return RateLimitResult.builder()
                .allowed(false)
                .remainingTokens(0L)
                .retryAfterSeconds(retryAfterSeconds)
                .retryAfterMillis(retryAfterSeconds * 1000L)
                .currentLimit(currentLimit)
                .source(source)
                .build();
    }

    public static RateLimitResult denied(long retryAfterSeconds, long retryAfterMillis, long currentLimit, String source) {
        return RateLimitResult.builder()
                .allowed(false)
                .remainingTokens(0L)
                .retryAfterSeconds(retryAfterSeconds)
                .retryAfterMillis(retryAfterMillis)
                .currentLimit(currentLimit)
                .source(source)
                .build();
    }
}
