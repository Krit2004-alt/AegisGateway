package com.aegis.gateway.service;

import com.aegis.gateway.model.RateLimitResult;
import com.aegis.gateway.model.TenantContext;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone in-memory sliding window rate limiter using Caffeine.
 * Provides a conservative degraded quota (1/3 of nominal sustained limit)
 * when the distributed Redis store is unavailable or its circuit breaker is open.
 */
@Slf4j
@Service
public class FallbackLocalRateLimiter {

    public static final String SOURCE_FALLBACK_DEGRADED = "LOCAL_FALLBACK_DEGRADED";
    private static final int MAX_CACHE_ENTRIES = 20_000;

    private final Cache<String, AtomicLong> windowCounts;

    public FallbackLocalRateLimiter() {
        this.windowCounts = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(5))
                .maximumSize(MAX_CACHE_ENTRIES)
                .build();
    }

    /**
     * Evaluates rate limiting under local in-memory partitioned fallback quota for a given TenantContext.
     *
     * @param context the tenant context containing policy and tier details
     * @return RateLimitResult indicating if allowed under degraded local quota
     */
    public RateLimitResult evaluateFallback(TenantContext context) {
        if (context == null || context.getPolicy() == null) {
            return evaluateInternal("anonymous", 60L, 60000L, 1L);
        }
        String key = context.getTenantId() != null ? context.getTenantId() : "anonymous";
        long limit = context.getPolicy().getSustainedQuota() + context.getPolicy().getBurstQuota();
        long windowSizeMillis = context.getPolicy().getWindowSizeMillis() > 0
                ? context.getPolicy().getWindowSizeMillis()
                : 60000L;
        return evaluateInternal(key, limit, windowSizeMillis, 1L);
    }

    /**
     * Checks if the request is allowed under conservative local quota using cost 1.
     */
    public Mono<RateLimitResult> isAllowed(String key, long limit, long windowSizeMillis) {
        return isAllowed(key, limit, windowSizeMillis, 1L);
    }

    /**
     * Checks if the request is allowed under conservative local quota (1/3 of nominal limit).
     * Employs sliding window counter decay across current and previous window buckets.
     *
     * @param key              tenant or client identifier
     * @param limit            nominal sustained limit
     * @param windowSizeMillis window duration in milliseconds
     * @param cost             token cost for this request
     * @return Mono emitting RateLimitResult with source LOCAL_FALLBACK_DEGRADED
     */
    public Mono<RateLimitResult> isAllowed(String key, long limit, long windowSizeMillis, long cost) {
        return Mono.fromSupplier(() -> evaluateInternal(key, limit, windowSizeMillis, cost));
    }

    /**
     * Synchronous in-memory sliding window rate evaluation using Caffeine.
     */
    public RateLimitResult evaluateInternal(String key, long limit, long windowSizeMillis, long cost) {
        long conservativeLimit = Math.max(1L, limit / 3L);
        long now = System.currentTimeMillis();
        long windowId = now / windowSizeMillis;

        String currentKey = key + ":" + windowId;
        String prevKey = key + ":" + (windowId - 1);

        AtomicLong currentCounter = windowCounts.get(currentKey, k -> new AtomicLong(0L));
        AtomicLong prevCounter = windowCounts.getIfPresent(prevKey);
        long prevCount = (prevCounter != null) ? prevCounter.get() : 0L;

        long timeIntoWindow = now % windowSizeMillis;
        double fractionElapsed = (double) timeIntoWindow / windowSizeMillis;
        double decay = Math.max(0.0, 1.0 - fractionElapsed);

        long currentCount = currentCounter.get();
        long estimatedCount = currentCount + (long) Math.floor(prevCount * decay);

        if (estimatedCount + cost <= conservativeLimit) {
            currentCounter.addAndGet(cost);
            long remaining = Math.max(0L, conservativeLimit - (estimatedCount + cost));

            log.debug("Fallback local rate limiter ALLOWED request for '{}': {}/{} used, remaining {}",
                    key, estimatedCount + cost, conservativeLimit, remaining);

            return RateLimitResult.builder()
                    .allowed(true)
                    .remainingTokens(remaining)
                    .retryAfterSeconds(0L)
                    .retryAfterMillis(0L)
                    .currentLimit(conservativeLimit)
                    .source(SOURCE_FALLBACK_DEGRADED)
                    .build();
        } else {
            long retryAfterMs = windowSizeMillis - timeIntoWindow;
            long retryAfterSec = Math.max(1L, (long) Math.ceil(retryAfterMs / 1000.0));

            log.warn("Fallback local rate limiter DENIED request for '{}': quota {}/{} exceeded. Retry-After: {}s",
                    key, estimatedCount + cost, conservativeLimit, retryAfterSec);

            return RateLimitResult.builder()
                    .allowed(false)
                    .remainingTokens(0L)
                    .retryAfterSeconds(retryAfterSec)
                    .retryAfterMillis(retryAfterMs)
                    .currentLimit(conservativeLimit)
                    .source(SOURCE_FALLBACK_DEGRADED)
                    .build();
        }
    }
}
