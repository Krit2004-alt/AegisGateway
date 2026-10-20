package com.aegis.gateway.repository;

import com.aegis.gateway.model.RateLimitResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Reactive Redis repository executing distributed sliding window rate limiting via Lua script.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisRateLimiterRepository {

    public static final String SOURCE_REDIS = "REDIS_SLIDING_WINDOW";
    private static final String KEY_PREFIX = "rate_limit:";

    private final ReactiveStringRedisTemplate redisTemplate;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> slidingWindowRedisScript;

    /**
     * Checks if the request is allowed using default cost of 1 token.
     *
     * @param tenantId         tenant or client identifier
     * @param limit            maximum allowed quota in the window
     * @param windowSizeMillis sliding window duration in milliseconds
     * @return Mono emitting RateLimitResult
     */
    public Mono<RateLimitResult> checkRateLimit(String tenantId, long limit, long windowSizeMillis) {
        return checkRateLimit(tenantId, limit, windowSizeMillis, 1L);
    }

    /**
     * Checks if the request is allowed given custom token cost.
     *
     * @param tenantId         tenant or client identifier
     * @param limit            maximum allowed quota in the window
     * @param windowSizeMillis sliding window duration in milliseconds
     * @param cost             token cost for this request
     * @return Mono emitting RateLimitResult
     */
    public Mono<RateLimitResult> checkRateLimit(String tenantId, long limit, long windowSizeMillis, long cost) {
        long now = System.currentTimeMillis();
        long windowId = now / windowSizeMillis;

        String currentKey = KEY_PREFIX + tenantId + ":" + windowId;
        String prevKey = KEY_PREFIX + tenantId + ":" + (windowId - 1);

        List<String> keys = List.of(currentKey, prevKey);
        List<String> args = List.of(
                String.valueOf(limit),
                String.valueOf(now),
                String.valueOf(windowSizeMillis),
                String.valueOf(cost)
        );

        return redisTemplate.execute(slidingWindowRedisScript, keys, args)
                .next()
                .map(rawList -> parseResult(rawList, limit))
                .defaultIfEmpty(RateLimitResult.allowed(limit, limit, SOURCE_REDIS))
                .doOnError(ex -> log.error("Error executing Redis rate limiter script for key '{}': {}", tenantId, ex.getMessage()));
    }

    /**
     * Checks if the request is allowed using default cost of 1 token (delegates to checkRateLimit).
     */
    public Mono<RateLimitResult> isAllowed(String key, long limit, long windowSizeMillis) {
        return checkRateLimit(key, limit, windowSizeMillis, 1L);
    }

    /**
     * Checks if the request is allowed given custom token cost (delegates to checkRateLimit).
     */
    public Mono<RateLimitResult> isAllowed(String key, long limit, long windowSizeMillis, long cost) {
        return checkRateLimit(key, limit, windowSizeMillis, cost);
    }

    /**
     * Safely parses the Lua script result table into a RateLimitResult.
     * Output format from Lua: {allowed (1 or 0), remaining_tokens, retry_after_ms}
     */
    protected RateLimitResult parseResult(List<?> rawList, long limit) {
        long allowedFlag = extractLong(rawList, 0, 0L);
        long remainingTokens = extractLong(rawList, 1, 0L);
        long retryAfterMs = extractLong(rawList, 2, 0L);

        boolean allowed = (allowedFlag == 1L);
        long retryAfterSeconds = retryAfterMs > 0 ? (long) Math.ceil(retryAfterMs / 1000.0) : 0L;

        return RateLimitResult.builder()
                .allowed(allowed)
                .remainingTokens(remainingTokens)
                .retryAfterSeconds(retryAfterSeconds)
                .retryAfterMillis(retryAfterMs)
                .currentLimit(limit)
                .source(SOURCE_REDIS)
                .build();
    }

    /**
     * Safely extracts a numeric output as Long to prevent ClassCastException across Redis drivers.
     *
     * @param list         the raw result list from Redis Lua execution
     * @param index        position in the list
     * @param defaultValue fallback value if null or unparseable
     * @return extracted Long value
     */
    protected long extractLong(List<?> list, int index, long defaultValue) {
        if (list == null || index < 0 || index >= list.size()) {
            return defaultValue;
        }
        Object item = list.get(index);
        if (item instanceof Number number) {
            return number.longValue();
        } else if (item != null) {
            try {
                return Long.parseLong(item.toString().trim());
            } catch (NumberFormatException e) {
                log.warn("Unable to parse number from Redis script output at index {}: '{}'", index, item);
                return defaultValue;
            }
        }
        return defaultValue;
    }
}
