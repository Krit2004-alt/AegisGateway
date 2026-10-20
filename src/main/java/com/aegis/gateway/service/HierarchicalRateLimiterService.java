package com.aegis.gateway.service;

import com.aegis.gateway.model.LocalTokenLease;
import com.aegis.gateway.model.RateLimitResult;
import com.aegis.gateway.model.TenantContext;
import com.aegis.gateway.repository.RedisRateLimiterRepository;
import com.github.benmanes.caffeine.cache.Cache;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Two-tier hierarchical rate limiter service combining L1 in-memory Caffeine cache,
 * L2 distributed sliding window Redis rate limiting wrapped in Resilience4j circuit breaker,
 * and a degraded local fallback rate limiter.
 */
@Service
public class HierarchicalRateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(HierarchicalRateLimiterService.class);
    public static final String SOURCE_L1_CACHE = "L1_CACHE";
    public static final String CIRCUIT_BREAKER_NAME = "redisRateLimiter";

    private final Cache<String, LocalTokenLease> localLeaseCache;
    private final RedisRateLimiterRepository redisRepository;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final FallbackLocalRateLimiter fallbackRateLimiter;
    private final MeterRegistry meterRegistry;

    private Counter l1Hits;
    private Counter l1Misses;
    private CircuitBreaker redisCircuitBreaker;

    @Autowired
    public HierarchicalRateLimiterService(
            Cache<String, LocalTokenLease> localLeaseCache,
            RedisRateLimiterRepository redisRepository,
            CircuitBreakerRegistry circuitBreakerRegistry,
            FallbackLocalRateLimiter fallbackRateLimiter,
            MeterRegistry meterRegistry) {
        this.localLeaseCache = localLeaseCache;
        this.redisRepository = redisRepository;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.fallbackRateLimiter = fallbackRateLimiter;
        this.meterRegistry = meterRegistry;
        initDependencies();
    }

    public HierarchicalRateLimiterService(
            Cache<String, LocalTokenLease> localLeaseCache,
            RedisRateLimiterRepository redisRepository,
            CircuitBreaker redisCircuitBreaker,
            FallbackLocalRateLimiter fallbackRateLimiter,
            MeterRegistry meterRegistry) {
        this.localLeaseCache = localLeaseCache;
        this.redisRepository = redisRepository;
        this.circuitBreakerRegistry = null;
        this.redisCircuitBreaker = redisCircuitBreaker != null ? redisCircuitBreaker : CircuitBreaker.ofDefaults(CIRCUIT_BREAKER_NAME);
        this.fallbackRateLimiter = fallbackRateLimiter;
        this.meterRegistry = meterRegistry;
        initMetricsOnly();
    }

    @PostConstruct
    public void initMetrics() {
        initDependencies();
    }

    private void initDependencies() {
        if (circuitBreakerRegistry != null && redisCircuitBreaker == null) {
            this.redisCircuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        } else if (redisCircuitBreaker == null) {
            this.redisCircuitBreaker = CircuitBreaker.ofDefaults(CIRCUIT_BREAKER_NAME);
        }
        initMetricsOnly();
    }

    private void initMetricsOnly() {
        if (meterRegistry != null && l1Hits == null) {
            this.l1Hits = Counter.builder("gateway.rate_limiter.l1.hits")
                    .description("Total L1 local cache rate limit hits")
                    .tag("layer", "L1")
                    .register(meterRegistry);
            this.l1Misses = Counter.builder("gateway.rate_limiter.l1.misses")
                    .description("Total L1 local cache misses needing Redis check")
                    .tag("layer", "L1")
                    .register(meterRegistry);
        }
    }

    /**
     * Evaluates rate limiting for a tenant context.
     *
     * @param context the resolved TenantContext
     * @return Mono emitting RateLimitResult
     */
    public Mono<RateLimitResult> evaluate(TenantContext context) {
        if (context == null || context.getPolicy() == null) {
            return isAllowed("anonymous", 60L, 60000L, 1L);
        }
        String tenantId = context.getTenantId();
        long totalLimit = context.getPolicy().getSustainedQuota() + context.getPolicy().getBurstQuota();
        long windowSizeMs = context.getPolicy().getWindowSizeMillis() > 0
                ? context.getPolicy().getWindowSizeMillis()
                : 60000L;
        return isAllowed(tenantId, totalLimit, windowSizeMs, 1L);
    }

    public Mono<RateLimitResult> isAllowed(TenantContext context) {
        return evaluate(context);
    }

    public Mono<RateLimitResult> isAllowed(String key, long limit, long windowSizeMillis) {
        return isAllowed(key, limit, windowSizeMillis, 1L);
    }

    public Mono<RateLimitResult> isAllowed(String key, long limit, long windowSizeMillis, long cost) {
        // High-throughput tiers (PRO, ENTERPRISE, or custom limit > 60) leverage L1 cache micro-batching.
        // The FREE tier (limit <= 60) enforces strict sliding window accounting directly in Redis
        // to prevent quota bypass and ensure noisy neighbor bursts are rejected at exactly quota depletion.
        boolean enableL1 = (limit > 60L);
        LocalTokenLease lease = enableL1 ? localLeaseCache.getIfPresent(key) : null;

        if (lease != null && !lease.isExpired() && lease.tryConsume(cost)) {
            if (l1Hits != null) {
                l1Hits.increment();
            }
            return Mono.just(RateLimitResult.builder()
                    .allowed(true)
                    .remainingTokens(lease.getRemaining())
                    .retryAfterSeconds(0)
                    .currentLimit(lease.getTotalLimit())
                    .source(SOURCE_L1_CACHE)
                    .build());
        }

        if (l1Misses != null) {
            l1Misses.increment();
        }

        return Mono.defer(() -> {
                    Mono<RateLimitResult> redisMono = redisRepository.checkRateLimit(key, limit, windowSizeMillis, cost);
                    return redisMono != null ? redisMono : Mono.empty();
                })
                .transformDeferred(CircuitBreakerOperator.of(redisCircuitBreaker))
                .map(redisResult -> {
                    if (enableL1 && redisResult.isAllowed() && redisResult.getRemainingTokens() > 0) {
                        long batchTokens = Math.min(50L, Math.max(1L, redisResult.getRemainingTokens() / 4));
                        long expiryEpoch = System.currentTimeMillis() + 2000L;
                        localLeaseCache.put(key, new LocalTokenLease(batchTokens, expiryEpoch, limit));
                    }
                    return redisResult;
                })
                .onErrorResume(throwable -> {
                    log.warn("Redis unavailable or Circuit Breaker OPEN. Falling back to local rate limiter for tenant {}. Cause: {}", key, throwable.getMessage());
                    return fallbackRateLimiter.isAllowed(key, limit, windowSizeMillis, cost);
                });
    }
}
