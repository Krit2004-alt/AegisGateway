package com.aegis.gateway.service;

import com.aegis.gateway.model.LocalTokenLease;
import com.aegis.gateway.model.RateLimitResult;
import com.aegis.gateway.repository.RedisRateLimiterRepository;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HierarchicalRateLimiterServiceTest {

    @Mock
    private RedisRateLimiterRepository redisRepository;

    private Cache<String, LocalTokenLease> localCache;
    private MeterRegistry meterRegistry;
    private FallbackLocalRateLimiter fallbackLocalRateLimiter;
    private CircuitBreaker circuitBreaker;
    private HierarchicalRateLimiterService service;

    @BeforeEach
    void setUp() {
        localCache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(2))
                .maximumSize(1000)
                .build();
        meterRegistry = new SimpleMeterRegistry();
        fallbackLocalRateLimiter = new FallbackLocalRateLimiter();
        circuitBreaker = CircuitBreaker.ofDefaults("redisRateLimiter");
        service = new HierarchicalRateLimiterService(
                localCache,
                redisRepository,
                circuitBreaker,
                fallbackLocalRateLimiter,
                meterRegistry
        );
    }

    @Test
    @DisplayName("Should serve from L1 cache when valid lease has available tokens")
    void shouldServeFromL1Cache() {
        String key = "tenant-fast";
        long expiry = System.currentTimeMillis() + 5000L;
        localCache.put(key, new LocalTokenLease(10L, expiry, 100L));

        StepVerifier.create(service.isAllowed(key, 100L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getSource()).isEqualTo(HierarchicalRateLimiterService.SOURCE_L1_CACHE);
                    assertThat(result.getRemainingTokens()).isEqualTo(9L);
                })
                .verifyComplete();

        verifyNoInteractions(redisRepository);

        double hits = meterRegistry.get("gateway.rate_limiter.l1.hits").counter().count();
        double misses = meterRegistry.get("gateway.rate_limiter.l1.misses").counter().count();
        assertThat(hits).isEqualTo(1.0);
        assertThat(misses).isEqualTo(0.0);
    }

    @Test
    @DisplayName("Should call Redis on L1 cache miss and populate L1 with micro-batch slice")
    void shouldCallRedisOnL1MissAndPopulateCache() {
        String key = "tenant-cold";
        when(redisRepository.checkRateLimit(eq(key), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(RateLimitResult.allowed(90L, 100L, "REDIS_SLIDING_WINDOW")));

        // 1st request: L1 miss -> calls Redis -> populates L1
        StepVerifier.create(service.isAllowed(key, 100L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getSource()).isEqualTo("REDIS_SLIDING_WINDOW");
                })
                .verifyComplete();

        verify(redisRepository, times(1)).checkRateLimit(eq(key), anyLong(), anyLong(), anyLong());

        double misses = meterRegistry.get("gateway.rate_limiter.l1.misses").counter().count();
        assertThat(misses).isEqualTo(1.0);

        // 2nd request: Should hit L1 cache directly!
        StepVerifier.create(service.isAllowed(key, 100L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getSource()).isEqualTo(HierarchicalRateLimiterService.SOURCE_L1_CACHE);
                })
                .verifyComplete();

        verify(redisRepository, times(1)).checkRateLimit(anyString(), anyLong(), anyLong(), anyLong());

        double hits = meterRegistry.get("gateway.rate_limiter.l1.hits").counter().count();
        assertThat(hits).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should return denied result and not populate L1 when Redis denies request")
    void shouldHandleRedisDenied() {
        String key = "tenant-blocked";
        when(redisRepository.checkRateLimit(eq(key), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(RateLimitResult.denied(5L, 60L, "REDIS_SLIDING_WINDOW")));

        StepVerifier.create(service.isAllowed(key, 60L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isFalse();
                    assertThat(result.getRetryAfterSeconds()).isEqualTo(5L);
                })
                .verifyComplete();

        assertThat(localCache.getIfPresent(key)).isNull();
    }

    @Test
    @DisplayName("Should route to FallbackLocalRateLimiter on Redis error without throwing 500")
    void shouldRouteToFallbackOnRedisError() {
        String key = "tenant-redis-failure";
        when(redisRepository.checkRateLimit(eq(key), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.error(new RuntimeException("Redis connection refused")));

        StepVerifier.create(service.isAllowed(key, 90L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getSource()).isEqualTo(FallbackLocalRateLimiter.SOURCE_FALLBACK_DEGRADED);
                    assertThat(result.getCurrentLimit()).isEqualTo(30L); // 1/3 of 90
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should route to FallbackLocalRateLimiter when circuit breaker is forced OPEN")
    void shouldRouteToFallbackWhenCircuitBreakerOpen() {
        String key = "tenant-circuit-open";
        circuitBreaker.transitionToOpenState();

        StepVerifier.create(service.isAllowed(key, 60L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getSource()).isEqualTo(FallbackLocalRateLimiter.SOURCE_FALLBACK_DEGRADED);
                    assertThat(result.getCurrentLimit()).isEqualTo(20L); // 1/3 of 60
                })
                .verifyComplete();

        // Redis repository was not even called because circuit breaker was OPEN
        verifyNoInteractions(redisRepository);
    }
}
