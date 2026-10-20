package com.aegis.gateway.repository;

import com.aegis.gateway.model.RateLimitResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisRateLimiterRepositoryTest {

    @Mock
    private ReactiveStringRedisTemplate redisTemplate;

    @Mock
    @SuppressWarnings("rawtypes")
    private RedisScript<List> slidingWindowRedisScript;

    private RedisRateLimiterRepository repository;

    @BeforeEach
    void setUp() {
        repository = new RedisRateLimiterRepository(redisTemplate, slidingWindowRedisScript);
    }

    @Test
    @DisplayName("Should return allowed result when Lua script indicates success")
    void shouldReturnAllowedWhenWithinLimit() {
        List<Object> scriptOutput = List.of(1L, 42L, 0L);
        when(redisTemplate.execute(any(), anyList(), anyList()))
                .thenReturn(Flux.just(scriptOutput));

        StepVerifier.create(repository.isAllowed("tenant-pro", 1000L, 60000L, 1L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getRemainingTokens()).isEqualTo(42L);
                    assertThat(result.getRetryAfterSeconds()).isEqualTo(0L);
                    assertThat(result.getCurrentLimit()).isEqualTo(1000L);
                    assertThat(result.getSource()).isEqualTo(RedisRateLimiterRepository.SOURCE_REDIS);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should return denied result with rounded retryAfterSeconds when limit is exceeded")
    void shouldReturnDeniedWhenLimitExceeded() {
        List<Object> scriptOutput = List.of(0L, 0L, 2450L);
        when(redisTemplate.execute(any(), anyList(), anyList()))
                .thenReturn(Flux.just(scriptOutput));

        StepVerifier.create(repository.isAllowed("tenant-free", 60L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isFalse();
                    assertThat(result.getRemainingTokens()).isEqualTo(0L);
                    assertThat(result.getRetryAfterSeconds()).isEqualTo(3L); // ceil(2450 / 1000)
                    assertThat(result.getCurrentLimit()).isEqualTo(60L);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should safely parse diverse numeric and string types without ClassCastException")
    void shouldSafelyParseMixedTypesWithoutClassCastException() {
        // Simulating different driver representations: Integer, Long, String
        List<Object> scriptOutput = List.of(1, "88", 0L);
        when(redisTemplate.execute(any(), anyList(), anyList()))
                .thenReturn(Flux.just(scriptOutput));

        StepVerifier.create(repository.isAllowed("tenant-mixed", 100L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getRemainingTokens()).isEqualTo(88L);
                    assertThat(result.getRetryAfterSeconds()).isEqualTo(0L);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should fallback gracefully when Redis returns empty flux")
    void shouldFallbackGracefullyWhenFluxEmpty() {
        when(redisTemplate.execute(any(), anyList(), anyList()))
                .thenReturn(Flux.empty());

        StepVerifier.create(repository.isAllowed("tenant-empty", 500L, 60000L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getRemainingTokens()).isEqualTo(500L);
                })
                .verifyComplete();
    }
}
