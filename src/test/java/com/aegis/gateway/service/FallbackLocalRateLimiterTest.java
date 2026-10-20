package com.aegis.gateway.service;

import com.aegis.gateway.model.RateLimitResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class FallbackLocalRateLimiterTest {

    private FallbackLocalRateLimiter fallbackLimiter;

    @BeforeEach
    void setUp() {
        fallbackLimiter = new FallbackLocalRateLimiter();
    }

    @Test
    @DisplayName("Should allow requests within conservative 1/3 limit with degraded source")
    void shouldAllowWithinConservativeLimit() {
        String key = "tenant-fallback-01";
        long nominalLimit = 90L; // 1/3 limit is 30

        StepVerifier.create(fallbackLimiter.isAllowed(key, nominalLimit, 60000L, 10L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getCurrentLimit()).isEqualTo(30L);
                    assertThat(result.getRemainingTokens()).isEqualTo(20L);
                    assertThat(result.getSource()).isEqualTo(FallbackLocalRateLimiter.SOURCE_FALLBACK_DEGRADED);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should reject requests exceeding conservative 1/3 limit with degraded source")
    void shouldRejectWhenConservativeLimitExceeded() {
        String key = "tenant-fallback-02";
        long nominalLimit = 60L; // 1/3 limit is 20

        // Consume 20 tokens
        StepVerifier.create(fallbackLimiter.isAllowed(key, nominalLimit, 60000L, 20L))
                .assertNext(result -> assertThat(result.isAllowed()).isTrue())
                .verifyComplete();

        // 21st token should be rejected
        StepVerifier.create(fallbackLimiter.isAllowed(key, nominalLimit, 60000L, 1L))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isFalse();
                    assertThat(result.getCurrentLimit()).isEqualTo(20L);
                    assertThat(result.getRemainingTokens()).isEqualTo(0L);
                    assertThat(result.getRetryAfterSeconds()).isGreaterThan(0L);
                    assertThat(result.getSource()).isEqualTo(FallbackLocalRateLimiter.SOURCE_FALLBACK_DEGRADED);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should evaluate fallback synchronously using TenantContext")
    void shouldEvaluateFallbackWithContext() {
        com.aegis.gateway.model.TenantPolicy policy = com.aegis.gateway.model.TenantPolicy.builder()
                .sustainedQuota(300L)
                .burstQuota(0L)
                .windowSizeMillis(60000L)
                .build();
        com.aegis.gateway.model.TenantContext context = com.aegis.gateway.model.TenantContext.builder()
                .tenantId("tenant-ctx-test")
                .policy(policy)
                .build();

        RateLimitResult result = fallbackLimiter.evaluateFallback(context);
        assertThat(result.isAllowed()).isTrue();
        assertThat(result.getCurrentLimit()).isEqualTo(100L); // 1/3 of 300
        assertThat(result.getSource()).isEqualTo(FallbackLocalRateLimiter.SOURCE_FALLBACK_DEGRADED);
    }
}
