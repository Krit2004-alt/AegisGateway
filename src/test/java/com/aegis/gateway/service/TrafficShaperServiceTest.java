package com.aegis.gateway.service;

import com.aegis.gateway.model.RateLimitResult;
import com.aegis.gateway.model.TenantContext;
import com.aegis.gateway.model.TenantPolicy;
import com.aegis.gateway.model.TenantTier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TrafficShaperServiceTest {

    private TrafficShaperService trafficShaperService;

    @BeforeEach
    void setUp() {
        trafficShaperService = new TrafficShaperService();
    }

    @Test
    @DisplayName("Should return original result if request was already allowed")
    void shouldReturnOriginalWhenAllowed() {
        TenantPolicy policy = TenantPolicy.builder()
                .trafficShapingEnabled(true)
                .maxQueueDelayMillis(250L)
                .build();
        TenantContext context = TenantContext.builder().policy(policy).build();
        RateLimitResult allowed = RateLimitResult.allowed(10L, 100L, "L1_CACHE");

        StepVerifier.create(trafficShaperService.shape(context, allowed))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getSource()).isEqualTo("L1_CACHE");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should return original denied result if traffic shaping is disabled")
    void shouldReturnDeniedWhenShapingDisabled() {
        TenantPolicy policy = TenantPolicy.builder()
                .trafficShapingEnabled(false)
                .maxQueueDelayMillis(250L)
                .build();
        TenantContext context = TenantContext.builder().policy(policy).build();
        RateLimitResult denied = RateLimitResult.denied(2L, 50L, 100L, "REDIS");

        StepVerifier.create(trafficShaperService.shape(context, denied))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isFalse();
                    assertThat(result.getSource()).isEqualTo("REDIS");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should delay and allow request with source SHAPED_BUFFER when shaping is enabled")
    void shouldShapeRequestWhenShapingEnabled() {
        TenantPolicy policy = TenantPolicy.builder()
                .tier(TenantTier.ENTERPRISE)
                .trafficShapingEnabled(true)
                .maxQueueDelayMillis(250L)
                .build();
        TenantContext context = TenantContext.builder()
                .tenantId("key-ent-acme")
                .policy(policy)
                .build();
        RateLimitResult denied = RateLimitResult.denied(1L, 50L, 10000L, "REDIS");

        StepVerifier.withVirtualTime(() -> trafficShaperService.shape(context, denied))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(40))
                .thenAwait(Duration.ofMillis(50))
                .assertNext(result -> {
                    assertThat(result.isAllowed()).isTrue();
                    assertThat(result.getSource()).isEqualTo(TrafficShaperService.SOURCE_SHAPED_BUFFER);
                    assertThat(result.getRemainingTokens()).isEqualTo(0L);
                    assertThat(result.getCurrentLimit()).isEqualTo(10000L);
                })
                .verifyComplete();
    }
}
