package com.aegis.gateway.service;

import com.aegis.gateway.config.TenantPolicyProperties;
import com.aegis.gateway.exception.UnauthorizedException;
import com.aegis.gateway.model.TenantContext;
import com.aegis.gateway.model.TenantTier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class TenantPolicyResolverTest {

    private TenantPolicyProperties policyProperties;
    private TenantPolicyResolver resolver;

    @BeforeEach
    void setUp() {
        policyProperties = new TenantPolicyProperties();
        policyProperties.init();
        resolver = new TenantPolicyResolver(policyProperties);
    }

    @Test
    @DisplayName("Should resolve ENTERPRISE tier when API key starts with key-ent-")
    void shouldResolveEnterprise() {
        StepVerifier.create(resolver.resolve("key-ent-acme-123", "10.0.0.1"))
                .assertNext(context -> {
                    assertThat(context.getTier()).isEqualTo(TenantTier.ENTERPRISE);
                    assertThat(context.getTenantId()).isEqualTo("key-ent-acme-123");
                    assertThat(context.getPolicy().getSustainedQuota()).isEqualTo(10000L);
                    assertThat(context.getPolicy().getBurstQuota()).isEqualTo(500L);
                    assertThat(context.getPolicy().isTrafficShapingEnabled()).isTrue();
                    assertThat(context.getPolicy().getMaxQueueDelayMillis()).isEqualTo(250L);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should resolve PRO tier when API key starts with key-pro-")
    void shouldResolvePro() {
        StepVerifier.create(resolver.resolve("key-pro-startup-456", "10.0.0.2"))
                .assertNext(context -> {
                    assertThat(context.getTier()).isEqualTo(TenantTier.PRO);
                    assertThat(context.getTenantId()).isEqualTo("key-pro-startup-456");
                    assertThat(context.getPolicy().getSustainedQuota()).isEqualTo(1000L);
                    assertThat(context.getPolicy().getBurstQuota()).isEqualTo(50L);
                    assertThat(context.getPolicy().isTrafficShapingEnabled()).isFalse();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should resolve FREE tier when API key starts with key-free-")
    void shouldResolveFreeWithKey() {
        StepVerifier.create(resolver.resolve("key-free-indie-789", "10.0.0.3"))
                .assertNext(context -> {
                    assertThat(context.getTier()).isEqualTo(TenantTier.FREE);
                    assertThat(context.getTenantId()).isEqualTo("key-free-indie-789");
                    assertThat(context.getPolicy().getSustainedQuota()).isEqualTo(60L);
                    assertThat(context.getPolicy().getBurstQuota()).isEqualTo(0L);
                    assertThat(context.getPolicy().isTrafficShapingEnabled()).isFalse();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should fallback to client IP and FREE tier when API key is null")
    void shouldFallbackToIpWhenApiKeyIsNull() {
        StepVerifier.create(resolver.resolve(null, "192.168.1.1"))
                .assertNext(context -> {
                    assertThat(context.getTier()).isEqualTo(TenantTier.FREE);
                    assertThat(context.getTenantId()).isEqualTo("ip:192.168.1.1");
                    assertThat(context.getClientIp()).isEqualTo("192.168.1.1");
                    assertThat(context.getApiKey()).isNull();
                    assertThat(context.getPolicy().getSustainedQuota()).isEqualTo(60L);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should emit UnauthorizedException when API key has unrecognized prefix")
    void shouldEmitUnauthorizedExceptionWhenInvalidPrefix() {
        StepVerifier.create(resolver.resolve("key-custom-invalid", "10.0.0.4"))
                .expectError(UnauthorizedException.class)
                .verify();
    }
}
