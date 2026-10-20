package com.aegis.gateway.filter;

import com.aegis.gateway.config.TenantPolicyProperties;
import com.aegis.gateway.model.TenantContext;
import com.aegis.gateway.model.TenantTier;
import com.aegis.gateway.service.TenantPolicyResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class TenantIdentificationFilterTest {

    private TenantPolicyProperties policyProperties;
    private TenantPolicyResolver policyResolver;
    private TenantIdentificationFilter filter;

    @BeforeEach
    void setUp() {
        policyProperties = new TenantPolicyProperties();
        policyProperties.init();
        policyResolver = new TenantPolicyResolver(policyProperties);
        filter = new TenantIdentificationFilter(policyResolver);
    }

    @Test
    @DisplayName("Should have filter order of -100")
    void shouldHaveCorrectFilterOrder() {
        assertThat(filter.getOrder()).isEqualTo(-100);
    }

    @Test
    @DisplayName("Should identify ENTERPRISE tenant from X-API-Key header")
    void shouldIdentifyEnterpriseTenantFromApiKeyHeader() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/orders")
                .header(TenantIdentificationFilter.HEADER_API_KEY, "key-ent-corp-001")
                .remoteAddress(new InetSocketAddress("10.0.0.1", 8080))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isTrue();

        TenantContext context = exchange.getAttribute(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT);
        assertThat(context).isNotNull();
        assertThat(context.getTenantId()).isEqualTo("key-ent-corp-001");
        assertThat(context.getApiKey()).isEqualTo("key-ent-corp-001");
        assertThat(context.getTier()).isEqualTo(TenantTier.ENTERPRISE);
        assertThat(context.getPolicy()).isNotNull();
        assertThat(context.getPolicy().getSustainedQuota()).isEqualTo(10000L);
        assertThat(context.getPolicy().getBurstQuota()).isEqualTo(500L);
        assertThat(context.getPolicy().isTrafficShapingEnabled()).isTrue();
        assertThat(context.getPolicy().getMaxQueueDelayMillis()).isEqualTo(250L);
    }

    @Test
    @DisplayName("Should identify PRO tenant from Authorization Bearer header")
    void shouldIdentifyProTenantFromBearerHeader() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/data")
                .header(HttpHeaders.AUTHORIZATION, "Bearer key-pro-client-77")
                .remoteAddress(new InetSocketAddress("10.0.0.2", 8080))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isTrue();

        TenantContext context = exchange.getAttribute(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT);
        assertThat(context).isNotNull();
        assertThat(context.getTenantId()).isEqualTo("key-pro-client-77");
        assertThat(context.getApiKey()).isEqualTo("key-pro-client-77");
        assertThat(context.getTier()).isEqualTo(TenantTier.PRO);
        assertThat(context.getPolicy()).isNotNull();
        assertThat(context.getPolicy().getSustainedQuota()).isEqualTo(1000L);
        assertThat(context.getPolicy().getBurstQuota()).isEqualTo(50L);
        assertThat(context.getPolicy().isTrafficShapingEnabled()).isFalse();
    }

    @Test
    @DisplayName("Should identify FREE tenant from X-API-Key header")
    void shouldIdentifyFreeTenantFromApiKeyHeader() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/public")
                .header(TenantIdentificationFilter.HEADER_API_KEY, "key-free-dev-01")
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isTrue();

        TenantContext context = exchange.getAttribute(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT);
        assertThat(context).isNotNull();
        assertThat(context.getTenantId()).isEqualTo("key-free-dev-01");
        assertThat(context.getTier()).isEqualTo(TenantTier.FREE);
        assertThat(context.getPolicy()).isNotNull();
        assertThat(context.getPolicy().getSustainedQuota()).isEqualTo(60L);
        assertThat(context.getPolicy().getBurstQuota()).isEqualTo(0L);
        assertThat(context.getPolicy().isTrafficShapingEnabled()).isFalse();
    }

    @Test
    @DisplayName("Should fallback to client IP with FREE tier when no credentials provided")
    void shouldFallbackToClientIpWhenNoCredentialsProvided() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/anonymous")
                .remoteAddress(new InetSocketAddress("192.168.1.150", 9999))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isTrue();

        TenantContext context = exchange.getAttribute(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT);
        assertThat(context).isNotNull();
        assertThat(context.getTenantId()).isEqualTo("ip:192.168.1.150");
        assertThat(context.getApiKey()).isNull();
        assertThat(context.getClientIp()).isEqualTo("192.168.1.150");
        assertThat(context.getTier()).isEqualTo(TenantTier.FREE);
        assertThat(context.getPolicy()).isNotNull();
        assertThat(context.getPolicy().getSustainedQuota()).isEqualTo(60L);
    }

    @Test
    @DisplayName("Should extract client IP from X-Forwarded-For header when present")
    void shouldExtractClientIpFromXForwardedForHeader() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/anonymous")
                .header("X-Forwarded-For", "203.0.113.195, 70.41.3.18, 150.172.238.178")
                .remoteAddress(new InetSocketAddress("10.0.0.1", 8080))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isTrue();

        TenantContext context = exchange.getAttribute(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT);
        assertThat(context).isNotNull();
        assertThat(context.getClientIp()).isEqualTo("203.0.113.195");
        assertThat(context.getTenantId()).isEqualTo("ip:203.0.113.195");
        assertThat(context.getTier()).isEqualTo(TenantTier.FREE);
    }

    @Test
    @DisplayName("Should return HTTP 401 when API key prefix is invalid")
    void shouldReturn401WhenApiKeyPrefixIsInvalid() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/secure")
                .header(TenantIdentificationFilter.HEADER_API_KEY, "invalid-key-prefix-123")
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(exchange.getResponse().getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
    }

    @Test
    @DisplayName("Should return HTTP 401 when X-API-Key is empty")
    void shouldReturn401WhenApiKeyIsEmpty() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/secure")
                .header(TenantIdentificationFilter.HEADER_API_KEY, "   ")
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(exchange.getResponse().getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
    }

    @Test
    @DisplayName("Should return HTTP 401 when Authorization header scheme is not Bearer")
    void shouldReturn401WhenAuthHeaderIsNotBearer() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/secure")
                .header(HttpHeaders.AUTHORIZATION, "Basic dXNlcjpwYXNz")
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("Should return HTTP 401 when Bearer token is empty")
    void shouldReturn401WhenBearerTokenIsEmpty() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/secure")
                .header(HttpHeaders.AUTHORIZATION, "Bearer    ")
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}
