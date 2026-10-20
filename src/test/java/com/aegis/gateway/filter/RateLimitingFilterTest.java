package com.aegis.gateway.filter;

import com.aegis.gateway.metrics.GatewayMetricsCollector;
import com.aegis.gateway.model.RateLimitResult;
import com.aegis.gateway.model.TenantContext;
import com.aegis.gateway.model.TenantPolicy;
import com.aegis.gateway.model.TenantTier;
import com.aegis.gateway.service.HierarchicalRateLimiterService;
import com.aegis.gateway.service.TrafficShaperService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RateLimitingFilterTest {

    @Mock
    private HierarchicalRateLimiterService rateLimiterService;

    @Mock
    private TrafficShaperService trafficShaperService;

    private MeterRegistry meterRegistry;
    private GatewayMetricsCollector metricsCollector;
    private RateLimitingFilter filter;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        metricsCollector = new GatewayMetricsCollector(meterRegistry);
        filter = new RateLimitingFilter(rateLimiterService, trafficShaperService, metricsCollector);
    }

    @Test
    @DisplayName("Should have filter order of 0")
    void shouldHaveCorrectFilterOrder() {
        assertThat(filter.getOrder()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should allow request, populate RFC 6585 headers, and record 200 metrics when rate limit permits")
    void shouldAllowRequestWhenPermitted() {
        TenantPolicy policy = TenantPolicy.builder()
                .tier(TenantTier.PRO)
                .sustainedQuota(1000L)
                .burstQuota(50L)
                .windowSizeMillis(60000L)
                .build();
        TenantContext tenantContext = TenantContext.builder()
                .tenantId("key-pro-tenant-01")
                .tier(TenantTier.PRO)
                .policy(policy)
                .build();

        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/orders").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);
        exchange.getAttributes().put(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT, tenantContext);

        RateLimitResult allowedResult = RateLimitResult.allowed(42L, 1050L, "L1_CACHE");
        when(rateLimiterService.evaluate(eq(tenantContext))).thenReturn(Mono.just(allowedResult));
        when(trafficShaperService.shape(eq(tenantContext), eq(allowedResult))).thenReturn(Mono.just(allowedResult));

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isTrue();
        HttpHeaders headers = exchange.getResponse().getHeaders();
        assertThat(headers.getFirst(RateLimitingFilter.HEADER_LIMIT)).isEqualTo("1050");
        assertThat(headers.getFirst(RateLimitingFilter.HEADER_REMAINING)).isEqualTo("42");
        assertThat(headers.getFirst(RateLimitingFilter.HEADER_SOURCE)).isEqualTo("L1_CACHE");
        assertThat(headers.getFirst(RateLimitingFilter.HEADER_RESET)).isNotNull();

        // Verify metrics
        assertThat(metricsCollector.getDecisionTimer().count()).isEqualTo(1L);
        Counter requestCounter = meterRegistry.find(GatewayMetricsCollector.METRIC_REQUESTS_TOTAL)
                .tag("tier", "PRO")
                .tag("status", "200")
                .tag("source", "L1_CACHE")
                .counter();
        assertThat(requestCounter).isNotNull();
        assertThat(requestCounter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should allow shaped request with source SHAPED_BUFFER and record metrics")
    void shouldAllowShapedRequest() {
        TenantPolicy policy = TenantPolicy.builder()
                .tier(TenantTier.ENTERPRISE)
                .sustainedQuota(10000L)
                .trafficShapingEnabled(true)
                .maxQueueDelayMillis(250L)
                .build();
        TenantContext tenantContext = TenantContext.builder()
                .tenantId("key-ent-corp")
                .tier(TenantTier.ENTERPRISE)
                .policy(policy)
                .build();

        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/data").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);
        exchange.getAttributes().put(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT, tenantContext);

        RateLimitResult deniedResult = RateLimitResult.denied(1L, 100L, 10000L, "REDIS");
        RateLimitResult shapedResult = RateLimitResult.builder()
                .allowed(true)
                .remainingTokens(0L)
                .currentLimit(10000L)
                .source(TrafficShaperService.SOURCE_SHAPED_BUFFER)
                .build();

        when(rateLimiterService.evaluate(eq(tenantContext))).thenReturn(Mono.just(deniedResult));
        when(trafficShaperService.shape(eq(tenantContext), eq(deniedResult))).thenReturn(Mono.just(shapedResult));

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isTrue();
        HttpHeaders headers = exchange.getResponse().getHeaders();
        assertThat(headers.getFirst(RateLimitingFilter.HEADER_SOURCE)).isEqualTo("SHAPED_BUFFER");
        assertThat(headers.getFirst(RateLimitingFilter.HEADER_LIMIT)).isEqualTo("10000");

        Counter shapedCounter = meterRegistry.find(GatewayMetricsCollector.METRIC_REQUESTS_TOTAL)
                .tag("tier", "ENTERPRISE")
                .tag("status", "200")
                .tag("source", "SHAPED_BUFFER")
                .counter();
        assertThat(shapedCounter).isNotNull();
        assertThat(shapedCounter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should return HTTP 429 with RFC 7807 problem details and record 429 metrics when exceeded")
    void shouldRejectWith429WhenRateLimitExceeded() {
        TenantPolicy policy = TenantPolicy.builder()
                .tier(TenantTier.FREE)
                .sustainedQuota(60L)
                .burstQuota(0L)
                .windowSizeMillis(60000L)
                .build();
        TenantContext tenantContext = TenantContext.builder()
                .tenantId("ip:192.168.1.1")
                .tier(TenantTier.FREE)
                .policy(policy)
                .build();

        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/resource").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);
        exchange.getAttributes().put(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT, tenantContext);

        RateLimitResult deniedResult = RateLimitResult.denied(4L, 4000L, 60L, "REDIS_SLIDING_WINDOW");
        when(rateLimiterService.evaluate(eq(tenantContext))).thenReturn(Mono.just(deniedResult));
        when(trafficShaperService.shape(eq(tenantContext), eq(deniedResult))).thenReturn(Mono.just(deniedResult));

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(exchange.getResponse().getHeaders().getContentType()).isEqualTo(RateLimitingFilter.PROBLEM_JSON_MEDIA_TYPE);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("4");
        assertThat(exchange.getResponse().getHeaders().getFirst(RateLimitingFilter.HEADER_LIMIT)).isEqualTo("60");
        assertThat(exchange.getResponse().getHeaders().getFirst(RateLimitingFilter.HEADER_REMAINING)).isEqualTo("0");

        Counter rejectedCounter = meterRegistry.find(GatewayMetricsCollector.METRIC_REQUESTS_TOTAL)
                .tag("tier", "FREE")
                .tag("status", "429")
                .tag("source", "REDIS_SLIDING_WINDOW")
                .counter();
        assertThat(rejectedCounter).isNotNull();
        assertThat(rejectedCounter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should bypass rate limiting if TenantContext is missing from attributes")
    void shouldBypassWhenContextMissing() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/v1/health").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        AtomicBoolean chainExecuted = new AtomicBoolean(false);
        GatewayFilterChain chain = ex -> {
            chainExecuted.set(true);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(chainExecuted.get()).isTrue();
    }
}
