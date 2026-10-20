package com.aegis.gateway.filter;

import com.aegis.gateway.metrics.GatewayMetricsCollector;
import com.aegis.gateway.model.RateLimitResult;
import com.aegis.gateway.model.TenantContext;
import com.aegis.gateway.service.HierarchicalRateLimiterService;
import com.aegis.gateway.service.TrafficShaperService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * Global rate limiting filter enforcing hierarchical caching (L1/L2),
 * traffic shaping, RFC 6585 telemetry headers, RFC 7807 HTTP 429 problem details,
 * and high-precision Micrometer metrics collection.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RateLimitingFilter implements GlobalFilter, Ordered {

    public static final String HEADER_LIMIT = "X-RateLimit-Limit";
    public static final String HEADER_REMAINING = "X-RateLimit-Remaining";
    public static final String HEADER_RESET = "X-RateLimit-Reset";
    public static final String HEADER_SOURCE = "X-RateLimit-Source";

    public static final MediaType PROBLEM_JSON_MEDIA_TYPE = MediaType.parseMediaType("application/problem+json");
    private static final int FILTER_ORDER = 0;

    private final HierarchicalRateLimiterService rateLimiterService;
    private final TrafficShaperService trafficShaperService;
    private final GatewayMetricsCollector metricsCollector;

    @Override
    public int getOrder() {
        return FILTER_ORDER;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        TenantContext tenantContext = exchange.getAttribute(TenantIdentificationFilter.AEGIS_TENANT_CONTEXT);
        if (tenantContext == null) {
            log.warn("Missing TenantContext in exchange attributes; bypassing rate limiting filter");
            return chain.filter(exchange);
        }

        long startNanos = System.nanoTime();

        return rateLimiterService.evaluate(tenantContext)
                .flatMap(result -> trafficShaperService.shape(tenantContext, result))
                .flatMap(result -> {
                    long durationNanos = System.nanoTime() - startNanos;
                    metricsCollector.recordDecisionLatency(durationNanos);

                    applyRateLimitHeaders(exchange, tenantContext, result);

                    String tier = (tenantContext.getTier() != null)
                            ? tenantContext.getTier().name()
                            : "UNKNOWN";
                    String source = (result.getSource() != null)
                            ? result.getSource()
                            : "UNKNOWN";

                    if (result.isAllowed()) {
                        metricsCollector.recordRequest(tier, "200", source);
                        return chain.filter(exchange);
                    }

                    metricsCollector.recordRequest(tier, "429", source);
                    return writeRateLimitExceededResponse(exchange, tenantContext, result);
                });
    }

    /**
     * Applies standard RFC 6585 rate limiting telemetry headers to the response.
     */
    protected void applyRateLimitHeaders(ServerWebExchange exchange, TenantContext tenantContext, RateLimitResult result) {
        HttpHeaders headers = exchange.getResponse().getHeaders();

        headers.set(HEADER_LIMIT, String.valueOf(result.getCurrentLimit()));
        headers.set(HEADER_REMAINING, String.valueOf(Math.max(0L, result.getRemainingTokens())));
        headers.set(HEADER_SOURCE, result.getSource() != null ? result.getSource() : "UNKNOWN");

        long windowSizeMs = (tenantContext.getPolicy() != null)
                ? tenantContext.getPolicy().getWindowSizeMillis()
                : 60000L;
        long secondsRemaining = Math.max(1L, (windowSizeMs - (System.currentTimeMillis() % windowSizeMs)) / 1000L);
        long resetSeconds = (result.getRetryAfterSeconds() > 0)
                ? result.getRetryAfterSeconds()
                : secondsRemaining;

        headers.set(HEADER_RESET, String.valueOf(resetSeconds));

        if (!result.isAllowed()) {
            headers.set(HttpHeaders.RETRY_AFTER, String.valueOf(Math.max(1L, result.getRetryAfterSeconds())));
        }
    }

    /**
     * Non-blockingly serializes and writes an RFC 7807 problem details JSON response for HTTP 429.
     */
    private Mono<Void> writeRateLimitExceededResponse(ServerWebExchange exchange, TenantContext tenantContext, RateLimitResult result) {
        ServerHttpResponse response = exchange.getResponse();
        if (response.isCommitted()) {
            return Mono.empty();
        }

        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        response.getHeaders().setContentType(PROBLEM_JSON_MEDIA_TYPE);

        String path = exchange.getRequest().getPath().value();
        long retryAfter = Math.max(1L, result.getRetryAfterSeconds());

        String tenantId = tenantContext.getTenantId() != null
                ? tenantContext.getTenantId().replace("\"", "\\\"")
                : "unknown";

        String jsonBody = String.format(
                "{\"type\":\"https://api.aegisgateway.com/errors/rate-limit-exceeded\"," +
                "\"title\":\"Too Many Requests\"," +
                "\"status\":429," +
                "\"detail\":\"Rate limit quota exceeded for tenant '%s'. Please retry after %d seconds.\"," +
                "\"instance\":\"%s\"}",
                tenantId,
                retryAfter,
                path.replace("\"", "\\\"")
        );

        byte[] bytes = jsonBody.getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }
}
