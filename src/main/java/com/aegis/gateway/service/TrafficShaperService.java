package com.aegis.gateway.service;

import com.aegis.gateway.model.RateLimitResult;
import com.aegis.gateway.model.TenantContext;
import com.aegis.gateway.model.TenantPolicy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Service responsible for traffic shaping and micro-queuing of requests for tenants
 * with traffic shaping enabled (e.g. ENTERPRISE tier).
 * Smooths traffic spikes non-blockingly using Mono.delay() up to the tenant's maxQueueDelayMillis.
 */
@Slf4j
@Service
public class TrafficShaperService {

    public static final String SOURCE_SHAPED_BUFFER = "SHAPED_BUFFER";

    /**
     * Evaluates and applies non-blocking traffic shaping if enabled for the tenant policy.
     *
     * @param context the tenant context containing policy and configuration
     * @param result  the initial rate limit evaluation result from L1/L2
     * @return Mono emitting either the shaped allowed result with source SHAPED_BUFFER or the original result
     */
    public Mono<RateLimitResult> shape(TenantContext context, RateLimitResult result) {
        if (result.isAllowed()) {
            return Mono.just(result);
        }

        if (context == null || context.getPolicy() == null) {
            return Mono.just(result);
        }

        TenantPolicy policy = context.getPolicy();
        if (!policy.isTrafficShapingEnabled() || policy.getMaxQueueDelayMillis() <= 0) {
            log.trace("Traffic shaping disabled for tenant '{}'", context.getTenantId());
            return Mono.just(result);
        }

        long maxQueueDelay = policy.getMaxQueueDelayMillis();
        long requestedDelay = result.getRetryAfterMillis();
        if (requestedDelay <= 0 && result.getRetryAfterSeconds() > 0) {
            requestedDelay = result.getRetryAfterSeconds() * 1000L;
        }

        long delayMs = (requestedDelay > 0) ? Math.min(requestedDelay, maxQueueDelay) : maxQueueDelay;
        if (delayMs <= 0) {
            delayMs = 1L;
        }

        log.debug("Shaping traffic for tenant '{}': delaying request non-blockingly for {} ms (max SLA: {} ms)",
                context.getTenantId(), delayMs, maxQueueDelay);

        return Mono.delay(Duration.ofMillis(delayMs))
                .map(tick -> RateLimitResult.builder()
                        .allowed(true)
                        .remainingTokens(0L)
                        .retryAfterSeconds(0L)
                        .retryAfterMillis(0L)
                        .currentLimit(result.getCurrentLimit())
                        .source(SOURCE_SHAPED_BUFFER)
                        .build()
                );
    }
}
