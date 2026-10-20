package com.aegis.gateway.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Central metrics collector for AegisGateway observability.
 * Captures rate limit decision latencies (P50, P95, P99 + SLA buckets)
 * and dimensional request throughput counters tagged by tier, status, and source.
 */
@Component
public class GatewayMetricsCollector {

    public static final String METRIC_DECISION_LATENCY = "gateway.decision.latency";
    public static final String METRIC_REQUESTS_TOTAL = "gateway.requests.total";

    private final MeterRegistry meterRegistry;
    private final Timer decisionTimer;

    public GatewayMetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.decisionTimer = Timer.builder(METRIC_DECISION_LATENCY)
                .description("Latency distribution of rate limiting decisions across L1/L2 and shaping")
                .publishPercentiles(0.50, 0.95, 0.99)
                .sla(
                        Duration.ofMillis(1),
                        Duration.ofMillis(2),
                        Duration.ofMillis(5),
                        Duration.ofMillis(10),
                        Duration.ofMillis(25),
                        Duration.ofMillis(50),
                        Duration.ofMillis(100),
                        Duration.ofMillis(250)
                )
                .register(meterRegistry);
    }

    /**
     * Records the duration of a rate limit decision evaluation in nanoseconds.
     *
     * @param durationNanos elapsed nanoseconds
     */
    public void recordDecisionLatency(long durationNanos) {
        decisionTimer.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Increments the request counter tagged by tier, HTTP status code, and evaluation source.
     *
     * @param tier   tenant tier (FREE, PRO, ENTERPRISE, ANONYMOUS)
     * @param status HTTP response status code (e.g. "200", "429")
     * @param source decision source (L1_CACHE, REDIS_SLIDING_WINDOW, SHAPED_BUFFER, LOCAL_FALLBACK_DEGRADED)
     */
    public void recordRequest(String tier, String status, String source) {
        Counter.builder(METRIC_REQUESTS_TOTAL)
                .description("Total number of gateway requests processed through rate limiting")
                .tag("tier", tier != null ? tier : "UNKNOWN")
                .tag("status", status != null ? status : "UNKNOWN")
                .tag("source", source != null ? source : "UNKNOWN")
                .register(meterRegistry)
                .increment();
    }

    public Timer getDecisionTimer() {
        return decisionTimer;
    }
}
