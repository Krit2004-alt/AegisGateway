package com.aegis.gateway.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayMetricsCollectorTest {

    private MeterRegistry meterRegistry;
    private GatewayMetricsCollector collector;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        collector = new GatewayMetricsCollector(meterRegistry);
    }

    @Test
    @DisplayName("Should record decision latency into Micrometer Timer")
    void shouldRecordDecisionLatency() {
        collector.recordDecisionLatency(15_000_000L); // 15 ms

        Timer timer = collector.getDecisionTimer();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(14.0);
    }

    @Test
    @DisplayName("Should record dimensional request counters with tier, status, and source tags")
    void shouldRecordRequestsTotalWithTags() {
        collector.recordRequest("ENTERPRISE", "200", "L1_CACHE");
        collector.recordRequest("ENTERPRISE", "200", "L1_CACHE");
        collector.recordRequest("PRO", "429", "REDIS_SLIDING_WINDOW");

        Counter ent200Counter = meterRegistry.find(GatewayMetricsCollector.METRIC_REQUESTS_TOTAL)
                .tag("tier", "ENTERPRISE")
                .tag("status", "200")
                .tag("source", "L1_CACHE")
                .counter();

        Counter pro429Counter = meterRegistry.find(GatewayMetricsCollector.METRIC_REQUESTS_TOTAL)
                .tag("tier", "PRO")
                .tag("status", "429")
                .tag("source", "REDIS_SLIDING_WINDOW")
                .counter();

        assertThat(ent200Counter).isNotNull();
        assertThat(ent200Counter.count()).isEqualTo(2.0);

        assertThat(pro429Counter).isNotNull();
        assertThat(pro429Counter.count()).isEqualTo(1.0);
    }
}
