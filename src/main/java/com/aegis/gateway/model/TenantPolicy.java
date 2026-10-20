package com.aegis.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Encapsulates the rate limiting and traffic shaping policies for a tenant tier.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TenantPolicy {

    private TenantTier tier;
    private long sustainedQuota;
    private long burstQuota;

    @Builder.Default
    private long windowSizeMillis = 60000L;

    private boolean trafficShapingEnabled;
    private long maxQueueDelayMillis;
}
