package com.aegis.gateway.config;

import com.aegis.gateway.model.TenantPolicy;
import com.aegis.gateway.model.TenantTier;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties mapped to 'aegis.policies' defining rate limits
 * and traffic shaping policies for each tenant tier.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "aegis.policies")
public class TenantPolicyProperties {

    /**
     * Default policy for FREE tier: 60 req/min, burst 0, shaping false.
     */
    private TenantPolicy free = TenantPolicy.builder()
            .tier(TenantTier.FREE)
            .sustainedQuota(60L)
            .burstQuota(0L)
            .windowSizeMillis(60000L)
            .trafficShapingEnabled(false)
            .maxQueueDelayMillis(0L)
            .build();

    /**
     * Default policy for PRO tier: 1000 req/min, burst 50, shaping false.
     */
    private TenantPolicy pro = TenantPolicy.builder()
            .tier(TenantTier.PRO)
            .sustainedQuota(1000L)
            .burstQuota(50L)
            .windowSizeMillis(60000L)
            .trafficShapingEnabled(false)
            .maxQueueDelayMillis(0L)
            .build();

    /**
     * Default policy for ENTERPRISE tier: 10000 req/min, burst 500, shaping true, maxDelay 250ms.
     */
    private TenantPolicy enterprise = TenantPolicy.builder()
            .tier(TenantTier.ENTERPRISE)
            .sustainedQuota(10000L)
            .burstQuota(500L)
            .windowSizeMillis(60000L)
            .trafficShapingEnabled(true)
            .maxQueueDelayMillis(250L)
            .build();

    @PostConstruct
    public void init() {
        if (free != null && free.getTier() == null) {
            free.setTier(TenantTier.FREE);
        }
        if (pro != null && pro.getTier() == null) {
            pro.setTier(TenantTier.PRO);
        }
        if (enterprise != null && enterprise.getTier() == null) {
            enterprise.setTier(TenantTier.ENTERPRISE);
        }
    }

    /**
     * Resolves the TenantPolicy associated with the specified TenantTier.
     *
     * @param tier the tenant tier
     * @return the resolved TenantPolicy, defaulting to FREE if null
     */
    public TenantPolicy getPolicy(TenantTier tier) {
        if (tier == null) {
            return free;
        }
        return switch (tier) {
            case FREE -> free;
            case PRO -> pro;
            case ENTERPRISE -> enterprise;
        };
    }
}
