package com.aegis.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Context object carrying tenant identity, tier, policy, and network origin across the gateway filter chain.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TenantContext {

    private String tenantId;
    private String apiKey;
    private TenantTier tier;
    private TenantPolicy policy;
    private String clientIp;
}
