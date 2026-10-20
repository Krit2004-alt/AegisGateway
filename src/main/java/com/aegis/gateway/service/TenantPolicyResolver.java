package com.aegis.gateway.service;

import com.aegis.gateway.config.TenantPolicyProperties;
import com.aegis.gateway.exception.UnauthorizedException;
import com.aegis.gateway.model.TenantContext;
import com.aegis.gateway.model.TenantPolicy;
import com.aegis.gateway.model.TenantTier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Service responsible for resolving tenant tier, policy, and identity context
 * based on API key prefix or falling back to client IP.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TenantPolicyResolver {

    public static final String PREFIX_ENTERPRISE = "key-ent-";
    public static final String PREFIX_PRO = "key-pro-";
    public static final String PREFIX_FREE = "key-free-";

    private final TenantPolicyProperties policyProperties;

    /**
     * Resolves TenantContext based on the provided API key or falls back to client IP.
     *
     * @param apiKey   API key from request (optional)
     * @param clientIp client IP address (optional fallback)
     * @return Mono emitting the resolved TenantContext, or Mono.error(UnauthorizedException) if the API key is invalid
     */
    public Mono<TenantContext> resolve(String apiKey, String clientIp) {
        if (apiKey != null && !apiKey.isBlank()) {
            String trimmedKey = apiKey.trim();
            TenantTier tier;

            if (trimmedKey.startsWith(PREFIX_ENTERPRISE)) {
                tier = TenantTier.ENTERPRISE;
            } else if (trimmedKey.startsWith(PREFIX_PRO)) {
                tier = TenantTier.PRO;
            } else if (trimmedKey.startsWith(PREFIX_FREE)) {
                tier = TenantTier.FREE;
            } else {
                log.warn("Unauthorized API key pattern encountered: {}", maskKey(trimmedKey));
                return Mono.error(new UnauthorizedException("Invalid API key: unrecognized tenant tier"));
            }

            TenantPolicy policy = policyProperties.getPolicy(tier);
            TenantContext context = TenantContext.builder()
                    .tenantId(trimmedKey)
                    .apiKey(trimmedKey)
                    .tier(tier)
                    .policy(policy)
                    .clientIp(clientIp)
                    .build();

            log.debug("Resolved tenant '{}' with tier '{}'", context.getTenantId(), tier);
            return Mono.just(context);
        }

        // Anonymous fallback using client IP
        String effectiveIp = (clientIp != null && !clientIp.isBlank()) ? clientIp.trim() : "127.0.0.1";
        String tenantId = "ip:" + effectiveIp;
        TenantTier tier = TenantTier.FREE;
        TenantPolicy policy = policyProperties.getPolicy(tier);

        TenantContext context = TenantContext.builder()
                .tenantId(tenantId)
                .apiKey(null)
                .tier(tier)
                .policy(policy)
                .clientIp(effectiveIp)
                .build();

        log.debug("Resolved anonymous tenant '{}' for IP '{}' with default FREE tier", tenantId, effectiveIp);
        return Mono.just(context);
    }

    private String maskKey(String key) {
        if (key.length() <= 8) {
            return "****";
        }
        return key.substring(0, 6) + "..." + key.substring(key.length() - 2);
    }
}
