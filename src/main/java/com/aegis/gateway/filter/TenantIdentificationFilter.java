package com.aegis.gateway.filter;

import com.aegis.gateway.exception.UnauthorizedException;
import com.aegis.gateway.service.TenantPolicyResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Global filter responsible for identifying tenant credentials (API key or Bearer token),
 * resolving tier policies, and attaching the TenantContext to the exchange attributes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TenantIdentificationFilter implements GlobalFilter, Ordered {

    public static final String AEGIS_TENANT_CONTEXT = "AEGIS_TENANT_CONTEXT";
    public static final String HEADER_API_KEY = "X-API-Key";
    public static final String BEARER_PREFIX = "Bearer ";
    private static final int FILTER_ORDER = -100;

    private final TenantPolicyResolver tenantPolicyResolver;

    @Override
    public int getOrder() {
        return FILTER_ORDER;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String apiKey;
        try {
            apiKey = extractApiKey(exchange.getRequest());
        } catch (UnauthorizedException ex) {
            log.warn("Authentication failed while extracting credentials: {}", ex.getMessage());
            return writeUnauthorizedResponse(exchange, ex.getMessage());
        }

        String clientIp = extractClientIp(exchange.getRequest());

        return tenantPolicyResolver.resolve(apiKey, clientIp)
                .flatMap(tenantContext -> {
                    exchange.getAttributes().put(AEGIS_TENANT_CONTEXT, tenantContext);
                    return chain.filter(exchange);
                })
                .onErrorResume(UnauthorizedException.class, ex -> {
                    log.warn("Tenant policy resolution failed: {}", ex.getMessage());
                    return writeUnauthorizedResponse(exchange, ex.getMessage());
                });
    }

    /**
     * Extracts the API key from either the 'X-API-Key' header or the 'Authorization: Bearer <token>' header.
     *
     * @param request the current reactive HTTP request
     * @return the extracted API key, or null if no authentication headers are present
     * @throws UnauthorizedException if an authentication header is provided but invalid or empty
     */
    protected String extractApiKey(ServerHttpRequest request) {
        HttpHeaders headers = request.getHeaders();

        // 1. Check X-API-Key header
        String apiKeyHeader = headers.getFirst(HEADER_API_KEY);
        if (apiKeyHeader != null) {
            String trimmedKey = apiKeyHeader.trim();
            if (trimmedKey.isEmpty()) {
                throw new UnauthorizedException("API key header '" + HEADER_API_KEY + "' cannot be empty");
            }
            return trimmedKey;
        }

        // 2. Check Authorization Bearer header
        String authHeader = headers.getFirst(HttpHeaders.AUTHORIZATION);
        if (authHeader != null) {
            String trimmedAuth = authHeader.trim();
            if (trimmedAuth.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length())) {
                String token = trimmedAuth.substring(BEARER_PREFIX.length()).trim();
                if (token.isEmpty()) {
                    throw new UnauthorizedException("Bearer token cannot be empty");
                }
                return token;
            } else {
                throw new UnauthorizedException("Invalid Authorization header scheme: Bearer token expected");
            }
        }

        // 3. Fallback: No credentials provided
        return null;
    }

    /**
     * Extracts the client IP address from X-Forwarded-For or remote address.
     *
     * @param request the current reactive HTTP request
     * @return the resolved client IP address
     */
    protected String extractClientIp(ServerHttpRequest request) {
        String xForwardedFor = request.getHeaders().getFirst("X-Forwarded-For");
        if (xForwardedFor != null && !xForwardedFor.isBlank()) {
            String[] ips = xForwardedFor.split(",");
            if (ips.length > 0 && !ips[0].isBlank()) {
                return ips[0].trim();
            }
        }

        InetSocketAddress remoteAddress = request.getRemoteAddress();
        if (remoteAddress != null && remoteAddress.getAddress() != null) {
            return remoteAddress.getAddress().getHostAddress();
        }

        return "127.0.0.1";
    }

    /**
     * Directly writes an HTTP 401 Unauthorized JSON response in a non-blocking reactive manner.
     *
     * @param exchange current server web exchange
     * @param message  error message to return
     * @return Mono completing when the response has been written
     */
    private Mono<Void> writeUnauthorizedResponse(ServerWebExchange exchange, String message) {
        ServerHttpResponse response = exchange.getResponse();
        if (response.isCommitted()) {
            return Mono.empty();
        }

        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        String sanitizedMessage = message != null ? message.replace("\"", "\\\"") : "Unauthorized";
        String jsonBody = String.format("{\"status\":401,\"error\":\"Unauthorized\",\"message\":\"%s\"}", sanitizedMessage);

        byte[] bytes = jsonBody.getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }
}
