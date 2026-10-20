package com.aegis.gateway.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class LocalTokenLeaseTest {

    @Test
    @DisplayName("Should successfully consume tokens within lease limits")
    void shouldConsumeTokens() {
        long expiry = System.currentTimeMillis() + 5000L;
        LocalTokenLease lease = new LocalTokenLease(10L, expiry, 100L);

        assertThat(lease.tryConsume(1)).isTrue();
        assertThat(lease.getAvailableTokens()).isEqualTo(9L);
        assertThat(lease.tryConsume(4)).isTrue();
        assertThat(lease.getAvailableTokens()).isEqualTo(5L);
        assertThat(lease.tryConsume(6)).isFalse(); // Not enough tokens
        assertThat(lease.getAvailableTokens()).isEqualTo(5L); // Unchanged
    }

    @Test
    @DisplayName("Should reject token consumption when lease is expired")
    void shouldRejectWhenExpired() {
        long pastExpiry = System.currentTimeMillis() - 1000L;
        LocalTokenLease lease = new LocalTokenLease(10L, pastExpiry, 100L);

        assertThat(lease.isExpired()).isTrue();
        assertThat(lease.tryConsume(1)).isFalse();
    }

    @Test
    @DisplayName("Should handle highly concurrent consumption lock-free without lost updates")
    void shouldHandleConcurrentConsumption() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 100;
        long totalTokens = 1000L;

        long expiry = System.currentTimeMillis() + 10000L;
        LocalTokenLease lease = new LocalTokenLease(totalTokens, expiry, totalTokens);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulConsumptions = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        if (lease.tryConsume(1)) {
                            successfulConsumptions.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        assertThat(successfulConsumptions.get()).isEqualTo(1000);
        assertThat(lease.getAvailableTokens()).isEqualTo(0L);
        assertThat(lease.tryConsume(1)).isFalse();
    }
}
