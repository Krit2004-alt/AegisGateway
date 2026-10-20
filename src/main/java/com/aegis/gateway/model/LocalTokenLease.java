package com.aegis.gateway.model;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-free local token lease cached in L1 memory.
 * Employs an AtomicLong for non-blocking token decrementing under high concurrency
 * without thread contention or synchronized blocks.
 */
public class LocalTokenLease {

    private final AtomicLong availableTokens;
    private final long expiryTimestamp;
    private final long totalLimit;

    /**
     * Constructs a new LocalTokenLease.
     *
     * @param initialTokens   number of tokens allocated in this lease
     * @param expiryTimestamp epoch timestamp in ms after which this lease expires
     * @param totalLimit      total sustained/burst limit configured for the tenant
     */
    public LocalTokenLease(long initialTokens, long expiryTimestamp, long totalLimit) {
        this.availableTokens = new AtomicLong(Math.max(0L, initialTokens));
        this.expiryTimestamp = expiryTimestamp;
        this.totalLimit = totalLimit;
    }

    /**
     * Attempts to consume a single token lock-free.
     *
     * @return true if 1 token was acquired before lease expiry; false otherwise
     */
    public boolean tryConsume() {
        return tryConsume(1L);
    }

    /**
     * Attempts to consume the specified cost of tokens using a lock-free CAS loop.
     *
     * @param cost number of tokens requested
     * @return true if sufficient tokens were consumed; false otherwise
     */
    public boolean tryConsume(long cost) {
        if (cost <= 0) {
            return true;
        }
        if (isExpired()) {
            return false;
        }

        while (true) {
            long current = availableTokens.get();
            if (current < cost) {
                return false;
            }
            if (availableTokens.compareAndSet(current, current - cost)) {
                return true;
            }
        }
    }

    /**
     * Checks if this lease has expired relative to current epoch time.
     *
     * @return true if expired; false otherwise
     */
    public boolean isExpired() {
        return System.currentTimeMillis() >= expiryTimestamp;
    }

    /**
     * Returns remaining available tokens safely without negative values.
     *
     * @return remaining token count (>= 0)
     */
    public long getRemaining() {
        return Math.max(0L, availableTokens.get());
    }

    /**
     * Returns current remaining tokens in this local lease.
     */
    public long getAvailableTokens() {
        return getRemaining();
    }

    /**
     * Returns the epoch expiration timestamp in milliseconds.
     */
    public long getExpiryTimestamp() {
        return expiryTimestamp;
    }

    /**
     * Returns the total limit governing this lease.
     */
    public long getTotalLimit() {
        return totalLimit;
    }
}
