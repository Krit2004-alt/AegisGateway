-- sliding_window_counter.lua
-- Sliding Window Counter Rate Limiter
-- KEYS[1]: Current window key
-- KEYS[2]: Previous window key
-- ARGV[1]: Limit (sustained + burst quota)
-- ARGV[2]: Current epoch timestamp (milliseconds)
-- ARGV[3]: Window size (milliseconds)
-- ARGV[4]: Request cost (tokens required, default 1)

local current_key = KEYS[1]
local prev_key = KEYS[2]

local limit = tonumber(ARGV[1])
local current_timestamp = tonumber(ARGV[2])
local window_size = tonumber(ARGV[3])
local cost = tonumber(ARGV[4]) or 1

-- 1. Retrieve current and previous window counts
local current_count = tonumber(redis.call('GET', current_key)) or 0
local prev_count = tonumber(redis.call('GET', prev_key)) or 0

-- 2. Calculate sliding window decay based on elapsed fraction of current window
local time_into_current_window = current_timestamp % window_size
local fraction_elapsed = time_into_current_window / window_size
local decay = 1.0 - fraction_elapsed
if decay < 0 then
    decay = 0
end

local estimated_count = current_count + math.floor(prev_count * decay)

-- 3. Check if request can be accommodated within the limit
if estimated_count < limit and (estimated_count + cost) <= limit then
    -- Allowed: increment current window counter by cost
    local new_count = redis.call('INCRBY', current_key, cost)

    -- Ensure TTL is set to 2x window size to cover current and subsequent window cycles
    local ttl_ms = math.ceil(window_size * 2)
    redis.call('PEXPIRE', current_key, ttl_ms)

    local remaining_tokens = limit - (estimated_count + cost)
    if remaining_tokens < 0 then
        remaining_tokens = 0
    end

    return {1, remaining_tokens, 0}
else
    -- Exceeded: calculate retry_after in milliseconds
    local retry_after_ms = window_size - time_into_current_window

    -- If decay during the current window can free up enough tokens earlier, calculate early retry
    if prev_count > 0 and (limit - current_count - cost) >= 0 then
        local needed_decay = (limit - current_count - cost) / prev_count
        local target_fraction = 1.0 - needed_decay
        local target_time_into_window = target_fraction * window_size
        local early_retry = math.ceil(target_time_into_window - time_into_current_window)
        if early_retry > 0 and early_retry < retry_after_ms then
            retry_after_ms = early_retry
        end
    end

    if retry_after_ms <= 0 then
        retry_after_ms = 1
    end

    return {0, 0, retry_after_ms}
end
