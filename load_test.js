import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Custom Trend and Rate metrics
const enterpriseDuration = new Trend('enterprise_req_duration');
const proDuration = new Trend('pro_req_duration');
const freeBlastDuration = new Trend('free_req_duration');
const throttledRate = new Rate('rate_limited_429_ratio');

export const options = {
  scenarios: {
    // 1. Enterprise Traffic (Traffic Shaping & High Throughput Test)
    enterprise_traffic: {
      executor: 'constant-vus',
      vus: 10,
      duration: '30s',
      exec: 'enterpriseScenario',
    },

    // 2. Pro Traffic (Steady-state sustained quota verification)
    pro_traffic: {
      executor: 'constant-vus',
      vus: 5,
      duration: '30s',
      exec: 'proScenario',
    },

    // 3. Free Tier Blast (Noisy Neighbor Attack Test)
    free_blast_traffic: {
      executor: 'constant-arrival-rate',
      rate: 150, // 150 req/sec to rapidly exceed 60 req/min limit
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 20,
      maxVUs: 50,
      exec: 'freeBlastScenario',
    },
  },

  // Latency & Quality of Service Thresholds
  thresholds: {
    // Enforce P95 < 1.5ms and P99 < 2.5ms across internal processing
    'http_req_duration': ['p(95)<1.5', 'p(99)<2.5'],
    'enterprise_req_duration': ['p(95)<1.5', 'p(99)<2.5'],
    'pro_req_duration': ['p(95)<1.5'],
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8080/api/v1/echo';

export function enterpriseScenario() {
  const params = {
    headers: {
      'X-API-Key': 'key-ent-corp-benchmark',
      'Content-Type': 'application/json',
    },
  };

  const res = http.get(BASE_URL, params);
  enterpriseDuration.add(res.timings.duration);

  check(res, {
    'enterprise status is 200': (r) => r.status === 200,
    'enterprise has rate limit header': (r) => r.headers['X-Ratelimit-Limit'] !== undefined,
    'enterprise source present': (r) => r.headers['X-Ratelimit-Source'] !== undefined,
  });

  sleep(0.01);
}

export function proScenario() {
  const params = {
    headers: {
      'Authorization': 'Bearer key-pro-client-benchmark',
      'Content-Type': 'application/json',
    },
  };

  const res = http.get(BASE_URL, params);
  proDuration.add(res.timings.duration);

  check(res, {
    'pro status is 200': (r) => r.status === 200,
    'pro has remaining header': (r) => r.headers['X-Ratelimit-Remaining'] !== undefined,
  });

  sleep(0.05);
}

export function freeBlastScenario() {
  const params = {
    headers: {
      'X-API-Key': 'key-free-noisy-neighbor',
      'Content-Type': 'application/json',
    },
  };

  const res = http.get(BASE_URL, params);
  freeBlastDuration.add(res.timings.duration);

  const isThrottled = res.status === 429;
  throttledRate.add(isThrottled);

  check(res, {
    'free status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    '429 response has Retry-After': (r) => r.status !== 429 || r.headers['Retry-After'] !== undefined,
  });
}
