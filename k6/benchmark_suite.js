import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

// Custom Metrics
export const count200 = new Counter('http_200_total');
export const count429 = new Counter('http_429_total');
export const countOther = new Counter('http_other_total');

export const enterpriseDuration = new Trend('enterprise_duration_ms', true);
export const proDuration = new Trend('pro_duration_ms', true);
export const freeDuration = new Trend('free_duration_ms', true);

export const throttledRate = new Rate('rate_limited_ratio');

const BASE_URL = __ENV.GATEWAY_URL || 'http://aegis-gateway:8080/api/v1/test';

// Configure k6 to expect both 200 and 429 as valid response codes across rate limiting tests
http.setResponseCallback(http.expectedStatuses(200, 429));

export const options = {
  scenarios: {
    // Stage 1: Free Tier Throttling Test (80 sequential requests: exactly 60 allowed then 20 throttled)
    stage_1_free_tier: {
      executor: 'per-vu-iterations',
      vus: 1,
      iterations: 80,
      maxDuration: '15s',
      startTime: '0s',
      exec: 'freeTierScenario',
    },

    // Stage 2: Enterprise Sustained & Burst Traffic Shaping (600 RPS for 10s)
    stage_2_enterprise_burst: {
      executor: 'constant-arrival-rate',
      rate: 600,
      timeUnit: '1s',
      duration: '10s',
      preAllocatedVUs: 30,
      maxVUs: 100,
      startTime: '5s',
      exec: 'enterpriseScenario',
    },

    // Stage 3: Pro Tier Sustained Load (200 RPS for 6s)
    stage_3_pro_sustained: {
      executor: 'constant-arrival-rate',
      rate: 200,
      timeUnit: '1s',
      duration: '6s',
      preAllocatedVUs: 15,
      maxVUs: 50,
      startTime: '16s',
      exec: 'proScenario',
    },
  },

  thresholds: {
    'http_req_duration': ['p(95)<300', 'p(99)<400'],
    'enterprise_duration_ms': ['p(95)<300', 'p(99)<400'],
    'pro_duration_ms': ['p(95)<50', 'p(99)<100'],
    'checks': ['rate>0.99'],
  },
};

// Stage 1 Handler: Free Tier
export function freeTierScenario() {
  const params = {
    headers: {
      'X-API-Key': 'key-free-k6-benchmark',
    },
  };

  const res = http.get(BASE_URL, params);
  freeDuration.add(res.timings.duration);

  if (res.status === 200) {
    count200.add(1);
    throttledRate.add(0);
  } else if (res.status === 429) {
    count429.add(1);
    throttledRate.add(1);
  } else {
    countOther.add(1);
  }

  check(res, {
    'free status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'free has rate limit limit 60': (r) => r.headers['X-Ratelimit-Limit'] === '60',
    'free 429 has Retry-After': (r) => r.status !== 429 || r.headers['Retry-After'] !== undefined,
  });

  sleep(0.01);
}

// Stage 2 Handler: Enterprise Tier Burst & Virtual Queue
export function enterpriseScenario() {
  const params = {
    headers: {
      'X-API-Key': 'key-ent-k6-benchmark',
    },
  };

  const res = http.get(BASE_URL, params);
  enterpriseDuration.add(res.timings.duration);

  if (res.status === 200) {
    count200.add(1);
    throttledRate.add(0);
  } else if (res.status === 429) {
    count429.add(1);
    throttledRate.add(1);
  } else {
    countOther.add(1);
  }

  check(res, {
    'enterprise status is 200': (r) => r.status === 200,
    'enterprise limit header is 10500': (r) => r.headers['X-Ratelimit-Limit'] === '10500',
    'enterprise source present': (r) => r.headers['X-Ratelimit-Source'] !== undefined,
  });
}

// Stage 3 Handler: Pro Tier Sustained
export function proScenario() {
  const params = {
    headers: {
      'Authorization': 'Bearer key-pro-k6-benchmark',
    },
  };

  const res = http.get(BASE_URL, params);
  proDuration.add(res.timings.duration);

  if (res.status === 200) {
    count200.add(1);
    throttledRate.add(0);
  } else if (res.status === 429) {
    count429.add(1);
    throttledRate.add(1);
  } else {
    countOther.add(1);
  }

  check(res, {
    'pro status is 200': (r) => r.status === 200,
    'pro limit header is 1050': (r) => r.headers['X-Ratelimit-Limit'] === '1050',
    'pro remaining header exists': (r) => r.headers['X-Ratelimit-Remaining'] !== undefined,
  });
}
