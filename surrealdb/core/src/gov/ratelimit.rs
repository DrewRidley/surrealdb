use std::collections::HashMap;
use std::sync::Mutex;

use web_time::{Duration, Instant};

#[derive(Debug, Default)]
pub(crate) struct RateLimiter {
	buckets: Mutex<HashMap<String, Bucket>>,
}

#[derive(Debug)]
struct Bucket {
	tokens: f64,
	last: Instant,
}

impl RateLimiter {
	pub(crate) fn admit(
		&self,
		key: String,
		limit: u64,
		period: Duration,
		burst: Option<u64>,
	) -> bool {
		if limit == 0 {
			return false;
		}
		let capacity = burst.unwrap_or(limit).max(1) as f64;
		let refill_per_second = limit as f64 / period.as_secs_f64().max(f64::EPSILON);
		let now = Instant::now();
		let mut buckets = self.buckets.lock().expect("rate limiter mutex poisoned");
		let bucket = buckets.entry(key).or_insert(Bucket {
			tokens: capacity,
			last: now,
		});
		let elapsed = now.duration_since(bucket.last).as_secs_f64();
		bucket.tokens = (bucket.tokens + elapsed * refill_per_second).min(capacity);
		bucket.last = now;
		if bucket.tokens < 1.0 {
			return false;
		}
		bucket.tokens -= 1.0;
		true
	}
}
