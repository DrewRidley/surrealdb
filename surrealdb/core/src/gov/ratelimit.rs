use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use web_time::{Duration, SystemTime, UNIX_EPOCH};

use crate::kvs::{Error as KvsError, Transaction};

#[derive(Debug, Default)]
pub(crate) struct RateLimiter {
	cleanup_counter: AtomicU64,
}

impl RateLimiter {
	pub(crate) async fn admit_kv(
		&self,
		txn: &Transaction,
		key: String,
		limit: u64,
		period: Duration,
		burst: Option<u64>,
	) -> Result<bool> {
		if limit == 0 {
			return Ok(false);
		}
		let capacity = burst.unwrap_or(limit).max(1) as f64;
		let refill_per_second = limit as f64 / period.as_secs_f64().max(f64::EPSILON);
		let now_ms = now_millis();
		let key = kv_key(&key);
		let old = txn.get(&key, None).await?;
		let mut bucket = old
			.as_deref()
			.and_then(decode_bucket)
			.filter(|bucket| bucket.expires_at_ms > now_ms)
			.unwrap_or(StoredBucket {
				tokens: capacity,
				last_ms: now_ms,
				expires_at_ms: expiry_millis(now_ms, period, capacity, refill_per_second),
			});

		let elapsed = now_ms.saturating_sub(bucket.last_ms) as f64 / 1000.0;
		bucket.tokens = (bucket.tokens + elapsed * refill_per_second).min(capacity);
		bucket.last_ms = now_ms;
		bucket.expires_at_ms = expiry_millis(now_ms, period, capacity, refill_per_second);
		if bucket.tokens < 1.0 {
			let new = encode_bucket(&bucket);
			if !put_bucket(txn, &key, &new, old.as_ref()).await? {
				return Ok(false);
			}
			self.cleanup_expired(txn, now_ms).await?;
			return Ok(false);
		}
		bucket.tokens -= 1.0;
		let new = encode_bucket(&bucket);
		if !put_bucket(txn, &key, &new, old.as_ref()).await? {
			return Ok(false);
		}
		self.cleanup_expired(txn, now_ms).await?;
		Ok(true)
	}

	async fn cleanup_expired(&self, txn: &Transaction, now_ms: u64) -> Result<()> {
		if self.cleanup_counter.fetch_add(1, Ordering::Relaxed) % 256 != 0 {
			return Ok(());
		}
		let start = RATE_LIMIT_PREFIX.to_vec();
		let mut end = RATE_LIMIT_PREFIX.to_vec();
		end.push(0xff);
		let scan = txn.scan(start..end, 64, 0, None).await?;
		for (key, val) in scan {
			if decode_bucket(&val).is_some_and(|bucket| bucket.expires_at_ms <= now_ms) {
				txn.del(&key).await?;
			}
		}
		Ok(())
	}
}

const RATE_LIMIT_PREFIX: &[u8] = b"/!rl";

#[derive(Clone, Copy, Debug)]
struct StoredBucket {
	tokens: f64,
	last_ms: u64,
	expires_at_ms: u64,
}

fn now_millis() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}

fn expiry_millis(now_ms: u64, period: Duration, capacity: f64, refill_per_second: f64) -> u64 {
	let refill_ms = ((capacity / refill_per_second.max(f64::EPSILON)) * 1000.0) as u64;
	now_ms
		.saturating_add(refill_ms)
		.saturating_add(period.as_millis().try_into().unwrap_or(u64::MAX))
}

fn kv_key(key: &str) -> Vec<u8> {
	let mut out = RATE_LIMIT_PREFIX.to_vec();
	out.extend_from_slice(&stable_hash(key.as_bytes()).to_be_bytes());
	out
}

fn stable_hash(bytes: &[u8]) -> u64 {
	let mut hash = 0xcbf29ce484222325_u64;
	for byte in bytes {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(0x100000001b3);
	}
	hash
}

fn encode_bucket(bucket: &StoredBucket) -> Vec<u8> {
	let mut out = Vec::with_capacity(24);
	out.extend_from_slice(&bucket.tokens.to_bits().to_be_bytes());
	out.extend_from_slice(&bucket.last_ms.to_be_bytes());
	out.extend_from_slice(&bucket.expires_at_ms.to_be_bytes());
	out
}

async fn put_bucket(
	txn: &Transaction,
	key: &Vec<u8>,
	new: &Vec<u8>,
	old: Option<&Vec<u8>>,
) -> Result<bool> {
	match txn.putc(key, new, old).await {
		Ok(()) => Ok(true),
		Err(err)
			if matches!(
				err.downcast_ref::<KvsError>(),
				Some(KvsError::TransactionConditionNotMet)
			) =>
		{
			Ok(false)
		}
		Err(err) => Err(err),
	}
}

fn decode_bucket(bytes: &[u8]) -> Option<StoredBucket> {
	if bytes.len() != 24 {
		return None;
	}
	let tokens = f64::from_bits(u64::from_be_bytes(bytes[0..8].try_into().ok()?));
	let last_ms = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
	let expires_at_ms = u64::from_be_bytes(bytes[16..24].try_into().ok()?);
	Some(StoredBucket {
		tokens,
		last_ms,
		expires_at_ms,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::{Datastore, LockType, TransactionType};

	#[tokio::test]
	async fn kv_admission_shares_bucket_across_limiter_instances() {
		let ds = Datastore::new("memory").await.unwrap();
		let limiter_a = RateLimiter::default();
		let limiter_b = RateLimiter::default();

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		assert!(
			limiter_a
				.admit_kv(&tx, "shared".to_string(), 1, Duration::from_secs(3600), None)
				.await
				.unwrap()
		);
		tx.commit().await.unwrap();

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		assert!(
			!limiter_b
				.admit_kv(&tx, "shared".to_string(), 1, Duration::from_secs(3600), None)
				.await
				.unwrap()
		);
		tx.cancel().await.unwrap();
	}
}
