use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::Result;

use crate::err::Error;
use crate::gov::usage::{ResourceKind, ResourceUsageSnapshot};

const NO_TRUNCATION: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChargeOutcome {
	Charged,
	/// The charge crossed an enforced limit while truncation was allowed.
	/// The second field is how many units from this charge were still within
	/// the limit and may be processed before stopping.
	Truncated(ResourceKind, u64),
}

/// Whether a resource budget is disabled, observing, or enforcing limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnforcementMode {
	Disabled,
	Monitor,
	Enforce,
}

/// Per-resource limits for a query or action budget.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceLimits {
	limits: [Option<u64>; ResourceKind::COUNT],
}

impl ResourceLimits {
	pub fn with_limit(mut self, kind: ResourceKind, limit: u64) -> Self {
		self.limits[kind.index()] = Some(limit);
		self
	}

	pub fn limit(&self, kind: ResourceKind) -> Option<u64> {
		self.limits[kind.index()]
	}
}

/// Low-overhead shared budget and usage counter for one admitted unit of work.
pub struct ResourceBudget {
	mode: EnforcementMode,
	limits: ResourceLimits,
	usage: [AtomicU64; ResourceKind::COUNT],
	truncated: AtomicU64,
	truncation_allowed: AtomicBool,
}

impl ResourceBudget {
	pub fn disabled() -> Self {
		Self::new(EnforcementMode::Disabled, ResourceLimits::default())
	}

	pub fn monitor(limits: ResourceLimits) -> Self {
		Self::new(EnforcementMode::Monitor, limits)
	}

	pub fn enforcing(limits: ResourceLimits) -> Self {
		Self::new(EnforcementMode::Enforce, limits)
	}

	fn new(mode: EnforcementMode, limits: ResourceLimits) -> Self {
		Self {
			mode,
			limits,
			usage: std::array::from_fn(|_| AtomicU64::new(0)),
			truncated: AtomicU64::new(NO_TRUNCATION),
			truncation_allowed: AtomicBool::new(false),
		}
	}

	pub fn mode(&self) -> EnforcementMode {
		self.mode
	}

	pub fn charge(&self, kind: ResourceKind, amount: u64) -> Result<()> {
		if matches!(self.mode, EnforcementMode::Disabled) || amount == 0 {
			return Ok(());
		}

		let previous = self.usage[kind.index()].fetch_add(amount, Ordering::Relaxed);
		let used = previous.saturating_add(amount);

		if matches!(self.mode, EnforcementMode::Enforce) {
			if let Some(limit) = self.limits.limit(kind) {
				if used > limit {
					return Err(Error::QueryResourceExceeded {
						resource: kind.label(),
						limit,
						used,
					}
					.into());
				}
			}
		}

		Ok(())
	}

	pub fn charge_or_truncate(&self, kind: ResourceKind, amount: u64) -> Result<ChargeOutcome> {
		if matches!(self.mode, EnforcementMode::Disabled) || amount == 0 {
			return Ok(ChargeOutcome::Charged);
		}

		let previous = self.usage[kind.index()].fetch_add(amount, Ordering::Relaxed);
		let used = previous.saturating_add(amount);

		if matches!(self.mode, EnforcementMode::Enforce) {
			if let Some(limit) = self.limits.limit(kind) {
				if used > limit {
					if !self.truncation_allowed.load(Ordering::Relaxed) {
						return Err(Error::QueryResourceExceeded {
							resource: kind.label(),
							limit,
							used,
						}
						.into());
					}
					self.mark_truncated(kind);
					let allowed = limit.saturating_sub(previous);
					return Ok(ChargeOutcome::Truncated(kind, allowed));
				}
			}
		}

		Ok(ChargeOutcome::Charged)
	}

	pub fn mark_truncated(&self, kind: ResourceKind) {
		let _ = self.truncated.compare_exchange(
			NO_TRUNCATION,
			kind.index() as u64,
			Ordering::Relaxed,
			Ordering::Relaxed,
		);
	}

	pub fn set_truncation_allowed(&self, allowed: bool) {
		self.truncation_allowed.store(allowed, Ordering::Relaxed);
	}

	pub fn truncated_kind(&self) -> Option<ResourceKind> {
		let index = self.truncated.load(Ordering::Relaxed);
		Self::kind_from_truncation_index(index)
	}

	pub fn take_truncated_kind(&self) -> Option<ResourceKind> {
		let index = self.truncated.swap(NO_TRUNCATION, Ordering::Relaxed);
		Self::kind_from_truncation_index(index)
	}

	fn kind_from_truncation_index(index: u64) -> Option<ResourceKind> {
		if index == NO_TRUNCATION {
			return None;
		}
		ResourceKind::ALL.into_iter().find(|kind| kind.index() as u64 == index)
	}

	pub fn usage(&self) -> ResourceUsageSnapshot {
		let mut values = [0; ResourceKind::COUNT];
		for kind in ResourceKind::ALL {
			values[kind.index()] = self.usage[kind.index()].load(Ordering::Relaxed);
		}
		ResourceUsageSnapshot::new(values)
	}
}

impl Default for ResourceBudget {
	fn default() -> Self {
		Self::disabled()
	}
}

impl std::fmt::Debug for ResourceBudget {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("ResourceBudget")
			.field("mode", &self.mode)
			.field("limits", &self.limits)
			.field("usage", &self.usage())
			.field("truncated", &self.truncated_kind())
			.finish()
	}
}

#[cfg(test)]
mod tests {
	use super::{ChargeOutcome, ResourceBudget, ResourceKind, ResourceLimits};

	#[test]
	fn charge_or_truncate_marks_scan_limit_without_erroring() {
		let budget = ResourceBudget::enforcing(
			ResourceLimits::default().with_limit(ResourceKind::ScanKey, 5),
		);
		budget.set_truncation_allowed(true);

		assert_eq!(
			budget.charge_or_truncate(ResourceKind::ScanKey, 3).unwrap(),
			ChargeOutcome::Charged
		);
		assert_eq!(
			budget.charge_or_truncate(ResourceKind::ScanKey, 3).unwrap(),
			ChargeOutcome::Truncated(ResourceKind::ScanKey, 2)
		);
		assert_eq!(budget.truncated_kind(), Some(ResourceKind::ScanKey));
	}

	#[test]
	fn charge_or_truncate_errors_when_truncation_is_not_allowed() {
		let budget = ResourceBudget::enforcing(
			ResourceLimits::default().with_limit(ResourceKind::ScanKey, 5),
		);

		let err = budget.charge_or_truncate(ResourceKind::ScanKey, 6).unwrap_err();
		assert!(err.to_string().contains("scan keys"));
		assert_eq!(budget.truncated_kind(), None);
	}
}
