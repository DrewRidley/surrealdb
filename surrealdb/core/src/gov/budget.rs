use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;

use crate::err::Error;
use crate::gov::usage::{ResourceKind, ResourceUsageSnapshot};

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
		}
	}

	pub fn mode(&self) -> EnforcementMode {
		self.mode
	}

	pub fn charge(&self, kind: ResourceKind, amount: u64) -> Result<()> {
		if matches!(self.mode, EnforcementMode::Disabled) || amount == 0 {
			return Ok(());
		}

		let used =
			self.usage[kind.index()].fetch_add(amount, Ordering::Relaxed).saturating_add(amount);

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
			.finish()
	}
}
