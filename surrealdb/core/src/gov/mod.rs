//! Resource governance primitives for admission, query budgets, and usage accounting.
//!
//! This module is intentionally small at first: it provides the budget spine that can be
//! threaded through `Context`, the 3.0 execution contexts, server admission, and nested
//! Surrealism host calls without changing query behaviour while disabled.

mod budget;
mod usage;

pub use budget::{ChargeOutcome, EnforcementMode, ResourceBudget, ResourceLimits};
pub use usage::{ResourceKind, ResourceUsageSnapshot};
