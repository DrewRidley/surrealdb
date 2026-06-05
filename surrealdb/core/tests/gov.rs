use surrealdb_core::gov::{EnforcementMode, ResourceBudget, ResourceKind, ResourceLimits};

#[test]
fn disabled_budget_does_not_record_usage_or_reject() {
	let budget = ResourceBudget::disabled();

	budget.charge(ResourceKind::ResultRow, 10).expect("disabled budget should not reject");

	let usage = budget.usage();
	assert_eq!(usage.get(ResourceKind::ResultRow), 0);
	assert_eq!(budget.mode(), EnforcementMode::Disabled);
}

#[test]
fn monitor_budget_records_usage_without_rejecting() {
	let limits = ResourceLimits::default().with_limit(ResourceKind::ResultRow, 5);
	let budget = ResourceBudget::monitor(limits);

	budget.charge(ResourceKind::ResultRow, 3).expect("below limit should not reject");
	budget.charge(ResourceKind::ResultRow, 4).expect("monitor mode should not reject");

	let usage = budget.usage();
	assert_eq!(usage.get(ResourceKind::ResultRow), 7);
	assert_eq!(budget.mode(), EnforcementMode::Monitor);
}

#[test]
fn enforcing_budget_rejects_when_limit_is_exceeded() {
	let limits = ResourceLimits::default().with_limit(ResourceKind::ResultRow, 5);
	let budget = ResourceBudget::enforcing(limits);

	budget.charge(ResourceKind::ResultRow, 5).expect("at limit should pass");
	let err = budget.charge(ResourceKind::ResultRow, 1).expect_err("over limit should reject");

	let message = err.to_string();
	assert!(message.contains("result rows"));
	assert!(message.contains("5"));
	assert!(message.contains("6"));
	assert_eq!(budget.usage().get(ResourceKind::ResultRow), 6);
}
