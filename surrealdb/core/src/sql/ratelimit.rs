use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, PermissionKind};
use crate::types::PublicDuration;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RateLimit {
	pub actions: Vec<PermissionKind>,
	pub condition: Option<Expr>,
	pub bucket: Expr,
	pub limit: u64,
	pub period: PublicDuration,
	pub scan: Option<u64>,
	pub scan_period: Option<PublicDuration>,
	pub result: Option<u64>,
}

pub(crate) type RateLimits = Vec<RateLimit>;

impl RateLimit {
	pub(crate) fn fmt_sql_clause(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("FOR ");
		if self.actions.is_empty() {
			if let Some(scan) = self.scan {
				let period = self.scan_period.as_ref().unwrap_or(&self.period);
				write_sql!(f, sql_fmt, "SCAN {} PER {}", scan, period);
			}
			return;
		}
		for (i, action) in self.actions.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			f.push_str(action.as_str().to_ascii_uppercase().as_str());
		}
		if let Some(condition) = &self.condition {
			write_sql!(f, sql_fmt, " WHERE {}", CoverStmts(condition));
		}
		write_sql!(f, sql_fmt, " BY {}", CoverStmts(&self.bucket));
		write_sql!(f, sql_fmt, " LIMIT {} PER {}", self.limit, self.period);
		if let Some(scan) = self.scan {
			let period = self.scan_period.as_ref().unwrap_or(&self.period);
			write_sql!(f, sql_fmt, " SCAN {} PER {}", scan, period);
		}
		if let Some(result) = self.result {
			write_sql!(f, sql_fmt, " RESULT {}", result);
		}
	}
}

impl ToSql for RateLimit {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("RATELIMIT ");
		self.fmt_sql_clause(f, sql_fmt);
	}
}

pub(crate) fn fmt_ratelimits_block(f: &mut String, sql_fmt: SqlFormat, ratelimits: &[RateLimit]) {
	if ratelimits.is_empty() {
		return;
	}
	f.push_str("RATELIMIT ");
	for (i, ratelimit) in ratelimits.iter().enumerate() {
		if i > 0 {
			f.push_str(", ");
		}
		ratelimit.fmt_sql_clause(f, sql_fmt);
	}
}

impl From<crate::catalog::RateLimit> for RateLimit {
	fn from(v: crate::catalog::RateLimit) -> Self {
		Self {
			actions: v.actions.into_iter().map(Into::into).collect(),
			condition: v.condition.map(Into::into),
			bucket: v.bucket.into(),
			limit: v.limit,
			period: v.period.into(),
			scan: v.scan,
			scan_period: v.scan_period.map(PublicDuration::from_std),
			result: v.result,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use surrealdb_types::ToSql;

	use super::RateLimit;
	use crate::sql::{Expr, Literal, PermissionKind};
	use crate::types::PublicDuration;

	#[test]
	fn scan_period_falls_back_to_limit_period_when_absent() {
		let ratelimit = RateLimit {
			actions: vec![PermissionKind::Select],
			condition: None,
			bucket: Expr::Literal(Literal::String("bucket".into())),
			limit: 10,
			period: PublicDuration::from(Duration::from_secs(60)),
			scan: Some(100),
			scan_period: None,
			result: None,
		};

		assert_eq!(
			ratelimit.to_sql(),
			"RATELIMIT FOR SELECT BY 'bucket' LIMIT 10 PER 1m SCAN 100 PER 1m"
		);
	}
}
