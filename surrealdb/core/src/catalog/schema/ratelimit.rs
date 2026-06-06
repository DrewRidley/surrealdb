use std::time::Duration;

use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::PermissionKind;
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::types::PublicDuration;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RateLimit {
	pub(crate) actions: Vec<PermissionKind>,
	pub(crate) condition: Option<Expr>,
	pub(crate) bucket: Expr,
	pub(crate) limit: u64,
	pub(crate) period: Duration,
	pub(crate) burst: Option<u64>,
	pub(crate) scan: Option<u64>,
	pub(crate) result: Option<u64>,
}

pub(crate) type RateLimits = Vec<RateLimit>;

impl RateLimit {
	pub(crate) fn to_sql_definition(&self) -> crate::sql::RateLimit {
		crate::sql::RateLimit {
			actions: self.actions.clone().into_iter().map(Into::into).collect(),
			condition: self.condition.clone().map(Into::into),
			bucket: self.bucket.clone().into(),
			limit: self.limit,
			period: PublicDuration::from_std(self.period),
			burst: self.burst,
			scan: self.scan,
			result: self.result,
		}
	}
}

impl ToSql for RateLimit {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, sql_fmt);
	}
}

impl InfoStructure for RateLimit {
	fn structure(self) -> Value {
		Value::from(map! {
			"actions" => self.actions.into_iter().map(|a| Value::from(a.to_string())).collect::<Vec<_>>().into(),
			"condition", if let Some(v) = self.condition => v.structure(),
			"by" => self.bucket.structure(),
			"limit" => self.limit.into(),
			"period" => Value::Duration(self.period.into()),
			"burst", if let Some(v) = self.burst => v.into(),
			"scan", if let Some(v) = self.scan => v.into(),
			"result", if let Some(v) = self.result => v.into(),
		})
	}
}

impl From<crate::sql::RateLimit> for RateLimit {
	fn from(v: crate::sql::RateLimit) -> Self {
		Self {
			actions: v.actions.into_iter().map(Into::into).collect(),
			condition: v.condition.map(Into::into),
			bucket: v.bucket.into(),
			limit: v.limit,
			period: v.period.into_inner(),
			burst: v.burst,
			scan: v.scan,
			result: v.result,
		}
	}
}
