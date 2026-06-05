use std::fmt;

/// A resource dimension that can be charged to a query or semantic action budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(usize)]
pub enum ResourceKind {
	WorkUnit = 0,
	KvRead,
	KvWrite,
	ScanKey,
	RowRead,
	RowWrite,
	ResultRow,
	ResultByte,
	IntermediateByte,
	FunctionCall,
	ScriptCall,
	ModuleCall,
	ModuleHostSqlCall,
	ModuleHostFunctionCall,
	ModuleHostKvRead,
	ModuleHostKvWrite,
	HttpCall,
	GraphHop,
}

impl ResourceKind {
	pub(crate) const COUNT: usize = 18;

	pub(crate) const ALL: [ResourceKind; Self::COUNT] = [
		ResourceKind::WorkUnit,
		ResourceKind::KvRead,
		ResourceKind::KvWrite,
		ResourceKind::ScanKey,
		ResourceKind::RowRead,
		ResourceKind::RowWrite,
		ResourceKind::ResultRow,
		ResourceKind::ResultByte,
		ResourceKind::IntermediateByte,
		ResourceKind::FunctionCall,
		ResourceKind::ScriptCall,
		ResourceKind::ModuleCall,
		ResourceKind::ModuleHostSqlCall,
		ResourceKind::ModuleHostFunctionCall,
		ResourceKind::ModuleHostKvRead,
		ResourceKind::ModuleHostKvWrite,
		ResourceKind::HttpCall,
		ResourceKind::GraphHop,
	];

	pub(crate) const fn index(self) -> usize {
		self as usize
	}

	pub fn label(self) -> &'static str {
		match self {
			ResourceKind::WorkUnit => "work units",
			ResourceKind::KvRead => "KV reads",
			ResourceKind::KvWrite => "KV writes",
			ResourceKind::ScanKey => "scan keys",
			ResourceKind::RowRead => "rows read",
			ResourceKind::RowWrite => "rows written",
			ResourceKind::ResultRow => "result rows",
			ResourceKind::ResultByte => "result bytes",
			ResourceKind::IntermediateByte => "intermediate bytes",
			ResourceKind::FunctionCall => "function calls",
			ResourceKind::ScriptCall => "script calls",
			ResourceKind::ModuleCall => "module calls",
			ResourceKind::ModuleHostSqlCall => "module host SQL calls",
			ResourceKind::ModuleHostFunctionCall => "module host function calls",
			ResourceKind::ModuleHostKvRead => "module host KV reads",
			ResourceKind::ModuleHostKvWrite => "module host KV writes",
			ResourceKind::HttpCall => "HTTP calls",
			ResourceKind::GraphHop => "graph hops",
		}
	}
}

impl fmt::Display for ResourceKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.label())
	}
}

/// Immutable usage snapshot returned by `ResourceBudget::usage`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceUsageSnapshot {
	values: [u64; ResourceKind::COUNT],
}

impl ResourceUsageSnapshot {
	pub(crate) fn new(values: [u64; ResourceKind::COUNT]) -> Self {
		Self {
			values,
		}
	}

	pub fn get(&self, kind: ResourceKind) -> u64 {
		self.values[kind.index()]
	}
}
