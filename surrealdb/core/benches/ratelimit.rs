#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

mod common;

use common::{block_on, setup_datastore_with_query};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

// ============================================================================
// Benchmark: RATELIMIT admission overhead
// ============================================================================

fn bench_ratelimit_admission(c: &mut Criterion) {
	let mut group = c.benchmark_group("ratelimit_admission");
	let runtime = common::create_runtime();

	let (baseline_dbs, baseline_ses) =
		block_on(setup_datastore_with_query("CREATE item:test SET name = 'baseline', age = 30;"));
	let (limited_dbs, limited_ses) = block_on(setup_datastore_with_query(
		"DEFINE TABLE item RATELIMIT FOR SELECT BY $session.id LIMIT 100000000 PER 1h; \
		 CREATE item:test SET name = 'limited', age = 30;",
	));

	group.throughput(Throughput::Elements(1));
	group.bench_function("select_by_id_baseline", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&baseline_dbs, &baseline_ses, "SELECT * FROM item:test;") });
	});
	group.bench_function("select_by_id_with_table_ratelimit", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&limited_dbs, &limited_ses, "SELECT * FROM item:test;") });
	});

	group.finish();
}

criterion_group! {
	name = benches;
	config = Criterion::default();
	targets = bench_ratelimit_admission,
}
criterion_main!(benches);
