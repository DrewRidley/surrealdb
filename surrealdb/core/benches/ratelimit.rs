#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

mod common;

use common::{block_on, setup_datastore_with_query};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;

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
	let (where_false_dbs, where_false_ses) = block_on(setup_datastore_with_query(
		"DEFINE TABLE item RATELIMIT FOR SELECT WHERE false BY $session.id LIMIT 1 PER 1h; \
		 CREATE item:test SET name = 'where_false', age = 30;",
	));
	let (ip_bucket_dbs, mut ip_bucket_ses) = block_on(setup_datastore_with_query(
		"DEFINE TABLE item RATELIMIT FOR SELECT BY $session.ip LIMIT 100000000 PER 1h; \
		 CREATE item:test SET name = 'ip_bucket', age = 30;",
	));
	ip_bucket_ses.ip = Some("127.0.0.1".to_string());

	group.throughput(Throughput::Elements(1));
	group.bench_function("select_by_id_baseline", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&baseline_dbs, &baseline_ses, "SELECT * FROM item:test;") });
	});
	group.bench_function("select_by_id_with_session_id_limit", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&limited_dbs, &limited_ses, "SELECT * FROM item:test;") });
	});
	group.bench_function("select_by_id_with_where_false_policy", |b| {
		b.to_async(&runtime).iter(|| async {
			query!(&where_false_dbs, &where_false_ses, "SELECT * FROM item:test;")
		});
	});
	group.bench_function("select_by_id_with_session_ip_limit", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&ip_bucket_dbs, &ip_bucket_ses, "SELECT * FROM item:test;") });
	});

	group.finish();
}

// ============================================================================
// Benchmark: RATELIMIT scan quota overhead
// ============================================================================

async fn setup_ratelimit_scan_datastore(
	ratelimit: Option<&str>,
	count: u64,
) -> (Datastore, Session) {
	let dbs = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	dbs.execute("USE NAMESPACE test DATABASE test", &ses, None).await.unwrap();
	if let Some(ratelimit) = ratelimit {
		dbs.execute(ratelimit, &ses, None).await.unwrap();
	}
	for i in 0..count {
		dbs.execute(&format!("CREATE item:{i} SET value = {i};"), &ses, None).await.unwrap();
	}
	(dbs, ses)
}

fn bench_ratelimit_scan(c: &mut Criterion) {
	let mut group = c.benchmark_group("ratelimit_scan");
	let runtime = common::create_runtime();
	let count = 100;

	let (baseline_dbs, baseline_ses) = block_on(setup_ratelimit_scan_datastore(None, count));
	let (scan_dbs, scan_ses) = block_on(setup_ratelimit_scan_datastore(
		Some("DEFINE TABLE item RATELIMIT FOR SCAN 1000000000 PER 1h;"),
		count,
	));
	let (combined_dbs, combined_ses) = block_on(setup_ratelimit_scan_datastore(
		Some(
			"DEFINE TABLE item RATELIMIT \
			 FOR SELECT BY $session.id LIMIT 100000000 PER 1h SCAN 1000000000 PER 1h RESULT 1000;",
		),
		count,
	));

	group.throughput(Throughput::Elements(count));
	group.bench_function("full_table_select_baseline", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&baseline_dbs, &baseline_ses, "SELECT * FROM item;") });
	});
	group.bench_function("full_table_select_with_scan_quota", |b| {
		b.to_async(&runtime).iter(|| async { query!(&scan_dbs, &scan_ses, "SELECT * FROM item;") });
	});
	group.bench_function("full_table_select_with_admission_scan_and_result", |b| {
		b.to_async(&runtime)
			.iter(|| async { query!(&combined_dbs, &combined_ses, "SELECT * FROM item;") });
	});

	group.finish();
}

criterion_group! {
	name = benches;
	config = Criterion::default();
	targets = bench_ratelimit_admission, bench_ratelimit_scan,
}
criterion_main!(benches);
