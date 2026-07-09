/**
[env]
# Consumed rate-limit tokens persist as bucket state in the KV store.
clean = true

[test]
reason = "Metering is statement-scoped: a SELECT admitted under a policy pays for every record it reads, including traversed edges and their targets. Here the traversal reads 3 records (user:drew, the friend edge, user:chiru) against a capacity-2 bucket, so the query is denied mid-flight"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:drew }]"

[[test.results]]
value = "[{ id: user:chiru }]"

[[test.results]]
value = "[{ id: friend:f9s9gmdu1lp3bhhuutw8, in: user:drew, out: user:chiru }]"
skip-record-id-key = true

[[test.results]]
error = "The query exceeded the rate limit for table user"
*/

DEFINE TABLE user RATELIMIT FOR SELECT BY 'k' LIMIT 2 PER 1h;
CREATE user:drew;
CREATE user:chiru;
RELATE user:drew->friend->user:chiru;
SELECT ->friend->user.* AS friends FROM user:drew;
