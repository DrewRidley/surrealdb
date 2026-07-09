/**
[env]
# Consumed rate-limit tokens persist as bucket state in the KV store.
clean = true

[test]
reason = "SELECT rate limits meter rows scanned, not rows returned: a selective filter cannot hide an expensive scan"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
value = "[{ id: post:2 }]"

[[test.results]]
value = "[{ id: post:3 }]"

[[test.results]]
error = "The query exceeded the rate limit for table post"
*/

DEFINE TABLE post RATELIMIT FOR SELECT BY 'shared-key' LIMIT 2 PER 1h;
CREATE post:1;
CREATE post:2;
CREATE post:3;
SELECT * FROM post WHERE title = 'no-such-row';
