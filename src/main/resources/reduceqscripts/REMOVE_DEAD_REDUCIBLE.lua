-- Returns 1 if removed, 0 if already removed, -1 if published, or -2 if claimed.

-- KEYS:
--  (1) publishedReduceZKey
--  (2) claimedReduceHKey
--  (3) deadReduceHKey
--  (4) pendingMappedSKey
--  (5) mappedResultsHKey
--  (6) payloadsReduceHKey

-- ARGS:
--  (1) reduceId

if redis.call('hdel', KEYS[3], ARGV[1]) == 0 then return 0; end
if redis.call('zscore', KEYS[1], ARGV[1]) then return -1; end
if redis.call('hexists', KEYS[2], ARGV[1]) == 1 then return -2; end

redis.call('del', KEYS[4], ARGV[1]);
redis.call('del', KEYS[5], ARGV[1]);

redis.call('hdel', KEYS[5], ARGV[1]);
return 1;
