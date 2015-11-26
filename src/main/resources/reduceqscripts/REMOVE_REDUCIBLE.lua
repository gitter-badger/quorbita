-- Returns 1 if removed, -1 if id was no longer claimed, or 0 if already removed.

-- KEYS:
--  (1) claimedReduceHKey
--  (2) claimStampsHKey
--  (3) pendingMappedSKey
--  (4) mappedResultsHKey
--  (5) payloadsReduceHKey

-- ARGS:
--  (1) reduceId

if redis.call('hdel', KEYS[2], ARGV[1]) == 0 then return -1; end

redis.call('del', KEYS[3], ARGV[1]);
redis.call('del', KEYS[4], ARGV[1]);

redis.call('hdel', KEYS[2], ARGV[1]);
redis.call('hdel', KEYS[5], ARGV[1]);
return redis.call('hdel', KEYS[1], ARGV[1]);
