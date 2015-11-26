-- Returns 1 if removed, -1 if id was no longer claimed, or 0 if already removed.

-- KEYS:
--  (1) claimedReduceHKey
--  (2) claimStampsHKey
--  (3) pendingMappedSKey
--  (4) mappedResultsHKey
--  (5) payloadsReduceHKey

-- ARGS:
--  (1) claimStamp
--  (2) reduceId

if redis.call('hget', KEYS[2], ARGV[2]) ~= ARGV[1] then return -1; end

redis.call('del', KEYS[3], ARGV[2]);
redis.call('del', KEYS[4], ARGV[2]);

redis.call('hdel', KEYS[2], ARGV[2]);
redis.call('hdel', KEYS[5], ARGV[2]);
return redis.call('hdel', KEYS[1], ARGV[2]);
