-- Returns 1 if killed or 0 if id was no longer claimed.

-- KEYS:
--  (1) deadReduceHKey
--  (2) claimedReduceHKey
--  (3) pendingMappedSKey
--  (4) payloadsReduceHKey

-- ARGS:
--  (1) reduceId
--  (2) reducePayload

local deleted = redis.call('hdel', KEYS[2], ARGV[2]);
if deleted == 0 then return deleted; end

local numPending = redis.call('scard', KEYS[3]);
redis.call('hset', KEYS[1], ARGV[1], numPending);

if KEYS[4] then
   redis.call('hset', KEYS[4], ARGV[1], ARGV[2]);
end

return deleted;
