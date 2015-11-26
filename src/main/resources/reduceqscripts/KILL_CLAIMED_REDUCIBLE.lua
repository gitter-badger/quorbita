-- Returns 1 if killed, -1 if id was no longer claimed, or 0 if already killed.

-- KEYS:
--  (1) deadReduceHKey
--  (2) claimedReduceHKey
--  (3) claimStampsHKey
--  (4) pendingMappedSKey
--  (5) payloadsReduceHKey

-- ARGS:
--  (1) claimStamp
--  (2) reduceId
--  (3) reducePayload

if redis.call('hget', KEYS[3], ARGV[2]) ~= ARGV[1] then return -1; end

local numPending = redis.call('scard', KEYS[4]);
local killed = redis.call('hsetnx', KEYS[1], ARGV[2], numPending);
redis.call('hdel', KEYS[2], ARGV[2]);
redis.call('hdel', KEYS[3], ARGV[2]);

if KEYS[5] and killed > 0 then
   redis.call('hset', KEYS[5], ARGV[2], ARGV[3]);
end

return killed;
