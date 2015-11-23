-- Returns 1 if killed or 0 if id was no longer claimed.

-- KEYS:
--  (1) deadHKey
--  (2) claimedIdsHKey
--  (3) payloadsHKey

-- ARGS:
--  (1) score
--  (2) id
--  (3) payload

local deleted = redis.call('hdel', KEYS[2], ARGV[2]);
if deleted == 0 then return deleted; end

redis.call('hset', KEYS[1], ARGV[2], ARGV[1]);

if KEYS[3] then
   redis.call('hset', KEYS[3], ARGV[2], ARGV[3]);
end

return deleted;
