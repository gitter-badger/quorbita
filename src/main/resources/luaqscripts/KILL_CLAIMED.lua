-- Returns 1 if killed or 0 if id was no longer claimed.

-- KEYS:
--  (1) deadHKey
--  (2) claimedIdsHKey
--  (3) payloadsHKey

-- ARGS:
--  (1) claimedScore
--  (2) score
--  (3) id
--  (4) payload

local claimedScore = redis.call('hget', KEYS[2], ARGV[3]);
if claimedScore == nil or claimedScore != ARGV[1] then return 0; end

redis.call('hdel', KEYS[2], ARGV[3]);
redis.call('hset', KEYS[1], ARGV[3], ARGV[2]);

if KEYS[3] then
   redis.call('hset', KEYS[3], ARGV[3], ARGV[4]);
end

return 1;
