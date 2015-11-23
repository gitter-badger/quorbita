-- Returns 1 if replublished or 0 if the id was no longer claimed/dead.

-- KEYS:
--  (1) publishedZKey
--  (2) claimedOrDeadHKey
--  (3) notifyLKey
--  (4) payloadsHKey

-- ARGS:
--  (1) claimedScore
--  (2) score
--  (3) id
--  (4) payload

local claimedScore = redis.call('hget', KEYS[2], ARGV[3]);
if claimedScore == nil or claimedScore != ARGV[1] then return 0; end

redis.call('hdel', KEYS[2], ARGV[3]);
redis.call('zadd', KEYS[1], 'NX', ARGV[2], ARGV[3]);

if KEYS[4] then
   redis.call('hset', KEYS[4], ARGV[3], ARGV[4]);
end

redis.call('lpush', KEYS[3], ARGV[3]);

return 1;
