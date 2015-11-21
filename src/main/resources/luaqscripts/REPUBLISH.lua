-- Returns 1 if replublished or 0 if the id was no longer claimed/dead.

-- KEYS:
--  (1) publishedZKey
--  (2) claimedOrDeadHKey
--  (3) notifyLKey
--  (4) payloadsHKey


-- ARGS:
--  (1) score
--  (2) id
--  (3) payload

local deleted = redis.call('hdel', KEYS[2], ARGV[2]);
if deleted == 0 then
   return deleted;
end

redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]);

if KEYS[4] then
   redis.call('hset', KEYS[4], ARGV[2], ARGV[3]);
end

redis.call('lpush', KEYS[3], ARGV[2]);

return deleted;
