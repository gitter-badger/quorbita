-- Returns 1 if replublished or 0 if the id was no longer claimed/dead.

-- KEYS:
--  (1) publishedReduceZKey
--  (2) claimedOrDeadReduceHKey
--  (3) notifyReducedLKey
--  (4) payloadsReduceHKey

-- ARGS:
--  (1) reduceId
--  (2) reducePayload

local weight = redis.call('hget', KEYS[2], ARGV[1]);
if weight == nil then
   return 0;
end

redis.call('hdel', KEYS[2], ARGV[1]);
redis.call('zadd', KEYS[1], 'NX', weight, ARGV[1]);

if KEYS[4] then
   redis.call('hset', KEYS[4], ARGV[1], ARGV[2]);
end

redis.call('lpush', KEYS[3], ARGV[1]);

return 1;
