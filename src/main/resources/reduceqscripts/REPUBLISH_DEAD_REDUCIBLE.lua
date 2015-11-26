-- Returns 1 if replublished, -1 if the id was no longer dead, or 0 if already published.

-- KEYS:
--  (1) publishedReduceZKey
--  (2) deadReduceHKey
--  (3) notifyReducedLKey
--  (4) payloadsReduceHKey

-- ARGS:
--  (1) reduceId
--  (2) reducePayload

if redis.call('hdel', KEYS[2], ARGV[1]) == 0 then return -1; end

local numPending = redis.call('scard', KEYS[3]);
local published = redis.call('zadd', KEYS[1], 'NX', numPending, ARGV[1]);

if published > 0 then
   if KEYS[4] then
      redis.call('hset', KEYS[4], ARGV[1], ARGV[2]);
   end

   if numPending == 0 then
      redis.call('lpush', KEYS[3], ARGV[1]);
   end
end

return published;
