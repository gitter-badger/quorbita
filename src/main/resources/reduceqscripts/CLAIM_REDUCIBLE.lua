-- Claims the reduceId and corresponding payload for the reduceId with the least amount of pending results.

-- KEYS:
--  (1) publishedReduceZKey
--  (2) claimedReduceHKey
--  (3) claimStampsHKey
--  (4) payloadsReduceHKey
--  (5) notifyReducedLKey

-- ARGS:
--  (1) claimStamp

while true do
   local idWeight = redis.call('zrange', KEYS[1], 0, 0, 'WITHSCORES');
   local id = idWeight[1];
   if id == nil then return {}; end

   redis.call('zrem', KEYS[1], id);
   redis.call('lpop', KEYS[5]);

   if redis.call('hsetnx', KEYS[2], id, idWeight[2]) > 0 then
      redis.call('hset', KEYS[3], id, ARGV[1]);
      return {id, redis.call('hget', KEYS[4], id)};
   end
end
