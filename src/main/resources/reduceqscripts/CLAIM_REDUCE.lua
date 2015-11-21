-- Claims the reduceId and corresponding payload for the reduceId with the least amount of pending results.

-- KEYS:
--  (1) publishedReduceZKey
--  (2) claimedReduceHKey
--  (3) payloadsReduceHKey
--  (4) notifyReducedLKey

-- ARGS:

while true do
   local idScore = redis.call('zrange', KEYS[1], 0, 0, 'WITHSCORES');
   local id = idScore[1];
   if id == nil then
      return {};
   end

   local claimed = redis.call('hsetnx', KEYS[2], id, idScore[2]);
   redis.call('zrem', KEYS[1], id);
   redis.call('lpop', KEYS[4]);
   if claimed > 0 then
      return {id, redis.call('hget', KEYS[3], id)};
   end
end
