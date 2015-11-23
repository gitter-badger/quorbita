-- Returns at most 'limit' claimed id payload pairs

-- KEYS:
--  (1) publishedZKey
--  (2) claimedHKey
--  (3) notifyLKey
--  (4) payloadsHKey

-- ARGS:
--  (1) score
--  (2) limit

local idPayloads = {};

for i = 1, ARGV[2], 1 do

   local id = redis.call('zrange', KEYS[1], 0, 0)[1];
   if id == nil then return idPayloads; end

   if redis.call('hsetnx', KEYS[2], id, ARGV[1]) > 0 then
      idPayloads[i] = {id, redis.call('hget', KEYS[4], id)};
   else
      i = i - 1;
   end

   redis.call('zrem', KEYS[1], id);
   redis.call('lpop', KEYS[3]);
end

return idPayloads;
