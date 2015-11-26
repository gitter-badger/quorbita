-- Returns 1 if published, 0 if already published, or -1 if already claimed for each id.

-- KEYS:
--  (1) publishedZKey
--  (2) claimedHKey
--  (3) payloadsHKey
--  (4) notifyLKey

-- ARGS:
--  (1) inverseScore
--  (2 3 ...) id payload

local published = {};
local i = 2;
local j = 1;

while true do

   local id = ARGV[i];
   if id == nil then return published; end

   if redis.call('hexists', KEYS[2], id) == 0 then
      if redis.call('zadd', KEYS[1], 'NX', ARGV[1], id) > 0 then
         redis.call('hsetnx', KEYS[3], id, ARGV[i+1]);
         redis.call('lpush', KEYS[4], id);
         published[j] = 1;
      else
         published[j] = 0;
      end
   else
      published[j] = -1;
   end

   i = i + 2;
   j = j + 1;
end
