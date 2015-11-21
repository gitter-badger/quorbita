-- PUBLISH_REDUCIBLE

-- KEYS:
--  (1) publishedZKey
--  (2) claimedHKey
--  (3) payloadsHKey
--  (4) notifyLKey
--  (5) reducePendingSKey

-- ARGS:
--  (1) score
--  (2 3 ...) id payload


local numPublished = 0;
local i = 2;

while true do

   local id = ARGV[i];
   if id == nil then
      return numPublished;
   end

   local claimed = redis.call('hexists', KEYS[2], id);
   if claimed == 0 then
      local added = redis.call('zadd', KEYS[1], 'NX', ARGV[1], id);
      if added > 0 then
         redis.call('hsetnx', KEYS[3], id, ARGV[i+1]);
         redis.call('sadd', KEYS[5], id);
         redis.call('lpush', KEYS[4], id);
         numPublished = numPublished + 1;
      end
   end

   i = i + 2;
end
