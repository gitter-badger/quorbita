-- PUBLISH_EPOCH_REDUCIBLE

-- KEYS:
--  (1) publishedZKey
--  (2) claimedHKey
--  (3) payloadsHKey
--  (4) notifyLKey
--  (5) reducePendingSKey
--  (6) reducePublishedZkey
--  (7) reduceClaimedHKey
--  (8) reducePayloadsHKey

-- ARGS:
--  (1) score
--  (2) reduceId
--  (3) reducePayload
--  (4 5 ...) id payload

local numPublished = 0;
local i = 4;

while true do

   local id = ARGV[i];
   if id == nil then break end

   local claimed = redis.call('hexists', KEYS[2], id);

   if claimed == 0 then
      redis.call('hsetnx', KEYS[3], id, ARGV[i+1]);
      redis.call('sadd', KEYS[5], id);

      local added = redis.call('zadd', KEYS[1], 'NX', ARGV[1], id);
      if added > 0 then
         redis.call('lpush', KEYS[4], id);
         numPublished = numPublished + 1;
      end
   end

   i = i + 2;
end

local claimed = redis.call('hexists', KEYS[7], ARGV[2]);
if claimed == 0 then
   redis.call('hsetnx', KEYS[8], ARGV[2], ARGV[3]);
   redis.call('zadd', KEYS[6], 'NX', ARGV[1], ARGV[2]);
end

return numPublished;
