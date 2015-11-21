-- Returns the number of published items, ignoring existing id entries.

-- KEYS:
--  (1) publishedZKey
--  (2) claimedHKey
--  (3) payloadsHKey
--  (4) notifyLKey

-- ARGS:
--  (1) score
--  (2 3 ...) id payload

local numPublished = 0;
local i = 2;

while ARGV[i] do

   local claimed = redis.call('hexists', KEYS[2], ARGV[i]);
   if claimed == 0 then
      local added = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[i]);
      if added > 0 then
         redis.call('hsetnx', KEYS[3], ARGV[i], ARGV[i+1]);
         redis.call('lpush', KEYS[4], ARGV[i]);
         numPublished = numPublished + 1;
      end
   end

   i = i + 2;
end

return numPublished;
