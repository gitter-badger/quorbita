-- Returns 1 if replublished, -1 if the id was no longer claimed/dead, or 0 if already published for each id.

-- KEYS:
--  (1) publishedZKey
--  (2) claimedOrDeadHKey
--  (3) notifyLKey
--  (4) payloadsHKey

-- ARGS:
--  (1) score
--  (2 3 ...) id payload

local republished = {};

local i = 2;
local incr;
if KEYS[4] then
   incr=2;
else
   incr=1;
end

local j = 1;

while true do

   local id = ARGV[i];
   if id == nil then return republished; end

   local deleteClaim = redis.call('hdel', KEYS[2], id);
   if deleteClaim == 0 then
      republished[j] = -1;
   else
      republished[j] = redis.call('zadd', KEYS[1], 'NX', ARGV[1], id);

      if KEYS[4] then
         redis.call('hset', KEYS[4], id, ARGV[i+1]);
      end

      redis.call('lpush', KEYS[3], id);
   end

   i = i + incr;
   j = j + 1;
end
