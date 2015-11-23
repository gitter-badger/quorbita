-- Returns 1 if replublished or -1 if the id was no longer claimed, or 0 if already published for each id.

-- KEYS:
--  (1) publishedZKey
--  (2) claimedHKey
--  (3) notifyLKey
--  (4) payloadsHKey

-- ARGS:
--  (1) claimedScore
--  (2) score
--  (3 4 ...) id payload

local republished = {};

local i = 3;
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

   local claimedScore = redis.call('hget', KEYS[2], id);
   if claimedScore == nil or claimedScore ~= ARGV[1] then
      republished[j] = -1;
   else
      redis.call('hdel', KEYS[2], id);
      republished[j] = redis.call('zadd', KEYS[1], 'NX', ARGV[2], id);

      if KEYS[4] then
         redis.call('hset', KEYS[4], id, ARGV[i+1]);
      end

      redis.call('lpush', KEYS[3], id);
   end

   i = i + incr;
   j = j + 1;
end
