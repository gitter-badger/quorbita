-- Returns 1 if killed, -1 if id was no longer claimed, or 0 if already dead for each id.

-- KEYS:
--  (1) deadHKey
--  (2) claimedIdsHKey
--  (3) payloadsHKey

-- ARGS:
--  (1) claimedScore
--  (2) score
--  (3 4 ...) id payload

local killed = {};

local i = 3;
local incr;
if KEYS[3] then
   incr=2;
else
   incr=1;
end

local j = 1;

while true do

   local id = ARGV[i];
   if id == nil then return killed; end

   local claimedScore = redis.call('hget', KEYS[2], id);
   if claimedScore == nil or claimedScore != ARGV[1] then
      killed[j] = -1;
   else
      redis.call('hdel', KEYS[2], ARGV[3]);
      killed[j] = redis.call('hset', KEYS[1], id, ARGV[2]);

      if KEYS[3] then
         redis.call('hset', KEYS[3], id, ARGV[i+1]);
      end
   end

   i = i + incr;
   j = j + 1;
end
