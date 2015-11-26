-- Returns 1 if killed, -1 if id was no longer claimed, or 0 if already dead for each id.

-- KEYS:
--  (1) deadHKey
--  (2) claimedHKey
--  (3) payloadsHKey

-- ARGS:
--  (1) deadStamp
--  (2 3 ...) id payload

local killed = {};

local i = 2;
local incr = KEYS[3] and 2 or 1;

local j = 1;

while true do

   local id = ARGV[i];
   if id == nil then return killed; end

   if redis.call('hdel', KEYS[2], id) == 0 then
      killed[j] = -1;
   else
      local killed = redis.call('hsetnx', KEYS[1], id, ARGV[1]);

      if KEYS[3] and killed > 0 then
         redis.call('hset', KEYS[3], id, ARGV[i+1]);
      end

      killed[j] = killed;
   end

   i = i + incr;
   j = j + 1;
end
