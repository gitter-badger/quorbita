-- Returns a list indicating if each job was removed (1) or not (0) or no longer claimed (-1).

-- KEYS:
--  (1) claimedHKey
--  (2) payloadsHKey

-- ARGS:
--  (1) claimStamp
--  (2 ...) id

local removed = {};

local i = 2;
local j = 1;

while true do

   local id = ARGV[i];
   if id == nil then return removed; end

   if redis.call('hget', KEYS[1], id) ~= ARGV[1] then
      removed[j] = -1;
   else
      redis.call('hdel', KEYS[2], id);
      removed[j] = redis.call('hdel', KEYS[1], id);
   end

   i = i + 1;
   j = j + 1;
end
