-- Returns a list indicating if each job was removed (1) or not (0).

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

   local claimStamp = redis.call('hget', KEYS[1], id);
   if claimStamp == nil or claimStamp ~= ARGV[1] then
      removed[j] = 0;
   else
      redis.call('hdel', KEYS[1], id);
      redis.call('hdel', KEYS[2], id);
      removed[j] = 1;
   end

   i = i + 1;
   j = j + 1;
end
