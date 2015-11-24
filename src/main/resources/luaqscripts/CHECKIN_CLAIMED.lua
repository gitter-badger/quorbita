-- Returns a list indicating if each job was checked in (1) or not (0).

-- KEYS:
--  (1) claimedHKey

-- ARGS:
--  (1) claimStamp
--  (2) newClaimStamp
--  (3 ...) id

local checkins = {};

local i = 3;
local j = 1;

while true do

   local id = ARGV[i];
   if id == nil then return checkins; end

   local claimStamp = redis.call('hget', KEYS[1], id);
   if claimStamp == nil or claimStamp ~= ARGV[1] then
      checkins[j] = 0;
   else
      checkins[j] = redis.call('hset', KEYS[1], id, ARGV[2]);
   end

   i = i + 1;
   j = j + 1;
end
