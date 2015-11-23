-- Returns a list indicating if each job was checked in (1) or not (0).

-- KEYS:
--  (1) claimedHKey

-- ARGS:
--  (1) score
--  (2 ...) id

local checkins = {};

local i = 2;

while true do

   local id = ARGV[i];
   if id == nil then return checkins; end

   if redis.call('hexists', KEYS[1], id) == 0 then
      checkins[i] = 0;
   else
      checkins[i] = redis.call('hset', KEYS[1], id, ARGV[1]);
   end

   i = i + 1;
end
