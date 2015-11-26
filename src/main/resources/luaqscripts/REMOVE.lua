-- Returns a list indicating if each job was removed (1) or not (0).

-- KEYS:
--  (1) claimedHKey
--  (2) payloadsHKey

-- ARGS:
--  (1 ...) id

local removed = {};

for i, id in pairs(ARGV) do

   if redis.call('hdel', KEYS[1], id) == 0 then
      removed[i] = 0;
   else
      redis.call('hdel', KEYS[2], id);
      removed[i] = 1;
   end
end

return removed;
