-- Returns a list indicating if each job was removed (1) or not (0).

-- KEYS:
--  (1) claimedOrDeadHKey
--  (2) payloadsHKey

-- ARGS:
--  (1 ...) id

local removed = {};

for i, id in pairs(ARGV) do
   local deleteClaim = redis.call('hdel', KEYS[1], id);
   if deleteClaim == 0 then
      removed[i] = deleteClaim;
   else
      redis.call('hdel', KEYS[2], id);
      removed[i] = deleteClaim;
   end
end

return removed;
