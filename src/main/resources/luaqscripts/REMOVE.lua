-- Returns a list indicating if each job was removed (1) or not (0).

-- KEYS:
--  (1) claimedOrDeadHKey
--  (2) payloadsHKey

-- ARGS:
--  (1 ...) id

local removed = {};

for i, id in pairs(ARGV) do
   local deleted = redis.call('hdel', KEYS[1], id);
   if deleted == 0 then
      removed[i] = deleted;
   else
      redis.call('hdel', KEYS[2], id);
      removed[i] = deleted;
   end
end

return removed;
