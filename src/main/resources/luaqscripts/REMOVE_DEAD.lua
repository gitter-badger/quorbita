-- Returns a list indicating if each job was removed (1), already removed (0), published (-1), or claimed (-2).

-- KEYS:
--  (1) publishedZKey
--  (2) claimedHKey
--  (3) deadHKey
--  (4) payloadsHKey

-- ARGS:
--  (1 ...) id

local removed = {};

for i, id in pairs(ARGV) do

   if redis.call('hdel', KEYS[3], id) == 0 then
      removed[i] = 0;
   else
      if redis.call('zscore', KEYS[1], id) then
         removed[i] = -1;
      elseif redis.call('hexists', KEYS[2], id) == 1 then
         removed[i] = -2;
      else
         redis.call('hdel', KEYS[4], id);
         removed[i] = 1;
      end
   end
end

return removed;
