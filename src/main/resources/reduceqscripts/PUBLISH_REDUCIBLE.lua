-- PUBLISH_REDUCIBLE

-- KEYS:
--  (1) publishedZKey
--  (2) claimedHKey
--  (3) payloadsHKey
--  (4) notifyLKey
--  (5) pendingMappedSKey
--  (6) mappedResultsHKey
--  (7) publishedReduceZKey
--  (8) claimedReduceHKey

-- ARGS:
--  (1) score
--  (2) reduceWeight
--  (3) reduceId
--  (4 5 ...) id payload


local numPublished = 0;
local weight = 0;
local i = 4;

while true do

   local id = ARGV[i];
   if id == nil then break end

   if redis.call('hexists', KEYS[2], id) == 0 then
      if redis.call('hexists', KEYS[6], id) == 0 then
         if redis.call('zadd', KEYS[1], 'NX', ARGV[1], id) > 0 then
            redis.call('hsetnx', KEYS[3], id, ARGV[i+1]);
            redis.call('sadd', KEYS[5], id);
            redis.call('lpush', KEYS[4], id);
            numPublished = numPublished + 1;
            weight = weight + ARGV[2]
         end
      end
   end

   i = i + 2;
end

if redis.call('hexists', KEYS[8], ARGV[3]) == 0 then
   redis.call('zadd', KEYS[7], 'XX', 'INCR', weight, ARGV[3]);
else
   redis.call('hincrby', KEYS[8], ARGV[3], weight);
end

return numPublished;
