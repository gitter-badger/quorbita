-- Returns 1 if the result was set, or 0 if a result already exists.
-- Notifies idle published reduce queue if zero mapped results are pending.

-- KEYS:
--  (1) publishedReduceZKey
--  (2) claimedReduceHKey
--  (3) mappedResultsHKey
--  (4) pendingMappedSKey
--  (5) notifyReducedLKey
--  (6) notifyMappedResultsLKey
--  (7) claimedHKey
--  (8) payloadsHKey

-- ARGS:
--  (1) reduceId
--  (2) reduceWeight
--  (3) id
--  (4) resultsPayload

local setResult = redis.call('hsetnx', KEYS[3], ARGV[3], ARGV[4]);
redis.call('srem', KEYS[4], ARGV[3]);

if setResult > 0 then
   redis.call('lpush', KEYS[6], ARGV[3]);

   local claimed = redis.call('hexists', KEYS[2], ARGV[1]);
   if claimed == 0 then
      local changed = redis.call('zadd', KEYS[1], 'XX', 'CH', 'INCR', -ARGV[2], ARGV[1]);
      if changed > 0 then
         local numPending = redis.call('scard', KEYS[4]);
         if numPending == 0 then
            redis.call('lpush', KEYS[5], ARGV[1]);
         end
      end
   else
      redis.call('hincrby', KEYS[2], ARGV[1], -ARGV[2]);
   end
end

redis.call('hdel', KEYS[7], ARGV[3]);
redis.call('hdel', KEYS[8], ARGV[3]);

return setResult;
