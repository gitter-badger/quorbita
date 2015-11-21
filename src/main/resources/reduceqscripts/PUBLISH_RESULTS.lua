-- Returns 1 if the result was set, or 0 if a result already exists.
-- Notifies idle published reduce queue if at most 'notifyReducedThreshold' pending results exist.

-- KEYS:
--  (1) publishedReduceZKey
--  (2) mappedResultsHKey
--  (3) pendingMappedSKey
--  (4) notifyReducedLKey
--  (5) notifyMappedResultsLKey
--  (6) claimedHKey
--  (7) payloadsHKey

-- ARGS:
--  (1) reduceId
--  (2) id
--  (3) resultsPayload
--  (4) notifyReducedThreshold

local setResult = redis.call('hsetnx', KEYS[2], ARGV[2], ARGV[3]);
redis.call('sdel', KEYS[3], ARGV[2]);

if setResult > 0 then
   redis.call('lpush', KEYS[5], ARGV[2]);

   local numPending = redis.call('scard', KEYS[3]);
   if numPending <= ARGV[4] then
      local isPublished = redis.call('zscore', KEYS[1], ARGV[1]);
      if isPublished then
         redis.call('lpush', KEYS[4], ARGV[2]);
      end
   end
end

redis.call('hdel', KEYS[6], ARGV[2]);
redis.call('hdel', KEYS[7], ARGV[2]);

return setResult;
