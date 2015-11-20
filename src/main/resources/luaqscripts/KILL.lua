-- Always returns 1
-- KEYS:
--  (1) deadHKey
--  (2) claimedIdsHKey
--  (3) payloadsHKey

-- ARGS:
--  (1) score
--  (2) id
--  (3) payload

if KEYS[3] then
   redis.call('hset', KEYS[3], ARGV[2], ARGV[3]);
end

redis.call('hset', KEYS[1], ARGV[2], ARGV[1]);
redis.call('hdel', KEYS[2], ARGV[2]);

return true;
