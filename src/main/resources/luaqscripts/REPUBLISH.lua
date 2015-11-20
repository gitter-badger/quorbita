-- Always returns 1

-- KEYS:
--  (1) publishedZKey
--  (2) claimedOrDeadHKey
--  (3) notifyLKey
--  (4) payloadsHKey


-- ARGS:
--  (1) score
--  (2) id
--  (3) payload

if KEYS[3] then
   redis.call('hset', KEYS[4], ARGV[2], ARGV[3]);
end

redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]);
redis.call('hdel', KEYS[2], ARGV[2]);
redis.call('lpush', KEYS[3], ARGV[2]);

return true;
