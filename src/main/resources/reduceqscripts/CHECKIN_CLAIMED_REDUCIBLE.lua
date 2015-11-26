-- Returns 1 if claim stamp was updated, 0 if no change was made, or -1 if no longer claimed

-- KEYS:
--  (1) claimStampsHKey

-- ARGS:
--  (1) claimStamp
--  (2) newClaimStamp
--  (3) reduceId

if redis.call('hget', KEYS[1], ARGV[3]) ~= ARGV[1] then return -1; end

return redis.call('hset', KEYS[1], ARGV[3], ARGV[2]);
