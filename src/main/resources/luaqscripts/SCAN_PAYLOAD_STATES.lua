-- Returns the current cursor and the state of wether a payload is published, claimed, dead or otherwise orphaned.

--  KEYS:
--  (1) payloadsHKey
--  (2) publishedZKey
--  (3) claimedIdHKey
--  (4) deadHKey

-- ARGS:
--  (1) cursor
--  (2) count

local scanResult = redis.call('hscan', KEYS[1], ARGV[1], 'COUNT', ARGV[2]);
local idPayload = scanResult[2];

local i = 1;
local j = 1;
local idPayloadStates = {};

while true do

   local id = idPayload[i];
   if id == nil then
      return {scanResult[1], idPayloadStates};
   end

   if redis.call('zscore', KEYS[2], id) then
      idPayloadStates[j] = {id, true, false, false};
   else if redis.call('hexists', KEYS[3], id) > 0 then
      idPayloadStates[j] = {id, false, true, false};
   else if redis.call('hexists', KEYS[4], id) > 0 then
      idPayloadStates[j] = {id, false, false, true};
   else
      idPayloadStates[j] = {id, false, false, false};
   end

   i = i + 2;
   j = j + 1;

end
