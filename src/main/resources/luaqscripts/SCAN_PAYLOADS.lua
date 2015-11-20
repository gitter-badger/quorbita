-- Returns the current cusor and a list of byte[] lists containing an id, score and payload each.

-- KEYS:
--  (1) key
--  (2) payloadsHKey

-- ARGS:
--  (1) scanCommand
--  (2) cursor
--  (3) count

local scanResult = redis.call(ARGV[1], KEYS[1], ARGV[2], 'COUNT', ARGV[3]);
local idScores = scanResult[2];

local i = 1;
local j = 1;
local idScoresPaylods = {};

while true do

   local id = idScores[i];
   if id == nil then
      return {scanResult[1], idScoresPaylods};
   end

   idScoresPaylods[j] = {id, idScores[i+1], redis.call('hget', KEYS[2], id)};

   i = i + 2;
   j = j + 1;

end
