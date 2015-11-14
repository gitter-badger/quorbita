package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.base.MoreObjects;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

public enum LuaQScripts {

  // CLAIM Returns at most 'limit' claimed id payload pairs
  // KEYS: idsZKey claimedIdsHKey notifyLKey payloadsHKey
  // ARGS: score limit
  //
  // local idPayloads = {}
  // for i = 1, ARGV[2], 1 do
  // __ local id = redis.call('zrange', KEYS[1], 0, 0)[1];
  // __ if id == nil then return idPayloads; end
  //
  // __ redis.call('lpop', KEYS[3]);
  //
  // __ local claimed = redis.call('hsetnx', KEYS[2], id, ARGV[1]);
  // __ redis.call('zremrangebyrank', KEYS[1], 0, 0);
  //
  // __ if claimed > 0 then
  // ____ if KEYS[4] then
  // ______ idPayloads[i] = {id, redis.call('hget', KEYS[4], id)};
  // ____ else
  // ______ idPayloads[i] = {id, false};
  // ____ end
  // __ else
  // ____ i = i - 1;
  // __ end
  // end
  // return idPayloads;
  CLAIM(
      "local idPayloads = {} for i = 1, ARGV[2], 1 do local id = redis.call('zrange', KEYS[1], 0, 0)[1]; if id == nil then return idPayloads; end redis.call('lpop', KEYS[3]); local claimed = redis.call('hsetnx', KEYS[2], id, ARGV[1]); redis.call('zremrangebyrank', KEYS[1], 0, 0); if claimed > 0 then if KEYS[4] then idPayloads[i] = {id, redis.call('hget', KEYS[4], id)}; else idPayloads[i] = {id, false}; end else i = i - 1; end end return idPayloads;"),

  // Returns 1 if added, 0 if already exists and -1 if it is already claimed.
  // KEYS: idsZKey claimedIdsHKey payloadsHKey notifyLKey
  // ARGS: score id payload
  //
  // local claimed = redis.call('hexists', KEYS[2], ARGV[2]);
  // if claimed > 0 then return -1; end
  // if ARGV[3] then redis.call('hsetnx', KEYS[3], ARGV[2], ARGV[3]); end
  // local added = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]);
  // if added > 0 then redis.call('lpush', KEYS[4], ARGV[2]); end
  // return added;
  PUBLISH(
      "local claimed = redis.call('hexists', KEYS[2], ARGV[2]); if claimed > 0 then return -1; end if ARGV[3] then redis.call('hsetnx', KEYS[3], ARGV[2], ARGV[3]); end local added = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]); if added > 0 then redis.call('lpush', KEYS[4], ARGV[2]); end return added;"),

  // Returns 1 if added, 0 if already exists and -1 if it is already claimed.
  // KEYS: idsZKey claimedIdsHKey payloadsHKey notifyLKey
  // ARGS: score id payload
  //
  // local numPublished = 0;
  // local i = 2;
  // while ARGV[i] do
  // __ local claimed = redis.call('hexists', KEYS[2], ARGV[i]);
  // __ if claimed == 0 then
  // ____ local pi = i+1;
  // ____ if ARGV[pi] then redis.call('hsetnx', KEYS[3], ARGV[i], ARGV[pi]); end
  // ____ local added = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[i]);
  // ____ if added > 0 then
  // ______ redis.call('lpush', KEYS[4], ARGV[i]);
  // ______ numPublished = numPublished + 1;
  // ____ end
  // __ end
  // __ i = i + 2;
  // end
  // return numPublished;
  MPUBLISH(
      "local numPublished = 0; local i = 2; while ARGV[i] do local claimed = redis.call('hexists', KEYS[2], ARGV[i]); if claimed == 0 then local pi = i+1; if ARGV[pi] then redis.call('hsetnx', KEYS[3], ARGV[i], ARGV[pi]); end local added = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[i]); if added > 0 then redis.call('lpush', KEYS[4], ARGV[i]); numPublished = numPublished + 1; end end i = i + 2; end return numPublished;"),

  // Always returns 1
  // KEYS: idsZKey hKey payloadsHKey notifyLKey
  // ARGS: score id payload
  //
  // if ARGV[3] then redis.call('hset', KEYS[3], ARGV[2], ARGV[3]); end
  // redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]);
  // redis.call('hdel', KEYS[2], ARGV[2]);
  // redis.call('lpush', KEYS[4], ARGV[2]);
  // return true;
  REPUBLISH(
      "if ARGV[3] then redis.call('hset', KEYS[3], ARGV[2], ARGV[3]); end redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]); redis.call('hdel', KEYS[2], ARGV[2]); redis.call('lpush', KEYS[4], ARGV[2]); return true;"),

  // Always returns 1
  // KEYS: deadHKey claimedIdsHKey payloadsHKey
  // ARGS: score id payload
  //
  // if ARGV[3] then redis.call('hset', KEYS[3], ARGV[2], ARGV[3]); end
  // redis.call('hset', KEYS[1], ARGV[2], ARGV[1]);
  // redis.call('hdel', KEYS[2], ARGV[2]);
  // return true;
  KILL(
      "if ARGV[3] then redis.call('hset', KEYS[3], ARGV[2], ARGV[3]); end redis.call('hset', KEYS[1], ARGV[2], ARGV[1]); redis.call('hdel', KEYS[2], ARGV[2]); return true;"),

  // Returns the current cusor and a list of byte[] lists containing an id, score and payload each.
  // KEYS: key payloadsHKey
  // ARGS: scanCommand cursor count
  //
  // local scanResult = redis.call(ARGV[1], KEYS[1], ARGV[2], 'COUNT', ARGV[3]);
  // local idScores = scanResult[2];
  // local i = 1;
  // local j = 1;
  // local idScoresPaylods = {};
  // while true do
  // __ local id = idScores[i];
  // __ if id == nil then return {scanResult[1], idScoresPaylods}; end
  // __ idScoresPaylods[j] = {id, idScores[i+1], redis.call('hget', KEYS[2], id)};
  // __ i = i + 2;
  // __ j = j + 1;
  // end
  SCAN_PAYLOADS(
      "local scanResult = redis.call(ARGV[1], KEYS[1], ARGV[2], 'COUNT', ARGV[3]); local idScores = scanResult[2]; local i = 1; local j = 1; local idScoresPaylods = {}; while true do local id = idScores[i]; if id == nil then return {scanResult[1], idScoresPaylods}; end idScoresPaylods[j] = {id, idScores[i+1], redis.call('hget', KEYS[2], id)}; i = i + 2; j = j + 1; end"),

  // Returns the current cursor and the state of wether a payload is published, claimed, dead or
  // otherwise orphaned.
  //
  // KEYS: payloadsHKey idZKey claimedIdHKey deadHKey
  // ARGS: cursor count
  //
  // local scanResult = redis.call('hscan', KEYS[1], ARGV[1], 'COUNT', ARGV[2]);
  // local idPayload = scanResult[2];
  // local i = 1;
  // local j = 1;
  // local idPayloadStates = {};
  // while true do
  // __ local id = idPayload[i];
  // __ if id == nil then return {scanResult[1], idPayloadStates}; end
  // __ local isPublished = redis.call('zscore', KEYS[2], id);
  // __ if isPublished then
  // ____ idPayloadStates[j] = {id, true, false, false};
  // __ else
  // ____ local isClaimed = redis.call('hexists', KEYS[3], id);
  // ____ if isClaimed > 0 then
  // ______ idPayloadStates[j] = {id, false, true, false};
  // ____ else
  // ______ local isDead = redis.call('hexists', KEYS[4], id);
  // ______ if isDead > 0 then
  // ________ idPayloadStates[j] = {id, false, false, true};
  // ______ else
  // ________ idPayloadStates[j] = {id, false, false, false};
  // ______ end
  // ____ end
  // __end
  // i = i + 2;
  // j = j + 1;
  // end
  SCAN_PAYLOAD_STATES(
      "local scanResult = redis.call('hscan', KEYS[1], ARGV[1], 'COUNT', ARGV[2]); local idPayload = scanResult[2]; local i = 1; local j = 1; local idPayloadStates = {}; while true do local id = idPayload[i]; if id == nil then return {scanResult[1], idPayloadStates}; end local isPublished = redis.call('zscore', KEYS[2], id); if isPublished then idPayloadStates[j] = {id, true, false, false}; else local isClaimed = redis.call('hexists', KEYS[3], id); if isClaimed > 0 then idPayloadStates[j] = {id, false, true, false}; else local isDead = redis.call('hexists', KEYS[4], id); if isDead > 0 then idPayloadStates[j] = {id, false, false, true}; else idPayloadStates[j] = {id, false, false, false}; end end end i = i + 2; j = j + 1; end");

  private transient final String luaScript;
  private transient final String sha1;
  private transient final ByteBuffer sha1Bytes;

  private LuaQScripts(final String luaScript) {

    this.luaScript = luaScript;
    this.sha1 = Hashing.sha1().hashString(luaScript, StandardCharsets.UTF_8).toString();
    this.sha1Bytes = ByteBuffer.wrap(sha1.getBytes(StandardCharsets.UTF_8));
  }

  public String getLuaScript() {
    return luaScript;
  }

  public String getSha1() {
    return this.sha1;
  }

  public ByteBuffer getSha1Bytes() {
    return sha1Bytes;
  }

  public static void loadMissingScripts(final JedisExecutor jedisExecutor) {

    final LuaQScripts[] scripts = LuaQScripts.values();

    final byte[][] scriptSha1Bytes =
        Stream.of(scripts).map(LuaQScripts::getSha1Bytes).map(ByteBuffer::array)
            .toArray(byte[][]::new);

    jedisExecutor.acceptJedis(jedis -> {
      final List<Long> existResults = jedis.scriptExists(scriptSha1Bytes);

      for (int i = 0; i < existResults.size(); i++) {
        if (existResults.get(i) == 0) {
          jedis.scriptLoad(scripts[i].getLuaScript());
        }
      }
    });
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("sha1", sha1).add("luaScript", luaScript)
        .toString();
  }
}
