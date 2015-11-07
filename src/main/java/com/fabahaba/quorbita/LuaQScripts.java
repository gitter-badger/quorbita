package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.base.MoreObjects;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

public enum LuaQScripts {

  // Returns the claimed id and payload, or just the id if no payload key is given.
  // KEYS: idsKey claimedIdsKey payloadsHashKey notifyList
  // ARGS: score
  //
  // while true do
  // __ local id = redis.call('zrange', KEYS[1], 0, 0)[1];
  // __ if id == nil then return {false, false}; end
  //
  // __ redis.call('lpop', KEYS[4]);
  //
  // __ local numAdded = redis.call('zadd', KEYS[2], 'NX', ARGV[1], id);
  // __ redis.call('zremrangebyrank', KEYS[1], 0, 0);
  //
  // __ if numAdded > 0 then
  // ____ if KEYS[3] then
  // ______ return {id, redis.call('hget', KEYS[3], id)};
  // ____ end
  // ____ return {id, false};
  // __ end
  // end
  CLAIM(
      "while true do local id = redis.call('zrange', KEYS[1], 0, 0)[1]; if id == nil then return {false, false}; end redis.call('lpop', KEYS[4]); local numAdded = redis.call('zadd', KEYS[2], 'NX', ARGV[1], id); redis.call('zremrangebyrank', KEYS[1], 0, 0); if numAdded > 0 then if KEYS[3] then return {id, redis.call('hget', KEYS[3], id)}; end return {id, false}; end end"),

  // Returns 1 if added, 0 if already exists and -1 if it is already claimed.
  // KEYS: idsKey claimedIdsKey payloadsHashKey notifyList
  // ARGS: score id payload
  //
  // local isClaimed = redis.call('zscore', KEYS[2], ARGV[2]);
  // if isClaimed then return -1; end
  // if ARGV[3] then redis.call('hsetnx', KEYS[3], ARGV[2], ARGV[3]); end
  // local numAdded = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]);
  // if numAdded > 0 then redis.call('lpush', KEYS[4], ARGV[2]); end
  // return numAdded;
  PUBLISH(
      "local isClaimed = redis.call('zscore', KEYS[2], ARGV[2]); if isClaimed then return -1; end if ARGV[3] then redis.call('hsetnx', KEYS[3], ARGV[2], ARGV[3]); end local numAdded = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]); if numAdded > 0 then redis.call('lpush', KEYS[4], ARGV[2]); end return numAdded;"),

  // Returns 1 if added, 0 if already exists and -1 if it is already claimed.
  // KEYS: idsKey claimedIdsKey payloadsHashKey notifyList
  // ARGS: score id payload
  //
  // local numPublished = 0;
  // local i = 2;
  // while ARGV[i] do
  // __ local isClaimed = redis.call('zscore', KEYS[2], ARGV[i]);
  // __ if not isClaimed then
  // ____ local pi = i+1;
  // ____ if ARGV[pi] then redis.call('hsetnx', KEYS[3], ARGV[i], ARGV[pi]); end
  // ____ local numAdded = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[i]);
  // ____ if numAdded > 0 then
  // ______ redis.call('lpush', KEYS[4], ARGV[i]);
  // ______ numPublished = numPublished + 1;
  // ____ end
  // __ end
  // __ i = i + 2;
  // end
  // return numPublished;
  MPUBLISH(
      "local numPublished = 0; local i = 2; while ARGV[i] do local isClaimed = redis.call('zscore', KEYS[2], ARGV[i]); if not isClaimed then local pi = i+1; if ARGV[pi] then redis.call('hsetnx', KEYS[3], ARGV[i], ARGV[pi]); end local numAdded = redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[i]); if numAdded > 0 then redis.call('lpush', KEYS[4], ARGV[i]); numPublished = numPublished + 1; end end i = i + 2; end return numPublished;"),

  // Always returns 1
  // KEYS: idsKey claimedIdsKey payloadsHashKey notifyList
  // ARGS: score id payload
  //
  // if ARGV[3] then redis.call('hset', KEYS[3], ARGV[2], ARGV[3]); end
  // redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]);
  // redis.call('zrem', KEYS[2], ARGV[2]);
  // redis.call('lpush', KEYS[4], ARGV[2]);
  // return true;
  REPUBLISH(
      "if ARGV[3] then redis.call('hset', KEYS[3], ARGV[2], ARGV[3]); end redis.call('zadd', KEYS[1], 'NX', ARGV[1], ARGV[2]); redis.call('zrem', KEYS[2], ARGV[2]); redis.call('lpush', KEYS[4], ARGV[2]); return true;");

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
          try {
            jedis.scriptLoad(scripts[i].getLuaScript());
          } catch (final Exception e) {
            System.out.print(scripts[i].getLuaScript());
            System.out.print(scripts[i].name());
            System.out.print(scripts[i]);
            throw e;
          }
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