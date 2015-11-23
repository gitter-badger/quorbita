package com.fabahaba.quorbita.reduceq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.lua.LuaScript;
import com.fabahaba.quorbita.lua.LuaScriptData;
import com.google.common.base.MoreObjects;

import redis.clients.jedis.Jedis;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

public enum ReduceQScripts implements LuaScript {

  PUBLISH_EPOCH_REDUCIBLE,
  PUBLISH_REDUCIBLE,
  PUBLISH_MAPPED_RESULT,
  CLAIM_REDUCIBLE,
  REPUBLISH_REDUCIBLE,
  REPUBLISH_DEAD_REDUCIBLE,
  KILL_REDUCIBLE;

  private transient final LuaScriptData luaScript;

  private ReduceQScripts() {

    this.luaScript =
        new LuaScriptData("/" + ReduceQScripts.class.getSimpleName().toLowerCase(Locale.ENGLISH)
            + "/" + name() + ".lua");
  }

  @Override
  public String getLuaScript() {
    return luaScript.getLuaScript();
  }

  @Override
  public String getSha1() {
    return luaScript.getSha1();
  }

  @Override
  public ByteBuffer getSha1Bytes() {
    return luaScript.getSha1Bytes();
  }

  @Override
  public Object eval(final JedisExecutor jedisExecutor, final int numRetries, final int keyCount,
      final byte[]... params) {

    return luaScript.eval(jedisExecutor, numRetries, keyCount, params);
  }

  @Override
  public Object eval(final JedisExecutor jedisExecutor, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return luaScript.eval(jedisExecutor, numRetries, keys, args);
  }

  @Override
  public Object eval(final Jedis jedis, final int keyCount, final byte[]... params) {

    return luaScript.eval(jedis, keyCount, params);
  }

  @Override
  public Object eval(final Jedis jedis, final List<byte[]> keys, final List<byte[]> args) {

    return luaScript.eval(jedis, keys, args);
  }

  public static void loadMissingScripts(final JedisExecutor jedisExecutor) {

    LuaScript.loadMissingScripts(jedisExecutor, ReduceQScripts.values());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("luaScript", luaScript).toString();
  }
}
