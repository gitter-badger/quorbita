package com.fabahaba.quorbita.luaq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.lua.LuaScript;
import com.fabahaba.quorbita.lua.LuaScriptData;
import com.google.common.base.MoreObjects;

import redis.clients.jedis.Jedis;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

public enum LuaQScripts implements LuaScript {

  PUBLISH,
  CLAIM,
  CHECKIN_CLAIMED,
  REMOVE,
  REMOVE_CLAIMED,
  REMOVE_DEAD,
  REPUBLISH,
  REPUBLISH_CLAIMED,
  REPUBLISH_DEAD,
  KILL,
  KILL_CLAIMED,
  SCAN_PAYLOADS,
  SCAN_PAYLOAD_STATES;

  private transient final LuaScriptData luaScript;

  private LuaQScripts() {

    this.luaScript =
        new LuaScriptData("/" + LuaQScripts.class.getSimpleName().toLowerCase(Locale.ENGLISH) + "/"
            + name() + ".lua");
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

    LuaScript.loadMissingScripts(jedisExecutor, LuaQScripts.values());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("luaScript", luaScript).toString();
  }
}
