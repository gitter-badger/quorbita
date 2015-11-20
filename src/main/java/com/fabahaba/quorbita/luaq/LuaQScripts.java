package com.fabahaba.quorbita.luaq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.LuaScript;
import com.fabahaba.quorbita.LuaScriptData;
import com.google.common.base.MoreObjects;

import java.nio.ByteBuffer;
import java.util.Locale;

public enum LuaQScripts implements LuaScript {

  CLAIM,
  MPUBLISH,
  REPUBLISH,
  KILL,
  SCAN_PAYLOADS,
  SCAN_PAYLOAD_STATES;

  private transient final LuaScriptData luaScript;

  private LuaQScripts() {

    this.luaScript =
        new LuaScriptData("src/main/resources/"
            + LuaQScripts.class.getSimpleName().toLowerCase(Locale.ENGLISH) + "/" + name() + ".lua");
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

  public static void loadMissingScripts(final JedisExecutor jedisExecutor) {

    LuaScript.loadMissingScripts(jedisExecutor, LuaQScripts.values());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("luaScript", luaScript).toString();
  }
}
