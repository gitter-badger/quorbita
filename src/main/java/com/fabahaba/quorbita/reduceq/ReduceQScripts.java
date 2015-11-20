package com.fabahaba.quorbita.reduceq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.LuaScript;
import com.fabahaba.quorbita.LuaScriptData;
import com.fabahaba.quorbita.luaq.LuaQScripts;
import com.google.common.base.MoreObjects;

import java.nio.ByteBuffer;
import java.util.Locale;

public enum ReduceQScripts implements LuaScript {

  PUBLISH_EPOCH_REDUCIBLE,
  PUBLISH_REDUCIBLE;

  private transient final LuaScriptData luaScript;

  private ReduceQScripts() {

    this.luaScript =
        new LuaScriptData("src/main/resources/"
            + ReduceQScripts.class.getSimpleName().toLowerCase(Locale.ENGLISH) + "/" + name()
            + ".lua");
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
