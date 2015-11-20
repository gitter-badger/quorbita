package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

public interface LuaScript {

  public String getLuaScript();

  public String getSha1();

  public ByteBuffer getSha1Bytes();

  default void loadIfMissing(final JedisExecutor jedisExecutor) {

    LuaScript.loadMissingScripts(jedisExecutor, this);
  }

  public static void loadMissingScripts(final JedisExecutor jedisExecutor,
      final LuaScript... luaScripts) {

    final byte[][] scriptSha1Bytes =
        Stream.of(luaScripts).map(LuaScript::getSha1Bytes).map(ByteBuffer::array)
            .toArray(byte[][]::new);

    jedisExecutor.acceptJedis(jedis -> {
      final List<Long> existResults = jedis.scriptExists(scriptSha1Bytes);

      for (int i = 0; i < existResults.size(); i++) {
        if (existResults.get(i) == 0) {
          jedis.scriptLoad(luaScripts[i].getLuaScript());
        }
      }
    });
  }
}
