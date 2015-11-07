package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;

import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

public final class LuaQFunctions {

  private LuaQFunctions() {}

  public static Long publish(final JedisExecutor jedisExecutor, final String id,
      final String payload, final String qKey, final String claimedQKey,
      final String payloadsHashKey, final String notifyList) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(
        LuaQScripts.PUBLISH.getLuaScript(), 4, qKey, claimedQKey, payloadsHashKey, notifyList,
        String.valueOf(System.currentTimeMillis()), id, payload));
  }

  public static Long republish(final JedisExecutor jedisExecutor, final String id,
      final String payload, final String qKey, final String claimedQKey,
      final String payloadsHashKey, final String notifyList) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(
        LuaQScripts.REPUBLISH.getLuaScript(), 4, qKey, claimedQKey, payloadsHashKey, notifyList,
        String.valueOf(System.currentTimeMillis()), id, payload));
  }

  public static List<String> claim(final JedisExecutor jedisExecutor, final String qKey,
      final String claimedQKey, final String payloadsHashKey, final String notifyList,
      final int timeoutSeconds) {

    final List<String> nonBlockingGet =
        LuaQFunctions.nonBlockingClaim(jedisExecutor, qKey, claimedQKey, payloadsHashKey,
            notifyList);

    if (nonBlockingGet.get(1) != null)
      return nonBlockingGet;

    return LuaQFunctions.blockingClaim(jedisExecutor, qKey, claimedQKey, payloadsHashKey,
        notifyList, timeoutSeconds);
  }

  @SuppressWarnings("unchecked")
  public static List<String> blockingClaim(final JedisExecutor jedisExecutor, final String qKey,
      final String claimedQKey, final String payloadsHashKey, final String notifyList,
      final int timeoutSeconds) {

    return (List<String>) jedisExecutor.applyPipelinedTransaction(pipeline -> {
      pipeline.blpop(timeoutSeconds, notifyList);
      return pipeline.evalsha(LuaQScripts.BLOCKED_CLAIM.getLuaScript(), 3, qKey, claimedQKey,
          payloadsHashKey);
    });
  }

  @SuppressWarnings("unchecked")
  public static List<String> nonBlockingClaim(final JedisExecutor jedisExecutor, final String qKey,
      final String claimedQKey, final String notifyList, final String payloadsHashKey) {

    return (List<String>) jedisExecutor.applyJedis(jedis -> jedis.evalsha(
        LuaQScripts.CLAIM.getLuaScript(), 4, qKey, claimedQKey, payloadsHashKey, notifyList,
        String.valueOf(System.currentTimeMillis())));
  }

  public static long remove(final JedisExecutor jedisExecutor, final String qKey,
      final String payloadsHashKey, final int numRetries, final String... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipeline(pipeline -> {
      for (final String id : ids) {
        removedResponses.add(pipeline.zrem(qKey, id));
        pipeline.hdel(payloadsHashKey, id);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }
}
