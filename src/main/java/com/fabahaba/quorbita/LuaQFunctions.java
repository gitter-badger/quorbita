package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public final class LuaQFunctions {

  private LuaQFunctions() {}

  public static Long publish(final JedisExecutor jedisExecutor, final byte[] id,
      final byte[] payload, final byte[] publishedQKey, final byte[] claimedQKey,
      final byte[] payloadsHashKey, final byte[] notifyListKey) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.PUBLISH
        .getSha1Bytes().array(), 4, publishedQKey, claimedQKey, payloadsHashKey, notifyListKey,
        LuaQFunctions.getEpochMillisBytes(), id, payload));
  }

  public static Long mpublish(final JedisExecutor jedisExecutor, final List<byte[]> keys,
      final Collection<byte[]> idPayloads) {

    final List<byte[]> args = new ArrayList<>(idPayloads.size() + 1);
    args.add(LuaQFunctions.getEpochMillisBytes());
    args.addAll(idPayloads);

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.MPUBLISH
        .getSha1Bytes().array(), keys, args));
  }

  public static Long republishAs(final JedisExecutor jedisExecutor, final byte[] id,
      final byte[] payload, final byte[] publishedQKey, final byte[] claimedQKey,
      final byte[] payloadsHashKey, final byte[] notifyListKey) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.REPUBLISH
        .getSha1Bytes().array(), 4, publishedQKey, claimedQKey, payloadsHashKey, notifyListKey,
        LuaQFunctions.getEpochMillisBytes(), id, payload));
  }

  public static Long republish(final JedisExecutor jedisExecutor, final byte[] id,
      final byte[] publishedQKey, final byte[] claimedQKey, final byte[] payloadsHashKey,
      final byte[] notifyListKey) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.REPUBLISH
        .getSha1Bytes().array(), 4, publishedQKey, claimedQKey, payloadsHashKey, notifyListKey,
        LuaQFunctions.getEpochMillisBytes(), id));
  }

  public static List<byte[]> claim(final JedisExecutor jedisExecutor, final byte[] publishedQKey,
      final byte[] claimedQKey, final byte[] payloadsHashKey, final byte[] notifyListKey,
      final int timeoutSeconds) {

    final List<byte[]> nonBlockingGet =
        LuaQFunctions.nonBlockingClaim(jedisExecutor, publishedQKey, claimedQKey, payloadsHashKey,
            notifyListKey);

    if (nonBlockingGet.get(0) != null)
      return nonBlockingGet;

    return LuaQFunctions.blockingClaim(jedisExecutor, publishedQKey, claimedQKey, payloadsHashKey,
        notifyListKey, timeoutSeconds);
  }

  private static final List<byte[]> EMPTY_Q_SENTINEL = Collections.unmodifiableList(Lists
      .newArrayList(null, null));

  public static List<byte[]> blockingClaim(final JedisExecutor jedisExecutor,
      final byte[] publishedQKey, final byte[] claimedQKey, final byte[] payloadsHashKey,
      final byte[] notifyListKey, final int timeoutSeconds) {

    return jedisExecutor.applyJedis(jedis -> {

      final List<byte[]> events = jedis.blpop(timeoutSeconds, notifyListKey);

      return events == null ? EMPTY_Q_SENTINEL : LuaQFunctions.claim(jedis, publishedQKey,
          claimedQKey, payloadsHashKey, notifyListKey);
    });
  }

  public static List<byte[]> nonBlockingClaim(final JedisExecutor jedisExecutor,
      final byte[] publishedQKey, final byte[] claimedQKey, final byte[] payloadsHashKey,
      final byte[] notifyListKey) {

    return jedisExecutor.applyJedis(jedis -> LuaQFunctions.claim(jedis, publishedQKey, claimedQKey,
        payloadsHashKey, notifyListKey));
  }

  @SuppressWarnings("unchecked")
  private static List<byte[]> claim(final Jedis jedis, final byte[] publishedQKey,
      final byte[] claimedQKey, final byte[] payloadsHashKey, final byte[] notifyListKey) {

    return (List<byte[]>) jedis.evalsha(LuaQScripts.CLAIM.getSha1Bytes().array(), 4, publishedQKey,
        claimedQKey, payloadsHashKey, notifyListKey, LuaQFunctions.getEpochMillisBytes());
  }

  public static String getEpochMillisString() {

    return String.valueOf(System.currentTimeMillis());
  }

  public static byte[] getEpochMillisBytes() {

    return LuaQFunctions.getEpochMillisString().getBytes(StandardCharsets.UTF_8);
  }

  public static long remove(final JedisExecutor jedisExecutor, final byte[] publishedQKey,
      final byte[] payloadsHashKey, final int numRetries, final String... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final String id : ids) {

        final byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);

        removedResponses.add(pipeline.zrem(publishedQKey, idBytes));
        pipeline.hdel(payloadsHashKey, idBytes);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static long remove(final JedisExecutor jedisExecutor, final byte[] publishedQKey,
      final byte[] claimedQKey, final byte[] payloadsHashKey, final int numRetries,
      final String... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final String id : ids) {

        final byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);

        removedResponses.add(pipeline.zrem(publishedQKey, idBytes));
        removedResponses.add(pipeline.zrem(claimedQKey, idBytes));
        pipeline.hdel(payloadsHashKey, idBytes);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static void clear(final JedisExecutor jedisExecutor, final byte[] publishedQKey,
      final byte[] claimedQKey, final byte[] payloadsHashKey, final byte[] notifyListKey,
      final int numRetries) {

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {
      pipeline.del(publishedQKey);
      pipeline.del(claimedQKey);
      pipeline.del(payloadsHashKey);
      pipeline.del(notifyListKey);
    }, numRetries);
  }

  public static List<byte[]> removeOrphanedPayloads(final JedisExecutor jedisExecutor,
      final byte[] publishedQKey, final byte[] claimedQKey, final byte[] payloadsHashKey,
      final int batchSize) {

    final Set<byte[]> payloadKeys = jedisExecutor.applyJedis(jedis -> jedis.hkeys(payloadsHashKey));

    if (payloadKeys == null || payloadKeys.isEmpty())
      return ImmutableList.of();

    final List<byte[]> removePayloadKeys = new LinkedList<>();

    Iterables.partition(payloadKeys, batchSize).forEach(payloadKeysBatch -> {

      final List<Response<Double>> scores = new ArrayList<>(payloadKeysBatch.size() * 2);

      jedisExecutor.acceptPipeline(pipeline -> payloadKeysBatch.stream().forEach(payloadKey -> {
        scores.add(pipeline.zscore(publishedQKey, payloadKey));
        scores.add(pipeline.zscore(claimedQKey, payloadKey));
      }));

      for (int p = 0, s = 0; p < payloadKeysBatch.size(); p++) {

        final Double publishedScore = scores.get(s++).get();
        final Double claimedScore = scores.get(s++).get();

        if (publishedScore == null && claimedScore == null) {
          removePayloadKeys.add(payloadKeysBatch.get(p));
        }
      }
    });

    if (!removePayloadKeys.isEmpty()) {
      jedisExecutor.applyJedis(jedis -> jedis.hdel(payloadsHashKey,
          removePayloadKeys.toArray(new byte[removePayloadKeys.size()][])));
    }

    return removePayloadKeys;
  }
}