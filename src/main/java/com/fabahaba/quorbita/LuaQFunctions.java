package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public final class LuaQFunctions {

  private LuaQFunctions() {}

  public static Long publish(final JedisExecutor jedisExecutor, final byte[] id,
      final byte[] payload, final byte[] publishedZKey, final byte[] claimedHKey,
      final byte[] payloadsHKey, final byte[] notifyLKey, final int numRetries) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.PUBLISH
        .getSha1Bytes().array(), 4, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        LuaQFunctions.getEpochMillisBytes(), id, payload), numRetries);
  }

  public static Long publish(final JedisExecutor jedisExecutor, final List<byte[]> keys,
      final Collection<byte[]> idPayloads, final int numRetries) {

    final List<byte[]> args = new ArrayList<>(idPayloads.size() + 1);
    args.add(LuaQFunctions.getEpochMillisBytes());
    args.addAll(idPayloads);

    return (Long) jedisExecutor
        .applyJedis(
            jedis -> jedis.evalsha(LuaQScripts.MPUBLISH.getSha1Bytes().array(), keys, args),
            numRetries);
  }

  public static Long republishAs(final JedisExecutor jedisExecutor, final byte[] id,
      final byte[] payload, final byte[] publishedZKey, final byte[] claimedHKey,
      final byte[] payloadsHKey, final byte[] notifyLKey, final int numRetries) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.REPUBLISH
        .getSha1Bytes().array(), 4, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        LuaQFunctions.getEpochMillisBytes(), id, payload), numRetries);
  }

  public static Long republish(final JedisExecutor jedisExecutor, final byte[] id,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final int numRetries) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.REPUBLISH
        .getSha1Bytes().array(), 4, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        LuaQFunctions.getEpochMillisBytes(), id), numRetries);
  }

  public static Long killAs(final JedisExecutor jedisExecutor, final byte[] id,
      final byte[] payload, final byte[] deadHKey, final byte[] claimedHKey,
      final byte[] payloadsHKey, final int numRetries) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.KILL.getSha1Bytes()
        .array(), 3, deadHKey, claimedHKey, payloadsHKey, LuaQFunctions.getEpochMillisBytes(), id,
        payload), numRetries);
  }

  public static Long kill(final JedisExecutor jedisExecutor, final byte[] id,
      final byte[] deadHKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final int numRetries) {

    return (Long) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.KILL.getSha1Bytes()
        .array(), 3, deadHKey, claimedHKey, payloadsHKey, LuaQFunctions.getEpochMillisBytes(), id),
        numRetries);
  }

  public static List<List<byte[]>> claim(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final byte[] claimLimit, final int timeoutSeconds) {

    final List<List<byte[]>> nonBlockingGet =
        LuaQFunctions.nonBlockingClaim(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey,
            notifyLKey, claimLimit);

    if (!nonBlockingGet.isEmpty())
      return nonBlockingGet;

    return LuaQFunctions.blockingClaim(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey,
        notifyLKey, claimLimit, timeoutSeconds);
  }

  public static List<List<byte[]>> blockingClaim(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final byte[] claimLimit, final int timeoutSeconds) {

    final List<List<byte[]>> result =
        jedisExecutor.applyJedis(jedis -> {

          final List<byte[]> event = jedis.blpop(timeoutSeconds, notifyLKey);

          return event == null || event.isEmpty() ? ImmutableList.<List<byte[]>>of()
              : LuaQFunctions.claim(jedis, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
                  claimLimit);
        });

    return result;
  }

  public static List<List<byte[]>> nonBlockingClaim(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final byte[] claimLimit) {

    return jedisExecutor.applyJedis(jedis -> LuaQFunctions.claim(jedis, publishedZKey, claimedHKey,
        payloadsHKey, notifyLKey, claimLimit));
  }

  @SuppressWarnings("unchecked")
  private static List<List<byte[]>> claim(final Jedis jedis, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] notifyListKey,
      final byte[] claimLimit) {

    return (List<List<byte[]>>) jedis.evalsha(LuaQScripts.CLAIM.getSha1Bytes().array(), 4,
        publishedZKey, claimedHKey, notifyListKey, payloadsHKey,
        LuaQFunctions.getEpochMillisBytes(), claimLimit);
  }

  public static void consume(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] notifyLKey,
      final Function<List<List<byte[]>>, Boolean> idPayloadConsumer, final byte[] claimLimit,
      final int maxBlockOnEmptyQSeconds) {

    jedisExecutor.acceptJedis(jedis -> {

      for (;;) {
        @SuppressWarnings("unchecked")
        final List<List<byte[]>> idPayloads =
            (List<List<byte[]>>) jedis.evalsha(LuaQScripts.CLAIM.getSha1Bytes().array(), 4,
                publishedZKey, claimedHKey, notifyLKey, payloadsHKey,
                LuaQFunctions.getEpochMillisBytes(), claimLimit);

        if (idPayloads.isEmpty()) {
          jedis.blpop(maxBlockOnEmptyQSeconds, notifyLKey);
          continue;
        }

        if (!idPayloadConsumer.apply(idPayloads))
          return;
      }
    });
  }

  public static void consume(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] notifyLKey,
      final Function<List<List<byte[]>>, Boolean> idPayloadConsumer, final byte[] claimLimit) {

    jedisExecutor.acceptJedis(jedis -> {

      for (;;) {
        @SuppressWarnings("unchecked")
        final List<List<byte[]>> idPayloads =
            (List<List<byte[]>>) jedis.evalsha(LuaQScripts.CLAIM.getSha1Bytes().array(), 4,
                publishedZKey, claimedHKey, notifyLKey, payloadsHKey,
                LuaQFunctions.getEpochMillisBytes(), claimLimit);

        if (!idPayloadConsumer.apply(idPayloads))
          return;
      }
    });
  }

  public static Long checkin(final JedisExecutor jedisExecutor, final byte[] claimedHKey,
      final byte[] id) {

    return jedisExecutor.applyJedis(jedis -> jedis.hset(claimedHKey, id,
        LuaQFunctions.getEpochMillisBytes()));
  }

  public static String getEpochMillisString() {

    return String.valueOf(System.currentTimeMillis());
  }

  public static byte[] getEpochMillisBytes() {

    return LuaQFunctions.getEpochMillisString().getBytes(StandardCharsets.UTF_8);
  }

  public static long zRemove(final JedisExecutor jedisExecutor, final byte[] zKey,
      final byte[] payloadsHKey, final int numRetries, final String... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final String id : ids) {

        final byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);

        removedResponses.add(pipeline.zrem(zKey, idBytes));
        pipeline.hdel(payloadsHKey, idBytes);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static long zRemove(final JedisExecutor jedisExecutor, final byte[] zKey,
      final byte[] payloadsHKey, final int numRetries, final byte[]... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final byte[] idBytes : ids) {

        removedResponses.add(pipeline.zrem(zKey, idBytes));
        pipeline.hdel(payloadsHKey, idBytes);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static long hRemove(final JedisExecutor jedisExecutor, final byte[] hKey,
      final byte[] payloadsHKey, final int numRetries, final String... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final String id : ids) {

        final byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);

        removedResponses.add(pipeline.hdel(hKey, idBytes));
        pipeline.hdel(payloadsHKey, idBytes);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static long hRemove(final JedisExecutor jedisExecutor, final byte[] hKey,
      final byte[] payloadsHKey, final int numRetries, final byte[]... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final byte[] idBytes : ids) {

        removedResponses.add(pipeline.hdel(hKey, idBytes));
        pipeline.hdel(payloadsHKey, idBytes);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static long remove(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final int numRetries,
      final String... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final String id : ids) {

        final byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);

        removedResponses.add(pipeline.zrem(publishedZKey, idBytes));
        removedResponses.add(pipeline.hdel(claimedHKey, idBytes));
        pipeline.hdel(payloadsHKey, idBytes);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static long remove(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final int numRetries,
      final byte[]... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final byte[] idBytes : ids) {

        removedResponses.add(pipeline.zrem(publishedZKey, idBytes));
        removedResponses.add(pipeline.hdel(claimedHKey, idBytes));
        pipeline.hdel(payloadsHKey, idBytes);
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static Long clear(final JedisExecutor jedisExecutor, final int numRetries,
      final byte[]... keys) {

    return jedisExecutor.applyJedis(jedis -> jedis.del(keys), numRetries);
  }

  public static Long zClearQ(final JedisExecutor jedisExecutor, final byte[] zKey,
      final byte[] payloadsHKey, final int numRetries) {

    return jedisExecutor.applyJedis(jedis -> {
      final Set<byte[]> qMembers = jedis.zrange(zKey, 0, -1);
      jedis.hdel(payloadsHKey, qMembers.toArray(new byte[qMembers.size()][]));
      return jedis.del(zKey);
    }, numRetries);
  }

  public static Long hClearQ(final JedisExecutor jedisExecutor, final byte[] hKey,
      final byte[] payloadsHKey, final int numRetries) {

    return jedisExecutor.applyJedis(jedis -> {
      final Set<byte[]> qMembers = jedis.hkeys(hKey);
      jedis.hdel(payloadsHKey, qMembers.toArray(new byte[qMembers.size()][]));
      return jedis.del(hKey);
    }, numRetries);
  }

  public static List<byte[]> removeOrphanedPayloads(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final int batchSize) {

    final Set<byte[]> payloadIds = jedisExecutor.applyJedis(jedis -> jedis.hkeys(payloadsHKey));

    if (payloadIds == null || payloadIds.isEmpty())
      return ImmutableList.of();

    final List<byte[]> removePayloadIds = new LinkedList<>();

    Iterables.partition(payloadIds, batchSize).forEach(payloadIdsBatch -> {

      final List<Response<Double>> scores = new ArrayList<>(payloadIdsBatch.size());
      final List<Response<byte[]>> claimedScores = new ArrayList<>(payloadIdsBatch.size());

      jedisExecutor.acceptPipeline(pipeline -> payloadIdsBatch.stream().forEach(payloadId -> {
        scores.add(pipeline.zscore(publishedZKey, payloadId));
        claimedScores.add(pipeline.hget(claimedHKey, payloadId));
      }));

      for (int p = 0; p < payloadIdsBatch.size(); p++) {

        final Double publishedScore = scores.get(p).get();
        final byte[] claimedScore = claimedScores.get(p).get();

        if (publishedScore == null && claimedScore == null) {
          removePayloadIds.add(payloadIdsBatch.get(p));
        }
      }
    });

    if (!removePayloadIds.isEmpty()) {
      jedisExecutor.applyJedis(jedis -> jedis.hdel(payloadsHKey,
          removePayloadIds.toArray(new byte[removePayloadIds.size()][])));
    }

    return removePayloadIds;
  }

  private static final byte[] SCAN_SENTINEL_CURSOR = "0".getBytes(StandardCharsets.UTF_8);
  private static final byte[] DEFAULT_COUNT = "10".getBytes(StandardCharsets.UTF_8);
  static final ScanParams DEFAULT_SCAN_PARAMS = new ScanParams().count(10);

  private static final byte[] ZSCAN = "zscan".getBytes(StandardCharsets.UTF_8);
  private static final byte[] HSCAN = "hscan".getBytes(StandardCharsets.UTF_8);

  public static void zScanPayloads(final JedisExecutor jedisExecutor, final byte[] zKey,
      final byte[] payloadsHKey, final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions.scanPayloads(jedisExecutor, ZSCAN, zKey, payloadsHKey, DEFAULT_COUNT,
        idScorePayloadsConsumer);
  }

  public static void hScanPayloads(final JedisExecutor jedisExecutor, final byte[] hKey,
      final byte[] payloadsHKey, final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions.scanPayloads(jedisExecutor, HSCAN, hKey, payloadsHKey, DEFAULT_COUNT,
        idScorePayloadsConsumer);
  }

  @SuppressWarnings("unchecked")
  public static void scanPayloads(final JedisExecutor jedisExecutor, final byte[] scanCommand,
      final byte[] key, final byte[] payloadsHKey, final byte[] count,
      final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    for (byte[] cursor = SCAN_SENTINEL_CURSOR;;) {

      final byte[] finalCursorRef = cursor;

      final List<?> results =
          (List<?>) jedisExecutor.applyJedis(jedis -> jedis.evalsha(LuaQScripts.SCAN_PAYLOADS
              .getSha1Bytes().array(), 2, key, payloadsHKey, scanCommand, finalCursorRef, count));

      cursor = (byte[]) results.get(0);

      idScorePayloadsConsumer.accept((List<List<byte[]>>) results.get(1));

      if (Arrays.equals(cursor, SCAN_SENTINEL_CURSOR))
        return;
    }
  }

  public static void hScanIdValues(final JedisExecutor jedisExecutor, final byte[] hKey,
      final Consumer<Entry<byte[], byte[]>> idValuesConsumer) {

    LuaQFunctions.hScanIdValues(jedisExecutor, hKey, DEFAULT_SCAN_PARAMS, idValuesConsumer);
  }

  public static void hScanIdValues(final JedisExecutor jedisExecutor, final byte[] hKey,
      final ScanParams scanParams, final Consumer<Entry<byte[], byte[]>> idValuesConsumer) {

    for (byte[] cursor = SCAN_SENTINEL_CURSOR;;) {

      final byte[] finalCursorRef = cursor;

      final ScanResult<Entry<byte[], byte[]>> results =
          jedisExecutor.applyJedis(jedis -> jedis.hscan(hKey, finalCursorRef, scanParams));

      cursor = results.getCursorAsBytes();

      results.getResult().forEach(idValuesConsumer::accept);

      if (Arrays.equals(cursor, SCAN_SENTINEL_CURSOR))
        return;
    }
  }

  public static void zScanIdScores(final JedisExecutor jedisExecutor, final byte[] zKey,
      final Consumer<Tuple> idValuesConsumer) {

    LuaQFunctions.zScanIdScores(jedisExecutor, zKey, DEFAULT_SCAN_PARAMS, idValuesConsumer);
  }

  public static void zScanIdScores(final JedisExecutor jedisExecutor, final byte[] zKey,
      final ScanParams scanParams, final Consumer<Tuple> idScoresConsumer) {

    for (byte[] cursor = SCAN_SENTINEL_CURSOR;;) {

      final byte[] finalCursorRef = cursor;

      final ScanResult<Tuple> results =
          jedisExecutor.applyJedis(jedis -> jedis.zscan(zKey, finalCursorRef, scanParams));

      cursor = results.getCursorAsBytes();

      results.getResult().forEach(idScoresConsumer::accept);

      if (Arrays.equals(cursor, SCAN_SENTINEL_CURSOR))
        return;
    }
  }
}
