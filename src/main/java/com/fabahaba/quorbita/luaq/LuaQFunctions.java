package com.fabahaba.quorbita.luaq;

import com.fabahaba.jedipus.JedisExecutor;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public final class LuaQFunctions {

  private LuaQFunctions() {}

  @SuppressWarnings("unchecked")
  public static List<Long> publish(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] notifyLKey,
      final int numRetries, final byte[]... idPayloads) {

    int i = 5;

    final byte[][] params = new byte[i + idPayloads.length][];

    params[0] = publishedZKey;
    params[1] = claimedHKey;
    params[2] = payloadsHKey;
    params[3] = notifyLKey;
    params[4] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] idOrPayload : idPayloads) {
      params[i++] = idOrPayload;
    }

    return (List<Long>) LuaQScripts.PUBLISH.eval(jedisExecutor, numRetries, 4, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> publish(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] notifyLKey,
      final int numRetries, final Collection<byte[]> idPayloads) {

    int i = 5;

    final byte[][] params = new byte[i + idPayloads.size()][];

    params[0] = publishedZKey;
    params[1] = claimedHKey;
    params[2] = payloadsHKey;
    params[3] = notifyLKey;
    params[4] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] idOrPayload : idPayloads) {
      params[i++] = idOrPayload;
    }

    return (List<Long>) LuaQScripts.PUBLISH.eval(jedisExecutor, numRetries, 4, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> republishAs(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedOrDeadHKey, final byte[] notifyLKey,
      final byte[] payloadsHKey, final int numRetries, final byte[]... idPayloads) {

    int i = 5;

    final byte[][] params = new byte[i + idPayloads.length][];

    params[0] = publishedZKey;
    params[1] = claimedOrDeadHKey;
    params[2] = notifyLKey;
    params[3] = payloadsHKey;
    params[4] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] idOrPayload : idPayloads) {
      params[i++] = idOrPayload;
    }

    return (List<Long>) LuaQScripts.REPUBLISH.eval(jedisExecutor, numRetries, 4, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> republishClaimedAs(final JedisExecutor jedisExecutor,
      final byte[] claimToken, final byte[] publishedZKey, final byte[] claimedHKey,
      final byte[] notifyLKey, final byte[] payloadsHKey, final int numRetries,
      final byte[]... idPayloads) {

    int i = 6;

    final byte[][] params = new byte[i + idPayloads.length][];

    params[0] = publishedZKey;
    params[1] = claimedHKey;
    params[2] = notifyLKey;
    params[3] = payloadsHKey;
    params[4] = claimToken;
    params[5] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] idOrPayload : idPayloads) {
      params[i++] = idOrPayload;
    }

    return (List<Long>) LuaQScripts.REPUBLISH_CLAIMED.eval(jedisExecutor, numRetries, 4, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> republish(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedOrDeadHKey, final byte[] notifyLKey, final int numRetries,
      final String... ids) {

    int i = 4;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = publishedZKey;
    params[1] = claimedOrDeadHKey;
    params[2] = notifyLKey;
    params[3] = LuaQFunctions.getEpochMillisBytes();

    for (final String id : ids) {
      params[i++] = id.getBytes(StandardCharsets.UTF_8);
    }

    return (List<Long>) LuaQScripts.REPUBLISH.eval(jedisExecutor, numRetries, 3, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> republish(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedOrDeadHKey, final byte[] notifyLKey, final int numRetries,
      final byte[]... ids) {

    int i = 4;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = publishedZKey;
    params[1] = claimedOrDeadHKey;
    params[2] = notifyLKey;
    params[3] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] id : ids) {
      params[i++] = id;
    }

    return (List<Long>) LuaQScripts.REPUBLISH.eval(jedisExecutor, numRetries, 3, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> republishClaimed(final JedisExecutor jedisExecutor,
      final byte[] claimToken, final byte[] publishedZKey, final byte[] claimedHKey,
      final byte[] notifyLKey, final int numRetries, final String... ids) {

    int i = 5;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = publishedZKey;
    params[1] = claimedHKey;
    params[2] = notifyLKey;
    params[3] = claimToken;
    params[4] = LuaQFunctions.getEpochMillisBytes();

    for (final String id : ids) {
      params[i++] = id.getBytes(StandardCharsets.UTF_8);
    }

    return (List<Long>) LuaQScripts.REPUBLISH_CLAIMED.eval(jedisExecutor, numRetries, 3, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> republishClaimed(final JedisExecutor jedisExecutor,
      final byte[] claimToken, final byte[] publishedZKey, final byte[] claimedHKey,
      final byte[] notifyLKey, final int numRetries, final byte[]... ids) {

    int i = 5;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = publishedZKey;
    params[1] = claimedHKey;
    params[2] = notifyLKey;
    params[3] = claimToken;
    params[4] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] idOrPayload : ids) {
      params[i++] = idOrPayload;
    }

    return (List<Long>) LuaQScripts.REPUBLISH_CLAIMED.eval(jedisExecutor, numRetries, 3, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> killAs(final JedisExecutor jedisExecutor, final byte[] deadHKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final int numRetries,
      final byte[]... idPayloads) {

    int i = 4;

    final byte[][] params = new byte[i + idPayloads.length][];

    params[0] = deadHKey;
    params[1] = claimedHKey;
    params[2] = payloadsHKey;
    params[3] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] idOrPayload : idPayloads) {
      params[i++] = idOrPayload;
    }

    return (List<Long>) LuaQScripts.KILL.eval(jedisExecutor, numRetries, 3, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> killClaimedAs(final JedisExecutor jedisExecutor,
      final byte[] claimToken, final byte[] deadHKey, final byte[] claimedHKey,
      final byte[] payloadsHKey, final int numRetries, final byte[]... idPayloads) {

    int i = 5;

    final byte[][] params = new byte[i + idPayloads.length][];

    params[0] = deadHKey;
    params[1] = claimedHKey;
    params[2] = payloadsHKey;
    params[3] = claimToken;
    params[4] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] idOrPayload : idPayloads) {
      params[i++] = idOrPayload;
    }

    return (List<Long>) LuaQScripts.KILL_CLAIMED.eval(jedisExecutor, numRetries, 3, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> kill(final JedisExecutor jedisExecutor, final byte[] deadHKey,
      final byte[] claimedHKey, final int numRetries, final String... ids) {

    int i = 3;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = deadHKey;
    params[1] = claimedHKey;
    params[2] = LuaQFunctions.getEpochMillisBytes();

    for (final String id : ids) {
      params[i++] = id.getBytes(StandardCharsets.UTF_8);
    }

    return (List<Long>) LuaQScripts.KILL.eval(jedisExecutor, numRetries, 2, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> kill(final JedisExecutor jedisExecutor, final byte[] deadHKey,
      final byte[] claimedHKey, final int numRetries, final byte[]... ids) {

    int i = 3;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = deadHKey;
    params[1] = claimedHKey;
    params[2] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] id : ids) {
      params[i++] = id;
    }

    return (List<Long>) LuaQScripts.KILL.eval(jedisExecutor, numRetries, 2, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> killClaimed(final JedisExecutor jedisExecutor, final byte[] claimToken,
      final byte[] deadHKey, final byte[] claimedHKey, final int numRetries, final String... ids) {

    int i = 4;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = deadHKey;
    params[1] = claimedHKey;
    params[2] = claimToken;
    params[3] = LuaQFunctions.getEpochMillisBytes();

    for (final String id : ids) {
      params[i++] = id.getBytes(StandardCharsets.UTF_8);
    }

    return (List<Long>) LuaQScripts.KILL_CLAIMED.eval(jedisExecutor, numRetries, 2, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> killClaimed(final JedisExecutor jedisExecutor, final byte[] claimToken,
      final byte[] deadHKey, final byte[] claimedHKey, final int numRetries, final byte[]... ids) {

    int i = 4;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = deadHKey;
    params[1] = claimedHKey;
    params[2] = claimToken;
    params[3] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] id : ids) {
      params[i++] = id;
    }

    return (List<Long>) LuaQScripts.KILL_CLAIMED.eval(jedisExecutor, numRetries, 2, params);
  }

  public static Optional<ClaimedIdPayloads> claim(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final byte[] claimLimit, final int timeoutSeconds) {

    final Optional<ClaimedIdPayloads> nonBlockingClaim =
        LuaQFunctions.nonBlockingClaim(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey,
            notifyLKey, claimLimit);

    return nonBlockingClaim.isPresent() ? nonBlockingClaim : LuaQFunctions.blockingClaim(
        jedisExecutor, publishedZKey, claimedHKey, payloadsHKey, notifyLKey, claimLimit,
        timeoutSeconds);
  }

  public static Optional<ClaimedIdPayloads> blockingClaim(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final byte[] claimLimit, final int timeoutSeconds) {

    final Optional<ClaimedIdPayloads> claim =
        Optional.ofNullable(jedisExecutor.applyJedis(jedis -> {

          final List<byte[]> event = jedis.blpop(timeoutSeconds, notifyLKey);

          if (event == null || event.isEmpty())
            return null;

          return LuaQFunctions.claim(jedis, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
              claimLimit);
        }));

    return claim;
  }

  public static Optional<ClaimedIdPayloads> nonBlockingClaim(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final byte[] claimLimit) {

    return Optional.ofNullable(jedisExecutor.applyJedis(jedis -> LuaQFunctions.claim(jedis,
        publishedZKey, claimedHKey, payloadsHKey, notifyLKey, claimLimit)));
  }

  @SuppressWarnings("unchecked")
  private static ClaimedIdPayloads claim(final Jedis jedis, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] notifyListKey,
      final byte[] claimLimit) {

    final byte[] epochMillisBytes = LuaQFunctions.getEpochMillisBytes();

    final List<List<byte[]>> idPayloads =
        (List<List<byte[]>>) LuaQScripts.CLAIM.eval(jedis, 4, publishedZKey, claimedHKey,
            notifyListKey, payloadsHKey, epochMillisBytes, claimLimit);

    return idPayloads.isEmpty() ? null : new ClaimedIdPayloads(ByteBuffer.wrap(epochMillisBytes),
        idPayloads);
  }

  public static void consume(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] notifyLKey,
      final Function<ClaimedIdPayloads, Boolean> idPayloadConsumer, final byte[] claimLimit,
      final int maxBlockOnEmptyQSeconds) {

    jedisExecutor.acceptJedis(jedis -> {

      for (;;) {
        final byte[] epochMillisBytes = LuaQFunctions.getEpochMillisBytes();

        @SuppressWarnings("unchecked")
        final List<List<byte[]>> idPayloads =
            (List<List<byte[]>>) LuaQScripts.CLAIM.eval(jedis, 4, publishedZKey, claimedHKey,
                notifyLKey, payloadsHKey, epochMillisBytes, claimLimit);

        if (idPayloads.isEmpty()) {
          jedis.blpop(maxBlockOnEmptyQSeconds, notifyLKey);
          continue;
        }

        if (!idPayloadConsumer.apply(new ClaimedIdPayloads(ByteBuffer.wrap(epochMillisBytes),
            idPayloads)))
          return;
      }
    });
  }

  public static void consume(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] notifyLKey,
      final Function<ClaimedIdPayloads, Boolean> idPayloadConsumer, final byte[] claimLimit) {

    jedisExecutor.acceptJedis(jedis -> {

      for (;;) {
        final byte[] epochMillisBytes = LuaQFunctions.getEpochMillisBytes();

        @SuppressWarnings("unchecked")
        final List<List<byte[]>> idPayloads =
            (List<List<byte[]>>) LuaQScripts.CLAIM.eval(jedis, 4, publishedZKey, claimedHKey,
                notifyLKey, payloadsHKey, epochMillisBytes, claimLimit);

        if (!idPayloadConsumer.apply(new ClaimedIdPayloads(ByteBuffer.wrap(epochMillisBytes),
            idPayloads)))
          return;
      }
    });
  }

  @SuppressWarnings("unchecked")
  public static ClaimedCheckins checkin(final JedisExecutor jedisExecutor,
      final byte[] claimedHKey, final int numRetries, final String... ids) {

    int i = 2;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = claimedHKey;
    params[1] = LuaQFunctions.getEpochMillisBytes();

    for (final String id : ids) {
      params[i++] = id.getBytes(StandardCharsets.UTF_8);
    }

    final List<Long> checkins =
        (List<Long>) LuaQScripts.CHECKIN.eval(jedisExecutor, numRetries, 1, params);

    return new ClaimedCheckins(ByteBuffer.wrap(params[1]), checkins);
  }

  @SuppressWarnings("unchecked")
  public static ClaimedCheckins checkin(final JedisExecutor jedisExecutor,
      final byte[] claimedHKey, final int numRetries, final byte[]... ids) {

    int i = 2;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = claimedHKey;
    params[1] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] id : ids) {
      params[i++] = id;
    }

    final List<Long> checkins =
        (List<Long>) LuaQScripts.CHECKIN.eval(jedisExecutor, numRetries, 1, params);

    return new ClaimedCheckins(ByteBuffer.wrap(params[1]), checkins);
  }

  @SuppressWarnings("unchecked")
  public static ClaimedCheckins checkinClaimed(final JedisExecutor jedisExecutor,
      final byte[] claimToken, final byte[] claimedHKey, final int numRetries, final String... ids) {

    int i = 3;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = claimedHKey;
    params[1] = claimToken;
    params[2] = LuaQFunctions.getEpochMillisBytes();

    for (final String id : ids) {
      params[i++] = id.getBytes(StandardCharsets.UTF_8);
    }

    final List<Long> checkins =
        (List<Long>) LuaQScripts.CHECKIN_CLAIMED.eval(jedisExecutor, numRetries, 1, params);

    return new ClaimedCheckins(ByteBuffer.wrap(params[2]), checkins);
  }

  @SuppressWarnings("unchecked")
  public static ClaimedCheckins checkinClaimed(final JedisExecutor jedisExecutor,
      final byte[] claimToken, final byte[] claimedHKey, final int numRetries, final byte[]... ids) {

    int i = 3;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = claimedHKey;
    params[1] = claimToken;
    params[2] = LuaQFunctions.getEpochMillisBytes();

    for (final byte[] id : ids) {
      params[i++] = id;
    }

    final List<Long> checkins =
        (List<Long>) LuaQScripts.CHECKIN_CLAIMED.eval(jedisExecutor, numRetries, 1, params);

    return new ClaimedCheckins(ByteBuffer.wrap(params[2]), checkins);
  }

  public static String getEpochMillisString() {

    return String.valueOf(System.currentTimeMillis());
  }

  public static byte[] getEpochMillisBytes() {

    return LuaQFunctions.getEpochMillisString().getBytes(StandardCharsets.UTF_8);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> removeClaimed(final JedisExecutor jedisExecutor,
      final byte[] claimToken, final byte[] claimedHKey, final byte[] payloadsHKey,
      final int numRetries, final String... ids) {

    int i = 3;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = claimedHKey;
    params[1] = payloadsHKey;
    params[2] = claimToken;

    for (final String id : ids) {
      params[i++] = id.getBytes(StandardCharsets.UTF_8);
    }

    return (List<Long>) LuaQScripts.REMOVE_CLAIMED.eval(jedisExecutor, numRetries, 2, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> removeClaimed(final JedisExecutor jedisExecutor,
      final byte[] claimToken, final byte[] claimedHKey, final byte[] payloadsHKey,
      final int numRetries, final byte[]... ids) {

    int i = 3;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = claimedHKey;
    params[1] = payloadsHKey;
    params[2] = claimToken;

    for (final byte[] id : ids) {
      params[i++] = id;
    }

    return (List<Long>) LuaQScripts.REMOVE_CLAIMED.eval(jedisExecutor, numRetries, 2, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> remove(final JedisExecutor jedisExecutor,
      final byte[] claimedOrDeadHKey, final byte[] payloadsHKey, final int numRetries,
      final String... ids) {

    int i = 2;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = claimedOrDeadHKey;
    params[1] = payloadsHKey;

    for (final String id : ids) {
      params[i++] = id.getBytes(StandardCharsets.UTF_8);
    }

    return (List<Long>) LuaQScripts.REMOVE.eval(jedisExecutor, numRetries, 2, params);
  }

  @SuppressWarnings("unchecked")
  public static List<Long> remove(final JedisExecutor jedisExecutor,
      final byte[] claimedOrDeadHKey, final byte[] payloadsHKey, final int numRetries,
      final byte[]... ids) {

    int i = 2;

    final byte[][] params = new byte[i + ids.length][];

    params[0] = claimedOrDeadHKey;
    params[1] = payloadsHKey;

    for (final byte[] id : ids) {
      params[i++] = id;
    }

    return (List<Long>) LuaQScripts.REMOVE.eval(jedisExecutor, numRetries, 2, params);
  }

  public static long removeAll(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] deadHKey, final byte[] payloadsHKey,
      final int numRetries, final String... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final String id : ids) {

        final byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);

        pipeline.zrem(publishedZKey, idBytes);
        pipeline.hdel(claimedHKey, idBytes);
        pipeline.hdel(deadHKey, idBytes);
        removedResponses.add(pipeline.hdel(payloadsHKey, idBytes));
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static long removeAll(final JedisExecutor jedisExecutor, final byte[] publishedZKey,
      final byte[] claimedHKey, final byte[] deadHKey, final byte[] payloadsHKey,
      final int numRetries, final byte[]... ids) {

    final List<Response<Long>> removedResponses = new ArrayList<>(ids.length);

    jedisExecutor.acceptPipelinedTransaction(pipeline -> {

      for (final byte[] idBytes : ids) {

        pipeline.zrem(publishedZKey, idBytes);
        pipeline.hdel(claimedHKey, idBytes);
        pipeline.hdel(deadHKey, idBytes);
        removedResponses.add(pipeline.hdel(payloadsHKey, idBytes));
      }
    }, numRetries);

    return removedResponses.stream().mapToLong(Response::get).sum();
  }

  public static Long clear(final JedisExecutor jedisExecutor, final int numRetries,
      final byte[]... keys) {

    return jedisExecutor.applyJedis(jedis -> jedis.del(keys), numRetries);
  }

  private static final byte[] SCAN_SENTINEL_CURSOR = "0".getBytes(StandardCharsets.UTF_8);
  private static final byte[] DEFAULT_COUNT = "10".getBytes(StandardCharsets.UTF_8);
  public static final ScanParams DEFAULT_SCAN_PARAMS = new ScanParams().count(10);

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

      final List<Object> results =
          (List<Object>) LuaQScripts.SCAN_PAYLOADS.eval(jedisExecutor, 1, 2, key, payloadsHKey,
              scanCommand, finalCursorRef, count);

      cursor = (byte[]) results.get(0);

      idScorePayloadsConsumer.accept((List<List<byte[]>>) results.get(1));

      if (Arrays.equals(cursor, SCAN_SENTINEL_CURSOR))
        return;
    }
  }

  public static void hScanIdValues(final JedisExecutor jedisExecutor, final byte[] hKey,
      final Consumer<List<Entry<byte[], byte[]>>> idValuesConsumer) {

    LuaQFunctions.hScanIdValues(jedisExecutor, hKey, DEFAULT_SCAN_PARAMS, idValuesConsumer);
  }

  public static void hScanIdValues(final JedisExecutor jedisExecutor, final byte[] hKey,
      final ScanParams scanParams, final Consumer<List<Entry<byte[], byte[]>>> idValuesConsumer) {

    for (byte[] cursor = SCAN_SENTINEL_CURSOR;;) {

      final byte[] finalCursorRef = cursor;

      final ScanResult<Entry<byte[], byte[]>> results =
          jedisExecutor.applyJedis(jedis -> jedis.hscan(hKey, finalCursorRef, scanParams));

      cursor = results.getCursorAsBytes();

      idValuesConsumer.accept(results.getResult());

      if (Arrays.equals(cursor, SCAN_SENTINEL_CURSOR))
        return;
    }
  }

  public static void zScanIdScores(final JedisExecutor jedisExecutor, final byte[] zKey,
      final Consumer<List<Tuple>> idValuesConsumer) {

    LuaQFunctions.zScanIdScores(jedisExecutor, zKey, DEFAULT_SCAN_PARAMS, idValuesConsumer);
  }

  public static void zScanIdScores(final JedisExecutor jedisExecutor, final byte[] zKey,
      final ScanParams scanParams, final Consumer<List<Tuple>> idScoresConsumer) {

    for (byte[] cursor = SCAN_SENTINEL_CURSOR;;) {

      final byte[] finalCursorRef = cursor;

      final ScanResult<Tuple> results =
          jedisExecutor.applyJedis(jedis -> jedis.zscan(zKey, finalCursorRef, scanParams));

      cursor = results.getCursorAsBytes();

      idScoresConsumer.accept(results.getResult());

      if (Arrays.equals(cursor, SCAN_SENTINEL_CURSOR))
        return;
    }
  }

  public static void scanPayloadStates(final JedisExecutor jedisExecutor,
      final byte[] payloadsHKey, final byte[] publishedZKey, final byte[] claimedHKey,
      final byte[] deadHKey, final Consumer<List<List<Object>>> idPayloadStatesConsumer) {

    LuaQFunctions.scanPayloadStates(jedisExecutor, payloadsHKey, publishedZKey, claimedHKey,
        deadHKey, DEFAULT_COUNT, idPayloadStatesConsumer);
  }

  @SuppressWarnings("unchecked")
  public static void scanPayloadStates(final JedisExecutor jedisExecutor,
      final byte[] payloadsHKey, final byte[] publishedZKey, final byte[] claimedHKey,
      final byte[] deadHKey, final byte[] count,
      final Consumer<List<List<Object>>> idPayloadStatesConsumer) {

    for (byte[] cursor = SCAN_SENTINEL_CURSOR;;) {

      final byte[] finalCursorRef = cursor;

      final List<Object> results =
          (List<Object>) LuaQScripts.SCAN_PAYLOAD_STATES.eval(jedisExecutor, 0, 4, payloadsHKey,
              publishedZKey, claimedHKey, deadHKey, finalCursorRef, count);

      cursor = (byte[]) results.get(0);

      idPayloadStatesConsumer.accept((List<List<Object>>) results.get(1));

      if (Arrays.equals(cursor, SCAN_SENTINEL_CURSOR))
        return;
    }
  }
}
