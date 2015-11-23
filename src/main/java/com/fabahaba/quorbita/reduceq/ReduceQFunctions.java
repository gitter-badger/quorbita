package com.fabahaba.quorbita.reduceq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.luaq.LuaQFunctions;

import java.util.Collection;

public class ReduceQFunctions {

  private static final int NUM_PUB_EPOCH_REDUCIBLE_KEYS = 9;
  private static final int NUM_PUB_EPOCH_REDUCIBLE_KEYS_AND_ARGS = 13;

  public static Long publishEpochReducible(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final byte[] pendingMappedSKey, final byte[] mappedResultsHKey,
      final byte[] publishedReduceZKey, final byte[] claimedReduceHKey,
      final byte[] payloadReduceHKey, final byte[] reduceWeight, final byte[] reduceId,
      final byte[] reducePayload, final Collection<byte[]> idPayloads, final int numRetries) {

    final byte[][] params = new byte[NUM_PUB_EPOCH_REDUCIBLE_KEYS_AND_ARGS + idPayloads.size()][];

    params[0] = publishedZKey;
    params[1] = claimedHKey;
    params[2] = payloadsHKey;
    params[3] = notifyLKey;
    params[4] = pendingMappedSKey;
    params[5] = mappedResultsHKey;
    params[6] = publishedReduceZKey;
    params[7] = claimedReduceHKey;
    params[8] = payloadReduceHKey;
    params[9] = LuaQFunctions.getEpochMillisBytes();
    params[10] = reduceWeight;
    params[11] = reduceId;
    params[12] = reducePayload;

    int i = NUM_PUB_EPOCH_REDUCIBLE_KEYS_AND_ARGS;
    for (final byte[] idOrPayload : idPayloads) {
      params[i++] = idOrPayload;
    }

    return (Long) ReduceQScripts.PUBLISH_EPOCH_REDUCIBLE.eval(jedisExecutor, numRetries,
        NUM_PUB_EPOCH_REDUCIBLE_KEYS, params);
  }

  private static final int NUM_PUB_REDUCIBLE_KEYS = 8;
  private static final int NUM_PUB_REDUCIBLE_KEYS_AND_ARGS = 11;

  public static Long publishReducible(final JedisExecutor jedisExecutor,
      final byte[] publishedZKey, final byte[] claimedHKey, final byte[] payloadsHKey,
      final byte[] notifyLKey, final byte[] pendingMappedSKey, final byte[] mappedResultsHKey,
      final byte[] publishedReduceZKey, final byte[] claimedReduceHKey, final byte[] reduceWeight,
      final byte[] reduceId, final Collection<byte[]> idPayloads, final int numRetries) {

    final byte[][] params = new byte[NUM_PUB_REDUCIBLE_KEYS_AND_ARGS + idPayloads.size()][];

    params[0] = publishedZKey;
    params[1] = claimedHKey;
    params[2] = payloadsHKey;
    params[3] = notifyLKey;
    params[4] = pendingMappedSKey;
    params[5] = mappedResultsHKey;
    params[6] = publishedReduceZKey;
    params[7] = claimedReduceHKey;
    params[8] = LuaQFunctions.getEpochMillisBytes();
    params[9] = reduceWeight;
    params[10] = reduceId;

    int i = NUM_PUB_REDUCIBLE_KEYS_AND_ARGS;
    for (final byte[] idOrPayload : idPayloads) {
      params[i++] = idOrPayload;
    }

    return (Long) ReduceQScripts.PUBLISH_REDUCIBLE.eval(jedisExecutor, numRetries,
        NUM_PUB_REDUCIBLE_KEYS, params);
  }

  public static Long publishMappedResult(final JedisExecutor jedisExecutor,
      final byte[] publishedReduceZKey, final byte[] claimedReduceHKey,
      final byte[] mappedResultsHKey, final byte[] pendingMappedSKey,
      final byte[] notifyReduceLKey, final byte[] notifyMappedResultsLKey,
      final byte[] claimedHKey, final byte[] payloadsHKey, final byte[] reduceId,
      final byte[] reduceWeight, final byte[] id, final byte[] resultPayload, final int numRetries) {

    return (Long) ReduceQScripts.PUBLISH_MAPPED_RESULT.eval(jedisExecutor, numRetries, 8,
        publishedReduceZKey, claimedReduceHKey, mappedResultsHKey, pendingMappedSKey,
        notifyReduceLKey, notifyMappedResultsLKey, claimedHKey, payloadsHKey, reduceId,
        reduceWeight, id, resultPayload);
  }

  public static Long republishReducibleAs(final JedisExecutor jedisExecutor,
      final byte[] publishedReduceZKey, final byte[] claimedReduceHKey,
      final byte[] notifyReduceLKey, final byte[] payloadReduceHKey, final byte[] reduceId,
      final byte[] reducePayload, final int numRetries) {

    return (Long) ReduceQScripts.REPUBLISH_REDUCIBLE.eval(jedisExecutor, numRetries, 4,
        publishedReduceZKey, claimedReduceHKey, notifyReduceLKey, payloadReduceHKey, reduceId,
        reducePayload);
  }

  public static Long republishReducible(final JedisExecutor jedisExecutor,
      final byte[] publishedReduceZKey, final byte[] claimedReduceHKey,
      final byte[] notifyReduceLKey, final byte[] reduceId, final int numRetries) {

    return (Long) ReduceQScripts.REPUBLISH_REDUCIBLE.eval(jedisExecutor, numRetries, 3,
        publishedReduceZKey, claimedReduceHKey, notifyReduceLKey, reduceId);
  }

  public static Long republishDeadReducibleAs(final JedisExecutor jedisExecutor,
      final byte[] publishedReduceZKey, final byte[] deadReduceHKey,
      final byte[] notifyReduceLKey, final byte[] payloadReduceHKey, final byte[] reduceId,
      final byte[] reducePayload, final int numRetries) {

    return (Long) ReduceQScripts.REPUBLISH_DEAD_REDUCIBLE.eval(jedisExecutor, numRetries, 4,
        publishedReduceZKey, deadReduceHKey, notifyReduceLKey, payloadReduceHKey, reduceId,
        reducePayload);
  }

  public static Long republishDeadReducibleAs(final JedisExecutor jedisExecutor,
      final byte[] publishedReduceZKey, final byte[] deadReduceHKey,
      final byte[] notifyReduceLKey, final byte[] reduceId, final int numRetries) {

    return (Long) ReduceQScripts.REPUBLISH_DEAD_REDUCIBLE.eval(jedisExecutor, numRetries, 3,
        publishedReduceZKey, deadReduceHKey, notifyReduceLKey, reduceId);
  }

  public static Long killReducibleAs(final JedisExecutor jedisExecutor,
      final byte[] deadReduceHKey, final byte[] claimedReduceHKey, final byte[] pendingMappedSKey,
      final byte[] payloadReduceHKey, final byte[] reduceId, final byte[] reducePayload,
      final int numRetries) {

    return (Long) ReduceQScripts.KILL_REDUCIBLE.eval(jedisExecutor, numRetries, 4, deadReduceHKey,
        claimedReduceHKey, pendingMappedSKey, payloadReduceHKey, reduceId, reducePayload);
  }

  public static Long killReducible(final JedisExecutor jedisExecutor, final byte[] deadReduceHKey,
      final byte[] claimedReduceHKey, final byte[] pendingMappedSKey, final byte[] reduceId,
      final int numRetries) {

    return (Long) ReduceQScripts.KILL_REDUCIBLE.eval(jedisExecutor, numRetries, 3, deadReduceHKey,
        claimedReduceHKey, pendingMappedSKey, reduceId);
  }
}
