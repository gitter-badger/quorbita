package com.fabahaba.quorbita.reduceq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.luaq.LuaQ;
import com.google.common.primitives.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Objects;

public class LuaReduceQ extends LuaQ implements ReduceQ {

  public static final String PUBLISHED_REDUCE_POSTFIX = ":R:PUB";
  public static final String CLAIMED_REDUCE_POSTFIX = ":R:CLAIM";
  public static final String PAYLOAD_REDUCE_POSTFIX = ":R:PAYLOAD";
  public static final String NOTIFY_REDUCE_POSTFIX = ":R:NOTIFY";
  public static final String DLQ_REDUCE_POSTFIX = ":R:DEAD";

  public static final String PENDING_MAPPED_PREFIX_POSTFIX = ":R:M:PENDING:";
  public static final String MAPPED_RESULTS_PREFIX_POSTFIX = ":R:M:RESULTS:";
  public static final String MAPPED_RESULTS_NOTIFY_PREFIX_POSTFIX = ":R:M:NOTIFY:";

  protected final byte[] publishedReduceZKey;
  protected final byte[] claimedReduceHKey;
  protected final byte[] payloadReduceHKey;
  protected final byte[] notifyReduceLKey;
  protected final byte[] deadReduceHKey;

  protected final byte[] pendingMappedSKeyPrefix;
  protected final byte[] mappedResultsHKeyPrefix;
  protected final byte[] mappedResultsNotifyLKeyPrefix;

  private final long defaultReduceWeight;
  private final byte[] defaultReduceWeightBytes;

  public LuaReduceQ(final JedisExecutor jedisExecutor, final String qName) {

    this(jedisExecutor, qName, 1);
  }

  public LuaReduceQ(final JedisExecutor jedisExecutor, final String qName,
      final long defaultReduceWeight) {

    super(jedisExecutor, qName);

    this.publishedReduceZKey = (qName + PUBLISHED_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.claimedReduceHKey = (qName + CLAIMED_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.payloadReduceHKey = (qName + PAYLOAD_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.notifyReduceLKey = (qName + NOTIFY_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.deadReduceHKey = (qName + DLQ_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);

    this.pendingMappedSKeyPrefix =
        (qName + PENDING_MAPPED_PREFIX_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.mappedResultsHKeyPrefix =
        (qName + MAPPED_RESULTS_PREFIX_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.mappedResultsNotifyLKeyPrefix =
        (qName + MAPPED_RESULTS_NOTIFY_PREFIX_POSTFIX).getBytes(StandardCharsets.UTF_8);

    this.defaultReduceWeight = defaultReduceWeight;
    this.defaultReduceWeightBytes =
        String.valueOf(defaultReduceWeight).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public long getDefaultReduceWeight() {
    return defaultReduceWeight;
  }

  @Override
  public Long publishEpochReducible(final byte[] reduceId, final byte[] reducePayload,
      final Collection<byte[]> idPayloads) {

    return publishEpochReducible(reduceId, reducePayload, defaultReduceWeightBytes, idPayloads);
  }

  @Override
  public Long publishEpochReducible(final byte[] reduceId, final byte[] reducePayload,
      final Collection<byte[]> idPayloads, final int numRetries) {

    return publishEpochReducible(reduceId, reducePayload, defaultReduceWeightBytes, idPayloads,
        numRetries);
  }

  @Override
  public Long publishEpochReducible(final byte[] reduceId, final byte[] reducePayload,
      final byte[] reduceWeight, final Collection<byte[]> idPayloads, final int numRetries) {

    final byte[] pendingMappedSKey = Bytes.concat(pendingMappedSKeyPrefix, reduceId);
    final byte[] mappedResultsHKey = Bytes.concat(mappedResultsHKeyPrefix, reduceId);

    return ReduceQFunctions.publishEpochReducible(jedisExecutor, publishedZKey, claimedHKey,
        payloadsHKey, notifyLKey, pendingMappedSKey, mappedResultsHKey, publishedReduceZKey,
        claimedReduceHKey, payloadReduceHKey, reduceWeight, reduceId, reducePayload, idPayloads,
        numRetries);
  }

  @Override
  public Long publishReducible(final byte[] reduceId, final Collection<byte[]> idPayloads) {

    return publishReducible(reduceId, defaultReduceWeightBytes, idPayloads);
  }

  @Override
  public Long publishReducible(final byte[] reduceId, final Collection<byte[]> idPayloads,
      final int numRetries) {

    return publishReducible(reduceId, defaultReduceWeightBytes, idPayloads, numRetries);
  }

  @Override
  public Long publishReducible(final byte[] reduceId, final byte[] reduceWeight,
      final Collection<byte[]> idPayloads, final int numRetries) {

    final byte[] pendingMappedSKey = Bytes.concat(pendingMappedSKeyPrefix, reduceId);
    final byte[] mappedResultsHKey = Bytes.concat(mappedResultsHKeyPrefix, reduceId);

    return ReduceQFunctions.publishReducible(jedisExecutor, publishedZKey, claimedHKey,
        payloadsHKey, notifyLKey, pendingMappedSKey, mappedResultsHKey, publishedReduceZKey,
        claimedReduceHKey, reduceWeight, reduceId, idPayloads, numRetries);
  }

  @Override
  public Long
      publishMappedResult(final byte[] reduceId, final byte[] id, final byte[] resultPayload) {

    return publishMappedResult(reduceId, defaultReduceWeightBytes, id, resultPayload);
  }

  @Override
  public Long publishMappedResult(final byte[] reduceId, final byte[] id,
      final byte[] resultPayload, final int numRetries) {

    return publishMappedResult(reduceId, defaultReduceWeightBytes, id, resultPayload, numRetries);
  }

  @Override
  public Long publishMappedResult(final byte[] reduceId, final byte[] reduceWeight,
      final byte[] id, final byte[] resultPayload, final int numRetries) {

    final byte[] pendingMappedSKey = Bytes.concat(pendingMappedSKeyPrefix, reduceId);
    final byte[] mappedResultsHKey = Bytes.concat(mappedResultsHKeyPrefix, reduceId);
    final byte[] notifyMappedResultsLKey = Bytes.concat(mappedResultsNotifyLKeyPrefix, reduceId);

    return ReduceQFunctions.publishMappedResult(jedisExecutor, publishedReduceZKey,
        claimedReduceHKey, mappedResultsHKey, pendingMappedSKey, notifyReduceLKey,
        notifyMappedResultsLKey, claimedHKey, payloadsHKey, reduceId, reduceWeight, id,
        resultPayload, numRetries);
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other)
      return true;
    if (other == null)
      return false;
    if (!getClass().equals(other.getClass()))
      return false;
    final LuaReduceQ castOther = LuaReduceQ.class.cast(other);
    return Objects.equals(jedisExecutor, castOther.jedisExecutor)
        && Objects.equals(qName, castOther.qName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jedisExecutor, qName);
  }
}
