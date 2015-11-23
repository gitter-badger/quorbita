package com.fabahaba.quorbita.reduceq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.luaq.LuaQ;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;

import redis.clients.jedis.Response;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LuaReduceQ extends LuaQ implements ReduceQ {

  private static final String REDUCE_POSTFIX_PREFIX = ":R";
  private static final String MAPPED_POSTFIX_PREFIX = ":M";

  public static final String PENDING_MAPPED_PREFIX_POSTFIX = REDUCE_POSTFIX_PREFIX
      + MAPPED_POSTFIX_PREFIX + ":PENDING:";
  public static final String MAPPED_RESULTS_PREFIX_POSTFIX = REDUCE_POSTFIX_PREFIX
      + MAPPED_POSTFIX_PREFIX + ":RESULTS:";
  public static final String MAPPED_RESULTS_NOTIFY_PREFIX_POSTFIX = REDUCE_POSTFIX_PREFIX
      + MAPPED_POSTFIX_PREFIX + ":NOTIFY:";

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

    this.publishedReduceZKey =
        (qName + REDUCE_POSTFIX_PREFIX + PUBLISHED_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.claimedReduceHKey =
        (qName + REDUCE_POSTFIX_PREFIX + CLAIMED_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.payloadReduceHKey =
        (qName + REDUCE_POSTFIX_PREFIX + PAYLOADS_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.notifyReduceLKey =
        (qName + REDUCE_POSTFIX_PREFIX + NOTIFY_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.deadReduceHKey =
        (qName + REDUCE_POSTFIX_PREFIX + DEAD_POSTFIX).getBytes(StandardCharsets.UTF_8);

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
  public Long republishReducibleAs(final byte[] reduceId, final byte[] reducePayload,
      final int numRetries) {

    return ReduceQFunctions
        .republishReducibleAs(jedisExecutor, publishedReduceZKey, claimedReduceHKey,
            notifyReduceLKey, payloadReduceHKey, reduceId, reducePayload, numRetries);
  }

  @Override
  public Long republishReducible(final byte[] reduceId, final int numRetries) {

    return ReduceQFunctions.republishReducible(jedisExecutor, publishedReduceZKey,
        claimedReduceHKey, notifyReduceLKey, reduceId, numRetries);
  }

  @Override
  public Long republishDeadReducibleAs(final byte[] reduceId, final byte[] reducePayload,
      final int numRetries) {

    return ReduceQFunctions.republishDeadReducibleAs(jedisExecutor, publishedReduceZKey,
        deadReduceHKey, notifyReduceLKey, payloadReduceHKey, reduceId, reducePayload, numRetries);
  }

  @Override
  public Long republishDeadReducible(final byte[] reduceId, final int numRetries) {

    return ReduceQFunctions.republishDeadReducibleAs(jedisExecutor, publishedReduceZKey,
        deadReduceHKey, notifyReduceLKey, reduceId, numRetries);
  }

  @Override
  public Long killReducibleAs(final byte[] reduceId, final byte[] reducePayload,
      final int numRetries) {

    final byte[] pendingMappedSKey = Bytes.concat(pendingMappedSKeyPrefix, reduceId);

    return ReduceQFunctions.killReducibleAs(jedisExecutor, deadReduceHKey, claimedReduceHKey,
        pendingMappedSKey, payloadReduceHKey, reduceId, reducePayload, numRetries);
  }

  @Override
  public Long killReducible(final byte[] reduceId, final int numRetries) {

    final byte[] pendingMappedSKey = Bytes.concat(pendingMappedSKeyPrefix, reduceId);

    return ReduceQFunctions.killReducible(jedisExecutor, deadReduceHKey, claimedReduceHKey,
        pendingMappedSKey, reduceId, numRetries);
  }

  @Override
  public List<byte[]> claimReducible() {

    return ReduceQFunctions.nonBlockingClaimReducible(jedisExecutor, publishedReduceZKey,
        claimedReduceHKey, payloadReduceHKey, notifyReduceLKey);
  }

  @Override
  public List<byte[]> claimReducible(final int timeoutSeconds) {

    return ReduceQFunctions.claimReducible(jedisExecutor, publishedReduceZKey, claimedReduceHKey,
        payloadReduceHKey, notifyReduceLKey, timeoutSeconds);
  }

  @Override
  public Long getPublishedReduceQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.zcard(publishedReduceZKey),
        getDefaultNumRetries());
  }

  @Override
  public Long getClaimedReduceQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.hlen(claimedReduceHKey), getDefaultNumRetries());
  }

  @Override
  public Long getReduceQSize() {

    return jedisExecutor
        .applyPipeline(
            pipeline -> ImmutableList.of(pipeline.zcard(publishedReduceZKey),
                pipeline.hlen(claimedReduceHKey)), getDefaultNumRetries()).stream()
        .mapToLong(Response::get).sum();
  }

  @Override
  public Long getDeadReduceQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.hlen(deadReduceHKey), getDefaultNumRetries());
  }

  @Override
  public List<Long> getReduceQSizes() {

    return jedisExecutor
        .applyPipeline(
            pipeline -> ImmutableList.of(pipeline.zcard(publishedReduceZKey),
                pipeline.hlen(claimedReduceHKey), pipeline.hlen(deadReduceHKey)),
            getDefaultNumRetries()).stream().map(Response::get).collect(Collectors.toList());
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
