package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import redis.clients.jedis.Response;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class LuaQ implements QuorbitaQ {

  public static final int DEFAULT_REMOVE_ORPHAN_PAYLOADS_BATCH_SIZE = 100;

  public static final String PUBLISHED_POSTFIX = ":PUBLISHED";
  public static final String CLAIMED_POSTFIX = ":CLAIMED";
  public static final String PAYLOADS_POSTFIX = ":PAYLOADS";
  public static final String NOTIFY_POSTFIX = ":NOTIFY";
  public static final String DLQ_POSTFIX = ":DEAD";

  private final JedisExecutor jedisExecutor;

  private final String qName;
  private final byte[] publishedQKey;
  private final byte[] claimedQKey;
  private final byte[] payloadsHashKey;
  private final byte[] notifyListKey;
  private final byte[] dlqKey;
  private final List<byte[]> keys;

  public LuaQ(final JedisExecutor jedisExecutor, final String qName) {

    this.jedisExecutor = jedisExecutor;
    this.qName = qName;
    this.publishedQKey = (qName + PUBLISHED_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.claimedQKey = (qName + CLAIMED_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.payloadsHashKey = (qName + PAYLOADS_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.notifyListKey = (qName + NOTIFY_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.dlqKey = (qName + DLQ_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.keys = ImmutableList.of(publishedQKey, claimedQKey, payloadsHashKey, notifyListKey);
  }

  @Override
  public JedisExecutor getJedisExecutor() {
    return jedisExecutor;
  }

  @Override
  public Long publish(final String id, final byte[] payload, final int numRetries) {

    return LuaQFunctions.publish(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        publishedQKey, claimedQKey, payloadsHashKey, notifyListKey, numRetries);
  }

  @Override
  public Long publish(final Collection<byte[]> idPayloads, final int numRetries) {

    return LuaQFunctions.publish(jedisExecutor, keys, idPayloads, numRetries);
  }

  @Override
  public Long republish(final String id, final int numRetries) {

    return LuaQFunctions.republish(jedisExecutor, id.getBytes(StandardCharsets.UTF_8),
        publishedQKey, claimedQKey, payloadsHashKey, notifyListKey, numRetries);
  }

  @Override
  public Long republishAs(final String id, final byte[] payload, final int numRetries) {

    return LuaQFunctions.republishAs(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        publishedQKey, claimedQKey, payloadsHashKey, notifyListKey, numRetries);
  }

  @Override
  public Long republishDeadAs(final String id, final byte[] payload, final int numRetries) {

    return LuaQFunctions.republishAs(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        publishedQKey, dlqKey, payloadsHashKey, notifyListKey, numRetries);
  }

  @Override
  public Long kill(final String id, final int numRetries) {

    return LuaQFunctions.kill(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), dlqKey,
        claimedQKey, payloadsHashKey, numRetries);
  }

  @Override
  public Long killAs(final String id, final byte[] payload, final int numRetries) {

    return LuaQFunctions.killAs(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        dlqKey, claimedQKey, payloadsHashKey, numRetries);
  }

  @Override
  public Long republishClaimedBefore(final byte[] before, final int numRetries) {

    return LuaQFunctions.republishClaimedBefore(jedisExecutor, publishedQKey, claimedQKey,
        notifyListKey, before, numRetries);
  }

  @Override
  public List<byte[]> claim() {

    return LuaQFunctions.nonBlockingClaim(jedisExecutor, publishedQKey, claimedQKey,
        payloadsHashKey, notifyListKey);
  }

  @Override
  public List<byte[]> claim(final int timeoutSeconds) {

    return LuaQFunctions.claim(jedisExecutor, publishedQKey, claimedQKey, payloadsHashKey,
        notifyListKey, timeoutSeconds);
  }

  @Override
  public Long checkin(final String id, final int numRetries) {

    return LuaQFunctions.checkin(jedisExecutor, claimedQKey, id.getBytes(StandardCharsets.UTF_8));
  }

  public long removePublished(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, publishedQKey, payloadsHashKey, numRetries, ids);
  }

  @Override
  public long removeClaimed(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, claimedQKey, payloadsHashKey, numRetries, ids);
  }

  @Override
  public long removeDead(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, dlqKey, payloadsHashKey, numRetries, ids);
  }

  @Override
  public long remove(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, publishedQKey, claimedQKey, payloadsHashKey,
        numRetries, ids);
  }

  @Override
  public void clear(final int numRetries) {

    LuaQFunctions.clear(jedisExecutor, numRetries, publishedQKey, claimedQKey, payloadsHashKey,
        notifyListKey, dlqKey);
  }

  @Override
  public void clearDLQ(final int numRetries) {

    LuaQFunctions.clearQ(jedisExecutor, dlqKey, payloadsHashKey, numRetries);
  }

  public List<byte[]> removeOrphanedPayloads() {

    return removeOrphanedPayloads(DEFAULT_REMOVE_ORPHAN_PAYLOADS_BATCH_SIZE);
  }

  public List<byte[]> removeOrphanedPayloads(final int batchSize) {

    return LuaQFunctions.removeOrphanedPayloads(jedisExecutor, publishedQKey, claimedQKey,
        payloadsHashKey, batchSize);
  }

  @Override
  public Long getPublishedQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.zcard(publishedQKey), getDefaultNumRetries());
  }

  @Override
  public Long getClaimedQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.zcard(claimedQKey), getDefaultNumRetries());
  }

  @Override
  public Long getQSize() {

    return jedisExecutor
        .applyPipeline(
            pipeline -> ImmutableList
                .of(pipeline.zcard(publishedQKey), pipeline.zcard(claimedQKey)),
            getDefaultNumRetries()).stream().mapToLong(Response::get).sum();
  }

  @Override
  public Long getDLQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.zcard(dlqKey), getDefaultNumRetries());
  }

  @Override
  public void scanClaimedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions.scanZSetPayloads(jedisExecutor, claimedQKey, payloadsHashKey,
        idScorePayloadsConsumer);
  }

  @Override
  public void scanPublishedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions.scanZSetPayloads(jedisExecutor, publishedQKey, payloadsHashKey,
        idScorePayloadsConsumer);
  }

  @Override
  public void scanDeadPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions.scanZSetPayloads(jedisExecutor, dlqKey, payloadsHashKey, idScorePayloadsConsumer);
  }

  @Override
  public String getQName() {
    return qName;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("jedisExecutor", jedisExecutor).add("qName", qName)
        .toString();
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other)
      return true;
    if (!(other instanceof LuaQ))
      return false;
    final LuaQ castOther = (LuaQ) other;
    return Objects.equals(jedisExecutor, castOther.jedisExecutor)
        && Objects.equals(qName, castOther.qName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jedisExecutor, qName);
  }
}
