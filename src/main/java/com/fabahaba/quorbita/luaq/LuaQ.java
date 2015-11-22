package com.fabahaba.quorbita.luaq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.BaseQ;
import com.fabahaba.quorbita.QuorbitaQ;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LuaQ extends BaseQ implements QuorbitaQ {

  public static final int DEFAULT_REMOVE_ORPHAN_PAYLOADS_BATCH_SIZE = 100;

  public LuaQ(final JedisExecutor jedisExecutor, final String qName) {

    super(jedisExecutor, qName);
  }

  @Override
  public Long publish(final String id, final byte[] payload, final int numRetries) {

    return publish(id.getBytes(StandardCharsets.UTF_8), payload, numRetries);
  }

  @Override
  public Long publish(final byte[] id, final byte[] payload, final int numRetries) {

    return LuaQFunctions.publish(jedisExecutor, id, payload, publishedZKey, claimedHKey,
        payloadsHKey, notifyLKey, numRetries);
  }

  @Override
  public Long publish(final Collection<byte[]> idPayloads, final int numRetries) {

    return LuaQFunctions.publish(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey,
        notifyLKey, idPayloads, numRetries);
  }

  @Override
  public Long republish(final String id, final int numRetries) {

    return republish(id.getBytes(StandardCharsets.UTF_8), numRetries);
  }

  @Override
  public Long republish(final byte[] id, final int numRetries) {

    return LuaQFunctions.republish(jedisExecutor, id, publishedZKey, claimedHKey, notifyLKey,
        numRetries);
  }

  @Override
  public Long republishAs(final String id, final byte[] payload, final int numRetries) {

    return LuaQFunctions.republishAs(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        publishedZKey, claimedHKey, notifyLKey, payloadsHKey, numRetries);
  }

  @Override
  public Long republishDead(final String id, final int numRetries) {

    return republishDead(id.getBytes(StandardCharsets.UTF_8), numRetries);
  }

  @Override
  public Long republishDead(final byte[] id, final int numRetries) {

    return LuaQFunctions.republish(jedisExecutor, id, publishedZKey, deadHKey, notifyLKey,
        numRetries);
  }

  @Override
  public Long republishDeadAs(final String id, final byte[] payload, final int numRetries) {

    return LuaQFunctions.republishAs(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        publishedZKey, deadHKey, notifyLKey, payloadsHKey, numRetries);
  }

  @Override
  public Long kill(final String id, final int numRetries) {

    return LuaQFunctions.kill(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), deadHKey,
        claimedHKey, numRetries);
  }

  @Override
  public Long killAs(final String id, final byte[] payload, final int numRetries) {

    return LuaQFunctions.killAs(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        deadHKey, claimedHKey, payloadsHKey, numRetries);
  }

  @Override
  public List<List<byte[]>> claim(final byte[] claimLimit) {

    return LuaQFunctions.nonBlockingClaim(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey,
        notifyLKey, claimLimit);
  }

  @Override
  public List<List<byte[]>> claim(final byte[] claimLimit, final int timeoutSeconds) {

    return LuaQFunctions.claim(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        claimLimit, timeoutSeconds);
  }

  @Override
  public void consume(final Function<List<List<byte[]>>, Boolean> idPayloadConsumer,
      final byte[] claimLimit, final int maxBlockOnEmptyQSeconds) {

    LuaQFunctions.consume(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        idPayloadConsumer, claimLimit, maxBlockOnEmptyQSeconds);
  }

  @Override
  public void consume(final Function<List<List<byte[]>>, Boolean> idPayloadConsumer,
      final byte[] claimLimit) {

    LuaQFunctions.consume(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        idPayloadConsumer, claimLimit);
  }

  @Override
  public boolean checkin(final String id, final int numRetries) {

    return LuaQFunctions.checkin(jedisExecutor, claimedHKey, id.getBytes(StandardCharsets.UTF_8)) == 0;
  }

  public long removePublished(final int numRetries, final String... ids) {

    return LuaQFunctions.zRemove(jedisExecutor, publishedZKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public long removeClaimed(final int numRetries, final String... ids) {

    return LuaQFunctions.hRemove(jedisExecutor, claimedHKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public long removeClaimed(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.hRemove(jedisExecutor, claimedHKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public long removeDead(final int numRetries, final String... ids) {

    return LuaQFunctions.hRemove(jedisExecutor, deadHKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public long removeDead(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.hRemove(jedisExecutor, deadHKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public long remove(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, publishedZKey, claimedHKey, deadHKey, payloadsHKey,
        numRetries, ids);
  }

  @Override
  public long remove(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.remove(jedisExecutor, publishedZKey, claimedHKey, deadHKey, payloadsHKey,
        numRetries, ids);
  }

  @Override
  public void clear(final int numRetries) {

    LuaQFunctions.clear(jedisExecutor, numRetries, publishedZKey, claimedHKey, payloadsHKey,
        notifyLKey, deadHKey);
  }

  @Override
  public void clearDLQ(final int numRetries) {

    LuaQFunctions.hClearQ(jedisExecutor, deadHKey, payloadsHKey, numRetries);
  }

  public List<byte[]> removeOrphanedPayloads() {

    return removeOrphanedPayloads(DEFAULT_REMOVE_ORPHAN_PAYLOADS_BATCH_SIZE);
  }

  public List<byte[]> removeOrphanedPayloads(final int batchSize) {

    return LuaQFunctions.removeOrphanedPayloads(jedisExecutor, publishedZKey, claimedHKey,
        payloadsHKey, batchSize);
  }

  @Override
  public Long getPublishedQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.zcard(publishedZKey), getDefaultNumRetries());
  }

  @Override
  public Long getClaimedQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.hlen(claimedHKey), getDefaultNumRetries());
  }

  @Override
  public Long getQSize() {

    return jedisExecutor
        .applyPipeline(
            pipeline -> ImmutableList.of(pipeline.zcard(publishedZKey), pipeline.hlen(claimedHKey)),
            getDefaultNumRetries()).stream().mapToLong(Response::get).sum();
  }

  @Override
  public List<Long> getQSizes() {

    return jedisExecutor
        .applyPipeline(
            pipeline -> ImmutableList.of(pipeline.zcard(publishedZKey), pipeline.hlen(claimedHKey),
                pipeline.hlen(deadHKey)), getDefaultNumRetries()).stream().map(Response::get)
        .collect(Collectors.toList());
  }

  @Override
  public Long getDLQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.hlen(deadHKey), getDefaultNumRetries());
  }

  @Override
  public void scanPublishedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions
        .zScanPayloads(jedisExecutor, publishedZKey, payloadsHKey, idScorePayloadsConsumer);
  }

  @Override
  public void scanClaimedIdScores(final Consumer<Entry<byte[], byte[]>> idValuesConsumer,
      final ScanParams scanParams) {

    LuaQFunctions.hScanIdValues(jedisExecutor, claimedHKey, scanParams, idValuesConsumer);
  }

  @Override
  public void scanClaimedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions.hScanPayloads(jedisExecutor, claimedHKey, payloadsHKey, idScorePayloadsConsumer);
  }

  @Override
  public void scanDeadPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions.hScanPayloads(jedisExecutor, deadHKey, payloadsHKey, idScorePayloadsConsumer);
  }

  @Override
  public void scanPayloadStates(final Consumer<List<List<Object>>> idPayloadStatesConsumer) {

    LuaQFunctions.scanPayloadStates(jedisExecutor, payloadsHKey, publishedZKey, claimedHKey,
        deadHKey, idPayloadStatesConsumer);
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
    if (other == null)
      return false;
    if (!getClass().equals(other.getClass()))
      return false;
    final LuaQ castOther = LuaQ.class.cast(other);
    return Objects.equals(jedisExecutor, castOther.jedisExecutor)
        && Objects.equals(qName, castOther.qName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jedisExecutor, qName);
  }
}
