package com.fabahaba.quorbita.luaq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.BaseQ;
import com.fabahaba.quorbita.QuorbitaQ;
import com.google.common.collect.ImmutableList;

import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LuaQ extends BaseQ implements QuorbitaQ {

  public static final int DEFAULT_REMOVE_ORPHAN_PAYLOADS_BATCH_SIZE = 100;

  public LuaQ(final JedisExecutor jedisExecutor, final String qName) {

    super(jedisExecutor, qName);
  }

  @Override
  public List<Long> publish(final int numRetries, final byte[]... idPayloads) {

    return LuaQFunctions.publish(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey,
        notifyLKey, numRetries, idPayloads);
  }

  @Override
  public List<Long> publish(final Collection<byte[]> idPayloads, final int numRetries) {

    return LuaQFunctions.publish(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey,
        notifyLKey, numRetries, idPayloads);
  }

  @Override
  public List<Long> republish(final int numRetries, final String... ids) {

    return LuaQFunctions.republish(jedisExecutor, publishedZKey, claimedHKey, notifyLKey,
        numRetries, ids);
  }

  @Override
  public List<Long> republish(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.republish(jedisExecutor, publishedZKey, claimedHKey, notifyLKey,
        numRetries, ids);
  }

  @Override
  public List<Long> republishAs(final int numRetries, final byte[]... idPayloads) {

    return LuaQFunctions.republishAs(jedisExecutor, publishedZKey, claimedHKey, notifyLKey,
        payloadsHKey, numRetries, idPayloads);
  }

  @Override
  public List<Long> republishClaimed(final byte[] claimToken, final int numRetries,
      final String... ids) {

    return LuaQFunctions.republishClaimed(jedisExecutor, claimToken, publishedZKey, claimedHKey,
        notifyLKey, numRetries, ids);
  }

  @Override
  public List<Long> republishClaimed(final byte[] claimToken, final int numRetries,
      final byte[]... ids) {

    return LuaQFunctions.republishClaimed(jedisExecutor, claimToken, publishedZKey, claimedHKey,
        notifyLKey, numRetries, ids);
  }

  @Override
  public List<Long> republishClaimedAs(final byte[] claimToken, final int numRetries,
      final byte[]... idPayloads) {

    return LuaQFunctions.republishClaimedAs(jedisExecutor, claimToken, publishedZKey, claimedHKey,
        notifyLKey, payloadsHKey, numRetries, idPayloads);
  }

  @Override
  public List<Long> republishDead(final int numRetries, final String... ids) {

    return LuaQFunctions.republish(jedisExecutor, publishedZKey, deadHKey, notifyLKey, numRetries,
        ids);
  }

  @Override
  public List<Long> republishDead(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.republish(jedisExecutor, publishedZKey, deadHKey, notifyLKey, numRetries,
        ids);
  }

  @Override
  public List<Long> republishDeadAs(final int numRetries, final byte[]... idPayloads) {

    return LuaQFunctions.republishAs(jedisExecutor, publishedZKey, deadHKey, notifyLKey,
        payloadsHKey, numRetries, idPayloads);
  }

  @Override
  public List<Long> kill(final int numRetries, final String... ids) {

    return LuaQFunctions.kill(jedisExecutor, deadHKey, claimedHKey, numRetries, ids);
  }

  @Override
  public List<Long> killAs(final int numRetries, final byte[]... idPayloads) {

    return LuaQFunctions.killAs(jedisExecutor, deadHKey, claimedHKey, payloadsHKey, numRetries,
        idPayloads);
  }

  @Override
  public List<Long> killClaimed(final byte[] claimToken, final int numRetries, final String... ids) {

    return LuaQFunctions.killClaimed(jedisExecutor, claimToken, deadHKey, claimedHKey, numRetries,
        ids);
  }

  @Override
  public List<Long> killClaimedAs(final byte[] claimToken, final int numRetries,
      final byte[]... idPayloads) {

    return LuaQFunctions.killClaimedAs(jedisExecutor, claimToken, deadHKey, claimedHKey,
        payloadsHKey, numRetries, idPayloads);
  }

  @Override
  public Optional<ClaimedIdPayloads> claim(final byte[] claimLimit) {

    return LuaQFunctions.nonBlockingClaim(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey,
        notifyLKey, claimLimit);
  }

  @Override
  public Optional<ClaimedIdPayloads> claim(final byte[] claimLimit, final int timeoutSeconds) {

    return LuaQFunctions.claim(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        claimLimit, timeoutSeconds);
  }

  @Override
  public void consume(final Function<ClaimedIdPayloads, Boolean> idPayloadConsumer,
      final byte[] claimLimit, final int maxBlockOnEmptyQSeconds) {

    LuaQFunctions.consume(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        idPayloadConsumer, claimLimit, maxBlockOnEmptyQSeconds);
  }

  @Override
  public void consume(final Function<ClaimedIdPayloads, Boolean> idPayloadConsumer,
      final byte[] claimLimit) {

    LuaQFunctions.consume(jedisExecutor, publishedZKey, claimedHKey, payloadsHKey, notifyLKey,
        idPayloadConsumer, claimLimit);
  }

  @Override
  public ClaimedCheckins checkin(final int numRetries, final String... ids) {

    return LuaQFunctions.checkin(jedisExecutor, claimedHKey, numRetries, ids);
  }

  @Override
  public ClaimedCheckins checkin(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.checkin(jedisExecutor, claimedHKey, numRetries, ids);
  }

  @Override
  public ClaimedCheckins checkinClaimed(final byte[] claimToken, final int numRetries,
      final String... ids) {

    return LuaQFunctions.checkinClaimed(jedisExecutor, claimToken, claimedHKey, numRetries, ids);
  }

  @Override
  public ClaimedCheckins checkinClaimed(final byte[] claimToken, final int numRetries,
      final byte[]... ids) {

    return LuaQFunctions.checkinClaimed(jedisExecutor, claimToken, claimedHKey, numRetries, ids);
  }

  @Override
  public List<Long> remove(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, claimedHKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public List<Long> remove(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.remove(jedisExecutor, claimedHKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public List<Long> removeClaimed(final byte[] claimToken, final int numRetries,
      final String... ids) {

    return LuaQFunctions.removeClaimed(jedisExecutor, claimToken, claimedHKey, payloadsHKey,
        numRetries, ids);
  }

  @Override
  public List<Long> removeClaimed(final byte[] claimToken, final int numRetries,
      final byte[]... ids) {

    return LuaQFunctions.removeClaimed(jedisExecutor, claimToken, claimedHKey, payloadsHKey,
        numRetries, ids);
  }

  @Override
  public List<Long> removeDead(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, deadHKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public List<Long> removeDead(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.remove(jedisExecutor, deadHKey, payloadsHKey, numRetries, ids);
  }

  @Override
  public long removeAll(final int numRetries, final String... ids) {

    return LuaQFunctions.removeAll(jedisExecutor, publishedZKey, claimedHKey, deadHKey,
        payloadsHKey, numRetries, ids);
  }

  @Override
  public long removeAll(final int numRetries, final byte[]... ids) {

    return LuaQFunctions.removeAll(jedisExecutor, publishedZKey, claimedHKey, deadHKey,
        payloadsHKey, numRetries, ids);
  }

  @Override
  public void clear(final int numRetries) {

    LuaQFunctions.clear(jedisExecutor, numRetries, publishedZKey, claimedHKey, payloadsHKey,
        notifyLKey, deadHKey);
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
  public Long getDeadQSize() {

    return jedisExecutor.applyJedis(jedis -> jedis.hlen(deadHKey), getDefaultNumRetries());
  }

  @Override
  public void scanPublishedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer) {

    LuaQFunctions
        .zScanPayloads(jedisExecutor, publishedZKey, payloadsHKey, idScorePayloadsConsumer);
  }

  @Override
  public void scanClaimedIdScores(final Consumer<List<Entry<byte[], byte[]>>> idValuesConsumer,
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
