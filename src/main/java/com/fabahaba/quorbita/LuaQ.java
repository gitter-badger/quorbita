package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisSentinelPoolExecutor;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

public class LuaQ {

  public static final String PUBLISHED_POSTFIX = ":PUBLISHED";
  public static final String CLAIMED_POSTFIX = ":CLAIMED";
  public static final String PAYLOADS_POSTFIX = ":PAYLOADS";
  public static final String NOTIFY_POSTFIX = ":NOTIFY";

  private final JedisSentinelPoolExecutor jedisExecutor;

  private final String qName;
  private final byte[] publishedQKey;
  private final byte[] claimedQKey;
  private final byte[] payloadsHashKey;
  private final byte[] notifyListKey;
  private final List<byte[]> keys;

  public LuaQ(final JedisSentinelPoolExecutor jedisExecutor, final String qName) {
    this.jedisExecutor = jedisExecutor;
    this.qName = qName;
    this.publishedQKey = (qName + PUBLISHED_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.claimedQKey = (qName + CLAIMED_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.payloadsHashKey = (qName + PAYLOADS_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.notifyListKey = (qName + NOTIFY_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.keys = ImmutableList.of(publishedQKey, claimedQKey, payloadsHashKey, notifyListKey);
  }

  protected JedisSentinelPoolExecutor getJedisExecutor() {
    return this.jedisExecutor;
  }

  public Long publish(final String id, final byte[] payload) {

    return LuaQFunctions.publish(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        publishedQKey, claimedQKey, payloadsHashKey, notifyListKey);
  }

  public Long mpublish(final Collection<byte[]> idPayloads) {

    return LuaQFunctions.mpublish(jedisExecutor, keys, idPayloads);
  }

  public Long republish(final String id) {

    return LuaQFunctions.republish(jedisExecutor, id.getBytes(StandardCharsets.UTF_8),
        publishedQKey, claimedQKey, payloadsHashKey, notifyListKey);
  }

  public Long republishAs(final String id, final byte[] payload) {

    return LuaQFunctions.republishAs(jedisExecutor, id.getBytes(StandardCharsets.UTF_8), payload,
        publishedQKey, claimedQKey, payloadsHashKey, notifyListKey);
  }

  public List<byte[]> claim(final int timeoutSeconds) {

    return LuaQFunctions.claim(jedisExecutor, publishedQKey, claimedQKey, payloadsHashKey,
        notifyListKey, timeoutSeconds);
  }

  public long removePublished(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, publishedQKey, payloadsHashKey, numRetries, ids);
  }

  public long removeClaimed(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, claimedQKey, payloadsHashKey, numRetries, ids);
  }

  public long remove(final int numRetries, final String... ids) {

    return LuaQFunctions.remove(jedisExecutor, publishedQKey, claimedQKey, payloadsHashKey,
        numRetries, ids);
  }

  public void clear(final int numRetries) {

    LuaQFunctions.clear(jedisExecutor, publishedQKey, claimedQKey, payloadsHashKey, notifyListKey,
        numRetries);
  }

  public List<byte[]> removeOrphanedPayloads(final int batchSize) {

    return LuaQFunctions.removeOrphanedPayloads(jedisExecutor, publishedQKey, claimedQKey,
        payloadsHashKey, batchSize);
  }

  public String getQName() {
    return this.qName;
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
    return Objects.equal(qName, castOther.qName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(qName);
  }
}
