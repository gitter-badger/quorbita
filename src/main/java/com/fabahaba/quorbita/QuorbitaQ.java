package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.luaq.ClaimedCheckins;
import com.fabahaba.quorbita.luaq.ClaimedIdPayloads;
import com.fabahaba.quorbita.luaq.LuaQFunctions;

import redis.clients.jedis.ScanParams;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public interface QuorbitaQ {

  public static final int DEFAULT_NUM_RETRIES = 2;

  default int getDefaultNumRetries() {

    return DEFAULT_NUM_RETRIES;
  }

  public String getQName();

  public JedisExecutor getJedisExecutor();

  default List<Long> publish(final byte[]... idPayloads) {

    return publish(getDefaultNumRetries(), idPayloads);
  }

  public List<Long> publish(final int numRetries, final byte[]... idPayloads);

  default List<Long> publish(final Collection<byte[]> idPayloads) {

    return publish(idPayloads, getDefaultNumRetries());
  }

  public List<Long> publish(final Collection<byte[]> idPayloads, final int numRetries);

  default List<Long> republish(final byte[]... ids) {

    return republish(getDefaultNumRetries(), ids);
  }

  public List<Long> republish(final int numRetries, final byte[]... ids);

  default List<Long> republish(final String... ids) {

    return republish(getDefaultNumRetries(), ids);
  }

  public List<Long> republish(final int numRetries, final String... ids);

  default List<Long> republishAs(final byte[]... idPayloads) {

    return republishAs(getDefaultNumRetries(), idPayloads);
  }

  public List<Long> republishAs(final int numRetries, byte[]... idPayloads);

  default List<Long> republishClaimed(final byte[] claimToken, final byte[]... ids) {

    return republishClaimed(claimToken, getDefaultNumRetries(), ids);
  }

  public List<Long> republishClaimed(final byte[] claimToken, final int numRetries,
      final byte[]... ids);

  default List<Long> republishClaimed(final byte[] claimToken, final String... ids) {

    return republishClaimed(claimToken, getDefaultNumRetries(), ids);
  }

  public List<Long> republishClaimed(final byte[] claimToken, final int numRetries,
      final String... ids);

  default List<Long> republishClaimedAs(final byte[] claimToken, final byte[]... idPayloads) {

    return republishClaimedAs(claimToken, getDefaultNumRetries(), idPayloads);
  }

  public List<Long> republishClaimedAs(final byte[] claimToken, final int numRetries,
      final byte[]... idPayloads);

  default List<Long> republishDead(final byte[]... ids) {

    return republishDead(getDefaultNumRetries(), ids);
  }

  public List<Long> republishDead(final int numRetries, final byte[]... ids);

  default List<Long> republishDead(final String... ids) {

    return republishDead(getDefaultNumRetries(), ids);
  }

  public List<Long> republishDead(final int numRetries, final String... ids);

  default List<Long> republishDeadAs(final byte[]... idPayloads) {

    return republishDeadAs(getDefaultNumRetries(), idPayloads);
  }

  public List<Long> republishDeadAs(final int numRetries, final byte[]... idPayloads);

  default List<Long> kill(final String... ids) {

    return kill(getDefaultNumRetries(), ids);
  }

  public List<Long> kill(final int numRetries, final String... ids);

  default List<Long> killAs(final byte[]... idPayloads) {

    return killAs(getDefaultNumRetries(), idPayloads);
  }

  public List<Long> killAs(final int numRetries, final byte[]... idPayloads);

  default List<Long> killClaimed(final byte[] claimToken, final String... ids) {

    return killClaimed(claimToken, getDefaultNumRetries(), ids);
  }

  public List<Long> killClaimed(final byte[] claimToken, final int numRetries, final String... ids);

  default List<Long> killClaimedAs(final byte[] claimToken, final byte[]... idPayloads) {

    return killClaimedAs(claimToken, getDefaultNumRetries(), idPayloads);
  }

  public List<Long> killClaimedAs(final byte[] claimToken, final int numRetries,
      final byte[]... idPayloads);

  public Optional<ClaimedIdPayloads> claim(final byte[] claimLimit);

  public Optional<ClaimedIdPayloads> claim(final byte[] claimLimit, final int timeoutSeconds);

  public void consume(final Function<ClaimedIdPayloads, Boolean> idPayloadConsumer,
      final byte[] claimLimit, final int maxBlockOnEmptyQSeconds);

  public void consume(final Function<ClaimedIdPayloads, Boolean> idPayloadConsumer,
      final byte[] claimLimit);

  default ClaimedCheckins checkin(final String... ids) {

    return checkin(getDefaultNumRetries(), ids);
  }

  public ClaimedCheckins checkin(final int numRetries, final String... ids);

  default ClaimedCheckins checkin(final byte[]... ids) {

    return checkin(getDefaultNumRetries(), ids);
  }

  public ClaimedCheckins checkin(final int numRetries, final byte[]... ids);

  default ClaimedCheckins checkinClaimed(final byte[] claimToken, final String... ids) {

    return checkinClaimed(claimToken, getDefaultNumRetries(), ids);
  }

  public ClaimedCheckins checkinClaimed(final byte[] claimToken, final int numRetries,
      final String... ids);

  default ClaimedCheckins checkinClaimed(final byte[] claimToken, final byte[]... ids) {

    return checkinClaimed(claimToken, getDefaultNumRetries(), ids);
  }

  public ClaimedCheckins checkinClaimed(final byte[] claimToken, final int numRetries,
      final byte[]... ids);


  default List<Long> remove(final String... ids) {

    return remove(getDefaultNumRetries(), ids);
  }

  public List<Long> remove(final int numRetries, final String... ids);

  default List<Long> remove(final byte[]... ids) {

    return remove(getDefaultNumRetries(), ids);
  }

  public List<Long> remove(final int numRetries, final byte[]... ids);

  default List<Long> removeClaimed(final byte[] claimToken, final String... ids) {

    return removeClaimed(claimToken, getDefaultNumRetries(), ids);
  }

  public List<Long> removeClaimed(final byte[] claimToken, final int numRetries,
      final String... ids);

  default List<Long> removeClaimed(final byte[] claimToken, final byte[]... ids) {

    return removeClaimed(claimToken, getDefaultNumRetries(), ids);
  }

  public List<Long> removeClaimed(final byte[] claimToken, final int numRetries,
      final byte[]... ids);

  default List<Long> removeDead(final String... ids) {

    return removeDead(getDefaultNumRetries(), ids);
  }

  public List<Long> removeDead(final int numRetries, final String... ids);

  default List<Long> removeDead(final byte[]... ids) {

    return removeDead(getDefaultNumRetries(), ids);
  }

  public List<Long> removeDead(final int numRetries, final byte[]... ids);

  default long removeAll(final String... ids) {

    return removeAll(getDefaultNumRetries(), ids);
  }

  public long removeAll(final int numRetries, final String... ids);

  default long removeAll(final byte[]... ids) {

    return removeAll(getDefaultNumRetries(), ids);
  }

  public long removeAll(final int numRetries, final byte[]... ids);

  default void clear() {

    clear(getDefaultNumRetries());
  }

  public void clear(final int numRetries);

  public Long getPublishedQSize();

  public Long getClaimedQSize();

  public Long getQSize();

  public Long getDeadQSize();

  public List<Long> getQSizes();

  public void scanPublishedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer);

  default void scanClaimedIdScores(final Consumer<List<Entry<byte[], byte[]>>> idValuesConsumer) {

    scanClaimedIdScores(idValuesConsumer, LuaQFunctions.DEFAULT_SCAN_PARAMS);
  }

  public void scanClaimedIdScores(final Consumer<List<Entry<byte[], byte[]>>> idValuesConsumer,
      final ScanParams scanParams);

  public void scanClaimedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer);

  public void scanDeadPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer);

  public void scanPayloadStates(final Consumer<List<List<Object>>> idPayloadStatesConsumer);
}
