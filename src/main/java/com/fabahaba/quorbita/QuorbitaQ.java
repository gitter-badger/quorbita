package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.luaq.ClaimedCheckins;
import com.fabahaba.quorbita.luaq.ClaimedIdPayloads;
import com.fabahaba.quorbita.luaq.LuaQFunctions;

import redis.clients.jedis.ScanParams;

import java.nio.ByteBuffer;
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

  default byte[] getDefaultInversePriority() {

    return LuaQFunctions.getEpochMillisBytes();
  }

  public String getQName();

  public JedisExecutor getJedisExecutor();

  default List<Long> publish(final byte[]... idPayloads) {

    return publish(getDefaultNumRetries(), idPayloads);
  }

  default List<Long> publish(final long inversePriority, final byte[]... idPayloads) {

    return publish(LuaQFunctions.longToBytes(inversePriority), getDefaultNumRetries(), idPayloads);
  }

  default List<Long> publish(final int numRetries, final byte[]... idPayloads) {

    return publish(getDefaultInversePriority(), numRetries, idPayloads);
  }

  public List<Long> publish(final byte[] inversePriority, final int numRetries,
      final byte[]... idPayloads);

  default List<Long> publish(final Collection<byte[]> idPayloads) {

    return publish(idPayloads, getDefaultNumRetries());
  }

  default List<Long> publish(final byte[] inversePriority, final Collection<byte[]> idPayloads) {

    return publish(inversePriority, idPayloads, getDefaultNumRetries());
  }

  default List<Long> publish(final Collection<byte[]> idPayloads, final int numRetries) {

    return publish(getDefaultInversePriority(), idPayloads, numRetries);
  }

  public List<Long> publish(final byte[] inversePriority, final Collection<byte[]> idPayloads,
      final int numRetries);

  default List<Long> republish(final byte[]... ids) {

    return republish(getDefaultNumRetries(), ids);
  }

  default List<Long> republish(final long inversePriority, final byte[]... ids) {

    return republish(LuaQFunctions.longToBytes(inversePriority), getDefaultNumRetries(), ids);
  }

  default List<Long> republish(final int numRetries, final byte[]... ids) {

    return republish(getDefaultInversePriority(), numRetries, ids);
  }

  public List<Long> republish(final byte[] inversePriority, final int numRetries,
      final byte[]... ids);

  default List<Long> republish(final String... ids) {

    return republish(getDefaultNumRetries(), ids);
  }

  default List<Long> republish(final byte[] inversePriority, final String... ids) {

    return republish(inversePriority, getDefaultNumRetries(), ids);
  }

  default List<Long> republish(final int numRetries, final String... ids) {

    return republish(getDefaultInversePriority(), numRetries, ids);
  }

  public List<Long> republish(final byte[] inversePriority, final int numRetries,
      final String... ids);

  default List<Long> republishAs(final byte[]... idPayloads) {

    return republishAs(getDefaultNumRetries(), idPayloads);
  }

  default List<Long> republishAs(final long inversePriority, final byte[]... idPayloads) {

    return republishAs(LuaQFunctions.longToBytes(inversePriority), getDefaultNumRetries(),
        idPayloads);
  }

  default List<Long> republishAs(final int numRetries, final byte[]... idPayloads) {

    return republishAs(getDefaultInversePriority(), numRetries, idPayloads);
  }

  public List<Long> republishAs(final byte[] inversePriority, final int numRetries,
      byte[]... idPayloads);

  default List<Long> republishClaimed(final ByteBuffer claimStamp, final byte[]... ids) {

    return republishClaimed(claimStamp.array(), getDefaultNumRetries(), ids);
  }

  default List<Long> republishClaimed(final byte[] inversePriority, final ByteBuffer claimStamp,
      final byte[]... ids) {

    return republishClaimed(inversePriority, claimStamp.array(), getDefaultNumRetries(), ids);
  }

  default List<Long> republishClaimed(final byte[] claimStamp, final int numRetries,
      final byte[]... ids) {

    return republishClaimed(getDefaultInversePriority(), claimStamp, numRetries, ids);
  }

  public List<Long> republishClaimed(final byte[] inversePriority, final byte[] claimStamp,
      final int numRetries, final byte[]... ids);

  default List<Long> republishClaimed(final byte[] claimStamp, final String... ids) {

    return republishClaimed(claimStamp, getDefaultNumRetries(), ids);
  }

  default List<Long> republishClaimed(final byte[] inversePriority, final byte[] claimStamp,
      final String... ids) {

    return republishClaimed(inversePriority, claimStamp, getDefaultNumRetries(), ids);
  }

  default List<Long> republishClaimed(final byte[] claimStamp, final int numRetries,
      final String... ids) {

    return republishClaimed(getDefaultInversePriority(), claimStamp, numRetries, ids);
  }

  public List<Long> republishClaimed(final byte[] inversePriority, final byte[] claimStamp,
      final int numRetries, final String... ids);

  default List<Long> republishClaimedAs(final ByteBuffer claimStamp, final byte[]... idPayloads) {

    return republishClaimedAs(claimStamp.array(), getDefaultNumRetries(), idPayloads);
  }

  default List<Long> republishClaimedAs(final byte[] inversePriority, final ByteBuffer claimStamp,
      final byte[]... idPayloads) {

    return republishClaimedAs(inversePriority, claimStamp.array(), getDefaultNumRetries(),
        idPayloads);
  }

  default List<Long> republishClaimedAs(final byte[] claimStamp, final int numRetries,
      final byte[]... idPayloads) {

    return republishClaimedAs(getDefaultInversePriority(), claimStamp, numRetries, idPayloads);
  }

  public List<Long> republishClaimedAs(final byte[] inversePriority, final byte[] claimStamp,
      final int numRetries, final byte[]... idPayloads);

  default List<Long> republishDead(final byte[]... ids) {

    return republishDead(getDefaultNumRetries(), ids);
  }

  default List<Long> republishDead(final long inversePriority, final byte[]... ids) {

    return republishDead(LuaQFunctions.longToBytes(inversePriority), getDefaultNumRetries(), ids);
  }

  default List<Long> republishDead(final int numRetries, final byte[]... ids) {

    return republishDead(getDefaultInversePriority(), numRetries, ids);
  }

  public List<Long> republishDead(final byte[] inversePriority, final int numRetries,
      final byte[]... ids);

  default List<Long> republishDead(final String... ids) {

    return republishDead(getDefaultNumRetries(), ids);
  }

  default List<Long> republishDead(final byte[] inversePriority, final String... ids) {

    return republishDead(inversePriority, getDefaultNumRetries(), ids);
  }

  default List<Long> republishDead(final int numRetries, final String... ids) {

    return republishDead(getDefaultInversePriority(), numRetries, ids);
  }

  public List<Long> republishDead(final byte[] inversePriority, final int numRetries,
      final String... ids);

  default List<Long> republishDeadAs(final byte[]... idPayloads) {

    return republishDeadAs(getDefaultNumRetries(), idPayloads);
  }

  default List<Long> republishDeadAs(final long inversePriority, final byte[]... idPayloads) {

    return republishDeadAs(LuaQFunctions.longToBytes(inversePriority), getDefaultNumRetries(),
        idPayloads);
  }

  default List<Long> republishDeadAs(final int numRetries, final byte[]... idPayloads) {

    return republishDeadAs(getDefaultInversePriority(), numRetries, idPayloads);
  }

  public List<Long> republishDeadAs(final byte[] inversePriority, final int numRetries,
      final byte[]... idPayloads);

  default List<Long> kill(final String... ids) {

    return kill(getDefaultNumRetries(), ids);
  }

  public List<Long> kill(final int numRetries, final String... ids);

  default List<Long> killAs(final byte[]... idPayloads) {

    return killAs(getDefaultNumRetries(), idPayloads);
  }

  public List<Long> killAs(final int numRetries, final byte[]... idPayloads);

  default List<Long> killClaimed(final byte[] claimStamp, final String... ids) {

    return killClaimed(claimStamp, getDefaultNumRetries(), ids);
  }

  public List<Long> killClaimed(final byte[] claimStamp, final int numRetries, final String... ids);

  default List<Long> killClaimedAs(final ByteBuffer claimStamp, final byte[]... idPayloads) {

    return killClaimedAs(claimStamp.array(), getDefaultNumRetries(), idPayloads);
  }

  public List<Long> killClaimedAs(final byte[] claimStamp, final int numRetries,
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

  default ClaimedCheckins checkinClaimed(final byte[] claimStamp, final String... ids) {

    return checkinClaimed(claimStamp, getDefaultNumRetries(), ids);
  }

  public ClaimedCheckins checkinClaimed(final byte[] claimStamp, final int numRetries,
      final String... ids);

  default ClaimedCheckins checkinClaimed(final ByteBuffer claimStamp, final byte[]... ids) {

    return checkinClaimed(claimStamp.array(), getDefaultNumRetries(), ids);
  }

  public ClaimedCheckins checkinClaimed(final byte[] claimStamp, final int numRetries,
      final byte[]... ids);

  default List<Long> remove(final String... ids) {

    return remove(getDefaultNumRetries(), ids);
  }

  public List<Long> remove(final int numRetries, final String... ids);

  default List<Long> remove(final byte[]... ids) {

    return remove(getDefaultNumRetries(), ids);
  }

  public List<Long> remove(final int numRetries, final byte[]... ids);

  default List<Long> removeClaimed(final byte[] claimStamp, final String... ids) {

    return removeClaimed(claimStamp, getDefaultNumRetries(), ids);
  }

  public List<Long> removeClaimed(final byte[] claimStamp, final int numRetries,
      final String... ids);

  default List<Long> removeClaimed(final ByteBuffer claimStamp, final byte[]... ids) {

    return removeClaimed(claimStamp.array(), getDefaultNumRetries(), ids);
  }

  public List<Long> removeClaimed(final byte[] claimStamp, final int numRetries,
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

  default void scanClaimedIdStampPairs(final Consumer<List<Entry<byte[], byte[]>>> idStampConsumer) {

    scanClaimedIdStampPairs(idStampConsumer, LuaQFunctions.DEFAULT_SCAN_PARAMS);
  }

  public void scanClaimedIdStampPairs(final Consumer<List<Entry<byte[], byte[]>>> idStampConsumer,
      final ScanParams scanParams);

  public void scanClaimedPayloads(final Consumer<List<List<byte[]>>> idStampPayloadsConsumer);

  public void scanDeadPayloads(final Consumer<List<List<byte[]>>> idStampPayloadsConsumer);

  public void scanPayloadStates(final Consumer<List<List<Object>>> idValuePayloadsConsumer);
}
