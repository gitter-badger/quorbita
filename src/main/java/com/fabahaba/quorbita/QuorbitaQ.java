package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public interface QuorbitaQ {

  public static final int DEFAULT_NUM_RETRIES = 2;

  default int getDefaultNumRetries() {

    return DEFAULT_NUM_RETRIES;
  }

  public String getQName();

  public JedisExecutor getJedisExecutor();

  default Long publish(final String id, final byte[] payload) {

    return publish(id, payload, getDefaultNumRetries());
  }

  public Long publish(final String id, final byte[] payload, final int numRetries);

  default Long publish(final byte[] id, final byte[] payload) {

    return publish(id, payload, getDefaultNumRetries());
  }

  public Long publish(final byte[] id, final byte[] payload, final int numRetries);

  default Long publish(final Collection<byte[]> idPayloads) {

    return publish(idPayloads, getDefaultNumRetries());
  }

  public Long publish(final Collection<byte[]> idPayloads, final int numRetries);

  default Long republish(final String id) {

    return republish(id, getDefaultNumRetries());
  }

  public Long republish(final String id, final int numRetries);

  default Long republishAs(final String id, final byte[] payload) {

    return republishAs(id, payload, getDefaultNumRetries());
  }

  public Long republishAs(final String id, final byte[] payload, final int numRetries);

  default Long republishDeadAs(final String id, final byte[] payload) {

    return republishDeadAs(id, payload, getDefaultNumRetries());
  }

  public Long republishDeadAs(final String id, final byte[] payload, final int numRetries);

  default Long republishClaimedBefore(final long before) {

    return republishClaimedBefore(before, getDefaultNumRetries());
  }

  default Long republishClaimedBefore(final long before, final int numRetries) {

    return republishClaimedBefore(String.valueOf(before), numRetries);
  }

  default Long republishClaimedBefore(final String before) {

    return republishClaimedBefore(before, getDefaultNumRetries());
  }

  default Long republishClaimedBefore(final String before, final int numRetries) {

    return republishClaimedBefore(before.getBytes(StandardCharsets.UTF_8), numRetries);
  }

  default Long republishClaimedBefore(final byte[] before) {

    return republishClaimedBefore(before, getDefaultNumRetries());
  }

  public Long republishClaimedBefore(final byte[] before, final int numRetries);

  default Long kill(final String id) {

    return kill(id, getDefaultNumRetries());
  }

  public Long kill(final String id, final int numRetries);

  default Long killAs(final String id, final byte[] payload) {

    return killAs(id, payload, getDefaultNumRetries());
  }

  public Long killAs(final String id, final byte[] payload, final int numRetries);

  public List<byte[]> claim();

  public List<byte[]> claim(final int timeoutSeconds);

  public void consume(final Function<List<byte[]>, Boolean> idPayloadConsumer,
      final int maxBlockOnEmptyQSeconds);

  public void consume(final Function<List<byte[]>, Boolean> idPayloadConsumer);

  default Long checkin(final String id) {

    return checkin(id, getDefaultNumRetries());
  }

  public Long checkin(final String id, final int numRetries);

  default long removeClaimed(final String... ids) {

    return removeClaimed(getDefaultNumRetries(), ids);
  }

  public long removeClaimed(final int numRetries, final String... ids);

  default long removeClaimed(final byte[]... ids) {

    return removeClaimed(getDefaultNumRetries(), ids);
  }

  public long removeClaimed(final int numRetries, final byte[]... ids);

  default long removeDead(final String... ids) {

    return removeDead(getDefaultNumRetries(), ids);
  }

  public long removeDead(final int numRetries, final String... ids);

  default long removeDead(final byte[]... ids) {

    return removeDead(getDefaultNumRetries(), ids);
  }

  public long removeDead(final int numRetries, final byte[]... ids);

  default long remove(final String... ids) {

    return remove(getDefaultNumRetries(), ids);
  }

  public long remove(final int numRetries, final String... ids);

  default long remove(final byte[]... ids) {

    return remove(getDefaultNumRetries(), ids);
  }

  public long remove(final int numRetries, final byte[]... ids);

  default void clear() {

    clear(getDefaultNumRetries());
  }

  public void clear(final int numRetries);

  default void clearDLQ() {

    clearDLQ(getDefaultNumRetries());
  }

  public void clearDLQ(final int numRetries);

  public Long getPublishedQSize();

  public Long getClaimedQSize();

  public Long getQSize();

  public Long getDLQSize();

  public List<Long> getQSizes();

  public void scanPublishedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer);

  public void scanClaimedPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer);

  public void scanDeadPayloads(final Consumer<List<List<byte[]>>> idScorePayloadsConsumer);
}
