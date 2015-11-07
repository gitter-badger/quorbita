package com.fabahaba.quorbita;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

public interface QuorbitaQ {

  public static final int DEFAULT_NUM_RETRIES = 2;

  default int getDefaultNumRetries() {

    return DEFAULT_NUM_RETRIES;
  }

  public String getQName();

  default Long publish(final String id, final byte[] payload) {

    return publish(id, payload, getDefaultNumRetries());
  }

  public Long publish(final String id, final byte[] payload, final int numRetries);

  default Long mpublish(final Collection<byte[]> idPayloads) {

    return mpublish(idPayloads, getDefaultNumRetries());
  }

  public Long mpublish(final Collection<byte[]> idPayloads, final int numRetries);

  default Long republish(final String id) {

    return republish(id, getDefaultNumRetries());
  }

  public Long republish(final String id, final int numRetries);

  default Long republishAs(final String id, final byte[] payload) {

    return republishAs(id, payload, getDefaultNumRetries());
  }

  public Long republishAs(final String id, final byte[] payload, final int numRetries);

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

  default List<byte[]> claim() {

    return claim(0);
  }

  public List<byte[]> claim(final int timeoutSeconds);

  public long removeClaimed(final int numRetries, final String... ids);

  public long remove(final int numRetries, final String... ids);

  default void clear() {

    clear(getDefaultNumRetries());
  }

  public void clear(final int numRetries);
}