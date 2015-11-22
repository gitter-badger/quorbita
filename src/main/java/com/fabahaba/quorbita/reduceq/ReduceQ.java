package com.fabahaba.quorbita.reduceq;

import com.fabahaba.quorbita.QuorbitaQ;

import java.util.Collection;

public interface ReduceQ extends QuorbitaQ {

  public long getDefaultReduceWeight();

  default Long publishEpochReducible(final byte[] reduceId, final byte[] reducePayload,
      final byte[] reduceWeight, final Collection<byte[]> idPayloads) {

    return publishEpochReducible(reduceId, reducePayload, reduceWeight, idPayloads,
        getDefaultNumRetries());
  }

  public Long publishEpochReducible(final byte[] reduceId, final byte[] reducePayload,
      final Collection<byte[]> idPayloads);

  public Long publishEpochReducible(final byte[] reduceId, final byte[] reducePayload,
      final Collection<byte[]> idPayloads, final int numRetries);

  public Long publishEpochReducible(final byte[] reduceId, final byte[] reducePayload,
      final byte[] reduceWeight, final Collection<byte[]> idPayloads, final int numRetries);

  default Long publishReducible(final byte[] reduceId, final byte[] reduceWeight,
      final Collection<byte[]> idPayloads) {

    return publishReducible(reduceId, reduceWeight, idPayloads, getDefaultNumRetries());
  }

  public Long publishReducible(final byte[] reduceId, final Collection<byte[]> idPayloads);

  public Long publishReducible(final byte[] reduceId, final Collection<byte[]> idPayloads,
      final int numRetries);

  public Long publishReducible(final byte[] reduceId, final byte[] reduceWeight,
      final Collection<byte[]> idPayloads, final int numRetries);
}
