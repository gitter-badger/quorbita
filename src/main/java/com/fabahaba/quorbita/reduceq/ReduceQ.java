package com.fabahaba.quorbita.reduceq;

import com.fabahaba.quorbita.QuorbitaQ;

import java.util.Collection;
import java.util.List;

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

  default Long publishMappedResult(final byte[] reduceId, final byte[] reduceWeight,
      final byte[] id, final byte[] resultPayload) {

    return publishMappedResult(reduceId, reduceWeight, id, resultPayload, getDefaultNumRetries());
  }

  public Long publishMappedResult(final byte[] reduceId, final byte[] id,
      final byte[] resultPayload, final int numRetries);

  public Long
      publishMappedResult(final byte[] reduceId, final byte[] id, final byte[] resultPayload);

  public Long publishMappedResult(final byte[] reduceId, final byte[] reduceWeight,
      final byte[] id, final byte[] resultPayload, final int numRetries);

  default Long republishReducibleAs(final byte[] reduceId, final byte[] reducePayload) {

    return republishReducibleAs(reduceId, reducePayload, getDefaultNumRetries());
  }

  public Long republishReducibleAs(final byte[] reduceId, final byte[] reducePayload,
      final int numRetries);

  default Long republishReducible(final byte[] reduceId) {

    return republishReducible(reduceId, getDefaultNumRetries());
  }

  public Long republishReducible(final byte[] reduceId, final int numRetries);

  default Long republishDeadReducibleAs(final byte[] reduceId, final byte[] reducePayload) {

    return republishDeadReducibleAs(reduceId, reducePayload, getDefaultNumRetries());
  }

  public Long republishDeadReducibleAs(final byte[] reduceId, final byte[] reducePayload,
      final int numRetries);

  default Long republishDeadReducible(final byte[] reduceId) {

    return republishDeadReducible(reduceId, getDefaultNumRetries());
  }

  public Long republishDeadReducible(final byte[] reduceId, final int numRetries);

  default Long killReducibleAs(final byte[] reduceId, final byte[] reducePayload) {

    return killReducibleAs(reduceId, reducePayload, getDefaultNumRetries());
  }

  public Long killReducibleAs(final byte[] reduceId, final byte[] reducePayload,
      final int numRetries);

  default Long killReducible(final byte[] reduceId) {

    return killReducible(reduceId, getDefaultNumRetries());
  }

  public Long killReducible(final byte[] reduceId, final int numRetries);

  public List<byte[]> claimReducible();

  public List<byte[]> claimReducible(final int timeoutSeconds);

  public Long getPublishedReduceQSize();

  public Long getClaimedReduceQSize();

  public Long getReduceQSize();

  public Long getDeadReduceQSize();

  public List<Long> getReduceQSizes();
}
