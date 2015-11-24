package com.fabahaba.quorbita.luaq;

import java.nio.ByteBuffer;
import java.util.List;

public class ClaimedIdPayloads {

  private final ByteBuffer claimStamp;
  private final List<List<byte[]>> idPayloads;

  public ClaimedIdPayloads(final ByteBuffer claimStamp, final List<List<byte[]>> idPayloads) {

    this.claimStamp = claimStamp;
    this.idPayloads = idPayloads;
  }

  public ByteBuffer getClaimStamp() {
    return claimStamp;
  }

  public List<List<byte[]>> getIdPayloads() {
    return idPayloads;
  }
}
