package com.fabahaba.quorbita.luaq;

import java.nio.ByteBuffer;
import java.util.List;

public class ClaimedIdPayloads {

  private final ByteBuffer claimToken;
  private final List<List<byte[]>> idPayloads;

  public ClaimedIdPayloads(final ByteBuffer claimToken, final List<List<byte[]>> idPayloads) {

    this.claimToken = claimToken;
    this.idPayloads = idPayloads;
  }

  public ByteBuffer getClaimToken() {
    return claimToken;
  }

  public List<List<byte[]>> getIdPayloads() {
    return idPayloads;
  }
}
