package com.fabahaba.quorbita.luaq;

import java.nio.ByteBuffer;
import java.util.List;

public class ClaimedCheckins {

  private final ByteBuffer claimToken;
  private final List<Long> checkins;

  public ClaimedCheckins(final ByteBuffer claimToken, final List<Long> checkins) {
    this.claimToken = claimToken;
    this.checkins = checkins;
  }

  public ByteBuffer getClaimToken() {
    return this.claimToken;
  }

  public List<Long> getCheckins() {
    return this.checkins;
  }
}
