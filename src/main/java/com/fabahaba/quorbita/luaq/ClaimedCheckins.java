package com.fabahaba.quorbita.luaq;

import java.nio.ByteBuffer;
import java.util.List;

public class ClaimedCheckins {

  private final ByteBuffer claimStamp;
  private final List<Long> checkins;

  public ClaimedCheckins(final ByteBuffer claimStamp, final List<Long> checkins) {

    this.claimStamp = claimStamp;
    this.checkins = checkins;
  }

  public ByteBuffer getClaimStamp() {
    return this.claimStamp;
  }

  public List<Long> getCheckins() {
    return this.checkins;
  }
}
