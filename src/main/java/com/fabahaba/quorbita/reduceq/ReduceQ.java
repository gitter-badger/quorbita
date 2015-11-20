package com.fabahaba.quorbita.reduceq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.BaseQ;

import java.nio.charset.StandardCharsets;

public class ReduceQ extends BaseQ {

  public static final String PUBLISHED_REDUCE_POSTFIX = ":RPUB";
  public static final String CLAIMED_REDUCE_POSTFIX = ":RCLAIM";
  public static final String PENDING_REDUCE_POSTFIX = ":RPENDING";
  public static final String PAYLOAD_REDUCE_POSTFIX = ":RPAYLOAD";
  public static final String NOTIFY_REDUCE_POSTFIX = ":RNOTIFY";
  public static final String DLQ_REDUCE_POSTFIX = ":RDEAD";

  protected final byte[] publishedReduceZKey;
  protected final byte[] claimedReduceHKey;
  protected final byte[] pendingReduceSKey;
  protected final byte[] payloadReduceHKey;
  protected final byte[] notifyReduceLKey;
  protected final byte[] deadReduceHKey;

  public ReduceQ(final JedisExecutor jedisExecutor, final String qName) {

    super(jedisExecutor, qName);

    this.publishedReduceZKey = (qName + PUBLISHED_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.claimedReduceHKey = (qName + CLAIMED_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.pendingReduceSKey = (qName + PENDING_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.payloadReduceHKey = (qName + PAYLOAD_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.notifyReduceLKey = (qName + NOTIFY_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.deadReduceHKey = (qName + DLQ_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
  }

}
