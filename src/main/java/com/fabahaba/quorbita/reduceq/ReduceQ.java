package com.fabahaba.quorbita.reduceq;

import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.BaseQ;

import java.nio.charset.StandardCharsets;

public class ReduceQ extends BaseQ {

  public static final String PUBLISHED_REDUCE_POSTFIX = ":R:PUB";
  public static final String CLAIMED_REDUCE_POSTFIX = ":R:CLAIM";
  public static final String PAYLOAD_REDUCE_POSTFIX = ":R:PAYLOAD";
  public static final String NOTIFY_REDUCE_POSTFIX = ":R:NOTIFY";
  public static final String DLQ_REDUCE_POSTFIX = ":R:DEAD";

  public static final String PENDING_REDUCE_POSTFIX = ":R:PENDING:";
  public static final String RESULTS_REDUCE_POSTFIX = ":R:RESULTS:";
  public static final String NOTIFY_RESULTS_REDUCE_POSTFIX = ":R:NOTIFY:";

  protected final byte[] publishedReduceZKey;
  protected final byte[] claimedReduceHKey;
  protected final byte[] payloadReduceHKey;
  protected final byte[] notifyReduceLKey;
  protected final byte[] deadReduceHKey;

  protected final byte[] pendingReduceSKey;
  protected final byte[] resultsReduceHKey;
  protected final byte[] resultsNotifyReduceLKey;

  public ReduceQ(final JedisExecutor jedisExecutor, final String qName) {

    super(jedisExecutor, qName);

    this.publishedReduceZKey = (qName + PUBLISHED_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.claimedReduceHKey = (qName + CLAIMED_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.payloadReduceHKey = (qName + PAYLOAD_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.notifyReduceLKey = (qName + NOTIFY_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.deadReduceHKey = (qName + DLQ_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);

    this.pendingReduceSKey = (qName + PENDING_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.resultsReduceHKey = (qName + RESULTS_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.resultsNotifyReduceLKey =
        (qName + NOTIFY_RESULTS_REDUCE_POSTFIX).getBytes(StandardCharsets.UTF_8);
  }

}
