package com.fabahaba.quorbita;

import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.base.MoreObjects;

import java.nio.charset.StandardCharsets;

public abstract class BaseQ {

  public static final String PUBLISHED_POSTFIX = ":PUBLISHED";
  public static final String CLAIMED_POSTFIX = ":CLAIMED";
  public static final String PAYLOADS_POSTFIX = ":PAYLOADS";
  public static final String NOTIFY_POSTFIX = ":NOTIFY";
  public static final String DLQ_POSTFIX = ":DEAD";

  protected final JedisExecutor jedisExecutor;

  protected final String qName;
  protected final byte[] publishedZKey;
  protected final byte[] claimedHKey;
  protected final byte[] payloadsHKey;
  protected final byte[] notifyLKey;
  protected final byte[] deadHKey;

  protected BaseQ(final JedisExecutor jedisExecutor, final String qName) {

    this.jedisExecutor = jedisExecutor;
    this.qName = qName;
    this.publishedZKey = (qName + PUBLISHED_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.claimedHKey = (qName + CLAIMED_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.payloadsHKey = (qName + PAYLOADS_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.notifyLKey = (qName + NOTIFY_POSTFIX).getBytes(StandardCharsets.UTF_8);
    this.deadHKey = (qName + DLQ_POSTFIX).getBytes(StandardCharsets.UTF_8);
  }

  public JedisExecutor getJedisExecutor() {
    return jedisExecutor;
  }

  public String getQName() {
    return qName;
  }

  @Override
  public String toString() {

    return MoreObjects.toStringHelper(this).add("jedisExecutor", jedisExecutor).add("qName", qName)
        .toString();
  }
}
