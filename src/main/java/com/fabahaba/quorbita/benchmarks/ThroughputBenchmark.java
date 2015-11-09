package com.fabahaba.quorbita.benchmarks;

import com.fabahaba.jedipus.DirectJedisExecutor;
import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.LuaQ;
import com.fabahaba.quorbita.LuaQScripts;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;

import redis.clients.jedis.Jedis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThroughputBenchmark {

  private final Jedis jedis;
  private final LuaQ publishQ;

  public ThroughputBenchmark(final Jedis jedis) {

    this.jedis = jedis;

    final JedisExecutor jedisExecutor = new DirectJedisExecutor(jedis);

    LuaQScripts.loadMissingScripts(jedisExecutor);

    this.publishQ = new LuaQ(jedisExecutor, ThroughputBenchmark.class.getSimpleName());
  }

  private static class Consumer implements Callable<long[]> {

    private final LuaQ luaQ;
    private final CountDownLatch latch;
    private final AtomicBoolean donePublishing;

    public Consumer(final LuaQ luaQ, final CountDownLatch latch, final AtomicBoolean donePublishing) {

      this.luaQ = luaQ;
      this.latch = latch;
      this.donePublishing = donePublishing;
    }

    @Override
    public long[] call() {

      final long[] startStop = new long[2];

      try {
        latch.await();
      } catch (final InterruptedException e) {
        throw Throwables.propagate(e);
      }

      startStop[0] = System.nanoTime();
      for (;;) {
        final List<byte[]> ipPayload = luaQ.claim();

        final byte[] id = ipPayload.get(0);

        if (id == null) {
          if (donePublishing.get()) {
            startStop[1] = System.nanoTime();
            return startStop;
          }
          continue;
        }

        luaQ.removeClaimed(id);
      }
    }
  }

  public void run(final int numJobs, final int payloadSizeBytes, final boolean concurrentPubSub) {

    run(numJobs, payloadSizeBytes, concurrentPubSub, Runtime.getRuntime().availableProcessors());
  }

  public void run(final int numJobs, final int payloadSizeBytes, final boolean concurrentPubSub,
      final int numConsumers) {

    publishQ.clear();

    final ExecutorService workerExecutor = Executors.newFixedThreadPool(numConsumers);
    final List<Future<long[]>> consumerFutures = new ArrayList<>(numConsumers);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean donePublishing = new AtomicBoolean(false);

    for (int i = 0; i < numConsumers; i++) {

      final LuaQ workerQ =
          new LuaQ(new DirectJedisExecutor(new Jedis(jedis.getClient().getHost(), jedis.getClient()
              .getPort())), ThroughputBenchmark.class.getSimpleName());

      consumerFutures.add(workerExecutor.submit(new Consumer(workerQ, startLatch, donePublishing)));
    }

    final byte[] payload = new byte[payloadSizeBytes];
    Arrays.fill(payload, (byte) 1);

    if (concurrentPubSub) {
      startLatch.countDown();
    }

    final long publishStart = System.nanoTime();
    for (long i = 0; i < numJobs; i++) {
      publishQ.publish(ByteBuffer.allocate(8).putLong(i).array(), payload);
    }
    final long publishDuration = System.nanoTime() - publishStart;
    donePublishing.set(true);

    if (!concurrentPubSub) {
      startLatch.countDown();
    }

    final long publishDurationMillis =
        TimeUnit.MILLISECONDS.convert(publishDuration, TimeUnit.NANOSECONDS);

    long minStart = Long.MAX_VALUE;
    long maxEnd = 0;
    for (final Future<long[]> consumerStartStopFuture : consumerFutures) {

      final long[] consumerStartStop = Futures.getUnchecked(consumerStartStopFuture);
      if (consumerStartStop[0] < minStart) {
        minStart = consumerStartStop[0];
      }
      if (consumerStartStop[1] > maxEnd) {
        maxEnd = consumerStartStop[1];
      }
    }

    workerExecutor.shutdown();

    final long consumeDuration = maxEnd - minStart;
    final long consumeDurationMillis =
        TimeUnit.MILLISECONDS.convert(consumeDuration, TimeUnit.NANOSECONDS);

    System.out.println("Publish / Second: "
        + (int) (numJobs * 1000 / (double) publishDurationMillis));
    System.out.println("Publish Duration Millis: " + publishDurationMillis);

    System.out.println("\nConsume / Second: "
        + (int) (numJobs * 1000 / (double) consumeDurationMillis));
    System.out.println("Consumption Duration Millis: " + consumeDurationMillis);

    if (concurrentPubSub)
      return;

    final long totalDuration = consumeDurationMillis + publishDurationMillis;

    System.out.println("\nPublish & Consume / Second: "
        + (int) (numJobs * 1000 / (double) totalDuration));
    System.out.println("Total Duration Millis: " + totalDuration);
  }

  public static void main(final String[] args) {

    final int numMessages = 100000;
    final int payloadSize = 1024;
    final int numConsumers = 2;
    final boolean concurrentPubSub = true;

    final ThroughputBenchmark benchmark = new ThroughputBenchmark(new Jedis("localhost"));

    benchmark.run(numMessages, payloadSize, concurrentPubSub, numConsumers);

    System.out.println();
  }
}
