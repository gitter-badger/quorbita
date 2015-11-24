package com.fabahaba.quorbita.benchmarks;

import com.fabahaba.fava.collect.MapUtils;
import com.fabahaba.jedipus.DirectJedisExecutor;
import com.fabahaba.jedipus.JedisExecutor;
import com.fabahaba.quorbita.luaq.ClaimedIdPayloads;
import com.fabahaba.quorbita.luaq.LuaQ;
import com.fabahaba.quorbita.luaq.LuaQScripts;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;

import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

public class LatencyBenchmark {

  private final Jedis jedisPublisherAndPrototype;
  private final LuaQ luaQ;

  public LatencyBenchmark(final Jedis jedis) {

    this.jedisPublisherAndPrototype = jedis;

    final JedisExecutor jedisExecutor = new DirectJedisExecutor(jedis);

    LuaQScripts.loadMissingScripts(jedisExecutor);

    this.luaQ = new LuaQ(jedisExecutor, ThroughputBenchmark.class.getSimpleName());
  }

  public LatencyBenchmark(final JedisExecutor pooledJedisExecutor) {

    this.jedisPublisherAndPrototype = null;

    LuaQScripts.loadMissingScripts(pooledJedisExecutor);

    this.luaQ = new LuaQ(pooledJedisExecutor, ThroughputBenchmark.class.getSimpleName());
  }

  static class Consumer implements Runnable {

    private final LuaQ luaQ;
    private final CountDownLatch latch;
    private final AtomicBoolean donePublishing;
    private final Map<Integer, long[]> publishClaimedStamps;
    private final byte[] consumeBatchSize;

    public Consumer(final LuaQ luaQ, final CountDownLatch latch,
        final AtomicBoolean donePublishing, final Map<Integer, long[]> publishClaimedStamps,
        final int consumeBatchSize) {

      this.luaQ = luaQ;
      this.latch = latch;
      this.donePublishing = donePublishing;
      this.publishClaimedStamps = publishClaimedStamps;
      this.consumeBatchSize = String.valueOf(consumeBatchSize).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void run() {

      try {
        latch.await();
      } catch (final InterruptedException e) {
        throw Throwables.propagate(e);
      }

      luaQ.consume(this::consumeClaimedIdPaylods, consumeBatchSize);
    }

    private Boolean consumeClaimedIdPaylods(final ClaimedIdPayloads claimedIdPayloads) {
      final long claimedStamp = System.nanoTime();

      if (claimedIdPayloads.getIdPayloads().isEmpty()) {
        if (donePublishing.get()) {
          if (luaQ.getPublishedQSize() == 0)
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
      }

      final byte[][] ids = claimedIdPayloads.getIdPayloads().stream().map(idPayloads -> {
        final byte[] idBytes = idPayloads.get(0);
        final int id = Integer.parseInt(new String(idBytes, StandardCharsets.UTF_8));
        publishClaimedStamps.get(id)[1] = claimedStamp;
        return idBytes;
      }).toArray(byte[][]::new);

      luaQ.removeClaimed(claimedIdPayloads.getClaimStamp(), ids);

      return Boolean.TRUE;
    }
  }

  public void run(final int numJobs, final int payloadSizeBytes) {

    run(numJobs, payloadSizeBytes, Runtime.getRuntime().availableProcessors());
  }

  public void run(final int numJobs, final int payloadSizeBytes, final int numConsumers) {

    luaQ.clear();

    final ExecutorService consumerExecutor = Executors.newFixedThreadPool(numConsumers);
    final List<Future<?>> consumerFutures = new ArrayList<>(numConsumers);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean donePublishing = new AtomicBoolean(false);
    final Map<Integer, long[]> publishClaimedStamps = new HashMap<>(MapUtils.capacity(numJobs));
    for (int i = 0; i < numJobs; i++) {
      publishClaimedStamps.put(i, new long[2]);
    }

    for (int i = 0; i < numConsumers; i++) {

      if (jedisPublisherAndPrototype == null) {
        consumerFutures.add(consumerExecutor.submit(new Consumer(luaQ, startLatch, donePublishing,
            publishClaimedStamps, 1)));
        continue;
      }

      final LuaQ directConsumerQ =
          new LuaQ(new DirectJedisExecutor(new Jedis(jedisPublisherAndPrototype.getClient()
              .getHost(), jedisPublisherAndPrototype.getClient().getPort())),
              ThroughputBenchmark.class.getSimpleName());

      consumerFutures.add(consumerExecutor.submit(new Consumer(directConsumerQ, startLatch,
          donePublishing, publishClaimedStamps, 1)));
    }

    final byte[] payload = new byte[payloadSizeBytes];
    Arrays.fill(payload, (byte) 1);

    startLatch.countDown();

    for (int i = 0; i < numJobs; i++) {
      final byte[] id = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
      final long publishClaimedStamp = System.nanoTime();
      luaQ.publish(id, payload);
      publishClaimedStamps.get(i)[0] = publishClaimedStamp;
    }

    donePublishing.set(true);
    for (final Future<?> consumerFuture : consumerFutures) {
      Futures.getUnchecked(consumerFuture);
    }

    consumerExecutor.shutdown();

    final long[] latencyNanos =
        publishClaimedStamps.values().stream()
            .mapToLong(publishClaimedStamp -> publishClaimedStamp[1] - publishClaimedStamp[0])
            .sorted().toArray();

    System.out.printf("Min latency %.2f (ms)%n", latencyNanos[0] / 1000000.0);
    System.out.printf("Max latency %.2f (ms)%n", latencyNanos[latencyNanos.length - 1] / 1000000.0);

    final long medianNanos = latencyNanos[latencyNanos.length / 2];
    System.out.printf("Median latency %.2f (ms)%n", medianNanos / 1000000.0);

    final double avgNanos = LongStream.of(latencyNanos).average().orElse(-1);
    System.out.printf("Average latency %.2f (ms)%n", avgNanos / 1000000);
  }

  public static void main(final String[] args) {

    final LatencyBenchmark benchmark = new LatencyBenchmark(new Jedis("localhost"));

    final int numJobs = 100000;
    final int payloadSize = 1024;
    final int numConsumers = 2;

    benchmark.run(numJobs, payloadSize, numConsumers);
  }

}
