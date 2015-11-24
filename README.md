#quorbita [![Build Status](https://travis-ci.org/jamespedwards42/quorbita.svg)](https://travis-ci.org/jamespedwards42/quorbita) [![JCenter](https://api.bintray.com/packages/jamespedwards42/libs/quorbita/images/download.svg) ](https://bintray.com/jamespedwards42/libs/quorbita/_latestVersion) [![License](http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat) ](http://www.apache.org/licenses/LICENSE-2.0)

Quorbita is a Java 8 client library that turns Redis into a reliable message broker that supports:
* Priority Queues.  The default implementation uses timestamps on publish.  The lowest score will be claimed first.
* Batch methods for publishing, claiming, checking in, removing, killing and republishing.
* Extending claims via check-ins for long running tasks.
* Blocking claim methods.
* De-duping messages if they are already claimed or published.
* Claim timestamps may optionally serve as tokens to prevent concurrent client bugs during republishing, checking in, removing or killing.
* Killing messages (Dead Letter Queue).
* Methods for scanning claimed or dead messages for republishing or killing.
  * You will need a dedicated service to re-drive abandoned claimed or dead messages.
* Barebones interface works with byte arrays to allow for the reuse of serialized data.

####Motivation
The motivation for creating Quorbita was to reuse existing Redis infrastructure rather than having to maintain an additional set of servers for a message broker.  If you are doing any kind of distributed processing you probably need a message queue and it also makes sense to have the swiss army knife of databases, Redis, on hand to coordinate state.  

Quorbita isn't trying to compete with the likes of Kafka or NATS on performance, but it should perform better than other centrally brokered queues such as ActiveMQ and RabbitMQ depending on the CPU speed and load on your Redis instance.

###Usage
```java
final JedisExecutor jedisExecutor = new DirectJedisExecutor(new Jedis("localhost"));

LuaQScripts.loadMissingScripts(jedisExecutor);

final String qName = "QUORBITA";
final QuorbitaQ quorbitaLuaQ = new LuaQ(jedisExecutor, qName);

quorbitaLuaQ.clear();

quorbitaLuaQ.publish("ID-1".getBytes(StandardCharsets.UTF_8),
    "PAYLOAD-1".getBytes(StandardCharsets.UTF_8));

final List<byte[]> idPayloads =
    ImmutableList.of("ID-2".getBytes(StandardCharsets.UTF_8),
        "PAYLOAD-2".getBytes(StandardCharsets.UTF_8), "ID-3".getBytes(StandardCharsets.UTF_8),
        "PAYLOAD-3".getBytes(StandardCharsets.UTF_8));

quorbitaLuaQ.publish(idPayloads);

// Block if empty; 0 blocks forever. There are also non blocking claim methods.
final int blockingClaimTimeoutSeconds = 3;

// Claim at most 10 id payload pairs.
final byte[] claimLimit = LuaQFunctions.numToBytes(10);

quorbitaLuaQ.claim(claimLimit, blockingClaimTimeoutSeconds).ifPresent(claimedIdPayloads -> {
  for (final List<byte[]> idPayload : claimedIdPayloads.getIdPayloads()) {

    final byte[] idBytes = idPayload.get(0);
    final String id = new String(idBytes, StandardCharsets.UTF_8);
    final String payload = new String(idPayload.get(1), StandardCharsets.UTF_8);

    System.out.printf("Claimed message with id '%s' and payload '%s'.%n", id, payload);

    quorbitaLuaQ.removeClaimed(claimedIdPayloads.getClaimStamp(), idBytes);
    // quorbitaLuaQ.checkinClaimed(claimedIdPayloads.getClaimStamp(), idBytes);
  }
});
```

###Benchmarking
```java
// If you have a pooled executor:
// final JedisExecutor jedisExecutor = ...
// final ThroughputBenchmark benchmark = new ThroughputBenchmark(jedisExecutor);

// Otherwise:
final Jedis jedis = new Jedis("localhost", 6379);
final ThroughputBenchmark throughputBenchmark = new ThroughputBenchmark(jedis);

final int numJobs = 10000;
final int payloadSize = 1024;
final int publishBatchSize = 100;
final int consumeBatchSize = 100;
final boolean batchRemove = true;
final int numConsumers = 2;
final boolean concurrentPubSub = true;

throughputBenchmark.run(numJobs, payloadSize, publishBatchSize, concurrentPubSub, consumeBatchSize,
batchRemove, numConsumers);

final LatencyBenchmark latencyBenchmark = new LatencyBenchmark(jedis);

latencyBenchmark.run(numJobs, payloadSize, numConsumers);
```

###Dependency Management
####Gradle
```groovy
repositories {
   jcenter()
}

dependencies {
   // compile 'redis.clients:jedis:+'
   // compile 'com.fabahaba:jedipus:+'
   compile 'com.fabahaba:quorbita:+'
}
```
