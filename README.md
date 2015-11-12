#quorbita [![Build Status](https://travis-ci.org/jamespedwards42/quorbita.svg)](https://travis-ci.org/jamespedwards42/quorbita) [![JCenter](https://api.bintray.com/packages/jamespedwards42/libs/quorbita/images/download.svg) ](https://bintray.com/jamespedwards42/libs/quorbita/_latestVersion) [![License](http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat) ](http://www.apache.org/licenses/LICENSE-2.0)

>Quorbita is a Java 8 client library that turns Redis into a reliable message broker.  

###Usage
```java
final JedisExecutor jedisExecutor = new DirectJedisExecutor(new Jedis("localhost"));

LuaQScripts.loadMissingScripts(jedisExecutor);

final String qName = "QUORBITA";
final QuorbitaQ quorbitaLuaQ = new LuaQ(jedisExecutor, qName);

quorbitaLuaQ.clear();

quorbitaLuaQ.publish("ID-1", "PAYLOAD-1".getBytes(StandardCharsets.UTF_8));

final List<byte[]> idPayloads = ImmutableList.of(
      "ID-2".getBytes(StandardCharsets.UTF_8),
      "PAYLOAD-2".getBytes(StandardCharsets.UTF_8),
      "ID-3".getBytes(StandardCharsets.UTF_8),
      "PAYLOAD-3".getBytes(StandardCharsets.UTF_8));

quorbitaLuaQ.publish(idPayloads);

// Block if empty; 0 blocks forever. There are also non blocking claim methods.
final int blockingClaimTimeoutSeconds = 3;

// Claim at most 10 id payload pairs.
final byte[] claimLimit = "10".getBytes(StandardCharsets.UTF_8);

for (final List<byte[]> idPayload : quorbitaLuaQ.claim(claimLimit, blockingClaimTimeoutSeconds)) {

  final String id = new String(idPayload.get(0), StandardCharsets.UTF_8);
  final String payload = new String(idPayload.get(1), StandardCharsets.UTF_8);

  System.out.printf("Claimed message with id '%s' and payload '%s'.%n", id, payload);

  quorbitaLuaQ.removeClaimed(id);
  // quorbitaLuaQ.checkin(id)
}
```

###Benchmarking
```java
// If you have a pooled executor:
// final JedisExecutor jedisExecutor = ...
// final ThroughputBenchmark benchmark = new ThroughputBenchmark(jedisExecutor);

// Otherwise
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

final int numJobs = 10000;
final int payloadSize = 1024;
final int numConsumers = 2;

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
