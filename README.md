#quorbita [![Build Status](https://travis-ci.org/jamespedwards42/quorbita.svg)](https://travis-ci.org/jamespedwards42/quorbita) [![JCenter](https://api.bintray.com/packages/jamespedwards42/libs/quorbita/images/download.svg) ](https://bintray.com/jamespedwards42/libs/quorbita/_latestVersion) [![License](http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat) ](http://www.apache.org/licenses/LICENSE-2.0)

###Usage
```java
final JedisExecutor jedisExecutor = ...;

LuaQScripts.loadMissingScripts(jedisExecutor);

final String qName = "QUORBITA";
final QuorbitaQ quorbitaLuaQ = new LuaQ(jedisExecutor, qName);

quorbitaLuaQ.clear();

quorbitaLuaQ.publish("ID-1", "PAYLOAD-1".getBytes(StandardCharsets.UTF_8));

final List<byte[]> idPayloads =
    ImmutableList.of("ID-2".getBytes(StandardCharsets.UTF_8),
        "PAYLOAD-2".getBytes(StandardCharsets.UTF_8), "ID-3".getBytes(StandardCharsets.UTF_8),
        "PAYLOAD-3".getBytes(StandardCharsets.UTF_8));
quorbitaLuaQ.publish(idPayloads);

final int blockingClaimTimeoutSeconds = 3;

for (;;) {
  final List<byte[]> idPayload = quorbitaLuaQ.claim(blockingClaimTimeoutSeconds);

  final byte[] idBytes = idPayload.get(0);
  if (idBytes == null) {
    break;
  }

  final String id = new String(idBytes, StandardCharsets.UTF_8);
  final String payload = new String(idPayload.get(1), StandardCharsets.UTF_8);

  System.out.println(String
      .format("Claimed message with id '%s' and payload '%s'", id, payload));

  quorbitaLuaQ.removeClaimed(id);
  // quorbitaLuaQ.checkin(id)
}

// republish abandoned ids
// quorbitaLuaQ.republishClaimedBefore(System.currentTimeMillis() - 60000);
```

###Dependency Management
####Gradle
```groovy
repositories {
   jcenter()
}

dependencies {
   compile 'com.fabahaba:quorbita:+'
}
```
