#quorbita [![Build Status](https://travis-ci.org/jamespedwards42/quorbita.svg)](https://travis-ci.org/jamespedwards42/quorbita) [![JCenter](https://api.bintray.com/packages/jamespedwards42/libs/quorbita/images/download.svg) ](https://bintray.com/jamespedwards42/libs/quorbita/_latestVersion) [![License](http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat) ](http://www.apache.org/licenses/LICENSE-2.0)

###Usage
```java
final JedisExecutor jedisExecutor = ...;

LuaQScripts.loadMissingScripts(jedisExecutor);

final String qName = "QUORBITA";
final LuaQ quorbitaLuaQ = new LuaQ(jedisExecutor, qName);

quorbitaLuaQ.clear(0);

quorbitaLuaQ.publish("ID-1", "PAYLOAD-1".getBytes(StandardCharsets.UTF_8));

// id, payload, id, payload, ...
final List<byte[]> messages =
  ImmutableList.of("ID-2".getBytes(StandardCharsets.UTF_8),
      "PAYLOAD-2".getBytes(StandardCharsets.UTF_8), "ID-3".getBytes(StandardCharsets.UTF_8),
      "PAYLOAD-3".getBytes(StandardCharsets.UTF_8));

quorbitaLuaQ.mpublish(messages);

final int blockingClaimTimeoutSeconds = 3;

for (;;) {
final List<String> idPayload =
    quorbitaLuaQ.claim(blockingClaimTimeoutSeconds).stream()
        .map(i -> i == null ? null : new String(i, StandardCharsets.UTF_8))
        .collect(Collectors.toList());

final String id = idPayload.get(0);
if (id == null)
  return;

final String payload = idPayload.get(1);
System.out.println(String
    .format("Claimed message with id '%s' and payload '%s'", id, payload));

quorbitaLuaQ.removeClaimed(1, id);
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
