package com.fabahaba.quorbita;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LuaScriptData implements LuaScript {

  private final String luaScript;
  private final String sha1;
  private final ByteBuffer sha1Bytes;

  public LuaScriptData(final String fileName) {

    try (final Stream<String> scriptLines = Files.lines(Paths.get(fileName))) {

      this.luaScript =
          scriptLines.filter(l -> !l.isEmpty() && !l.contains("--"))
              .collect(Collectors.joining(" ")).replaceAll("\\s+", " ");

      this.sha1 = Hashing.sha1().hashString(luaScript, StandardCharsets.UTF_8).toString();
      this.sha1Bytes = ByteBuffer.wrap(sha1.getBytes(StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String getLuaScript() {
    return luaScript;
  }

  @Override
  public String getSha1() {
    return this.sha1;
  }

  @Override
  public ByteBuffer getSha1Bytes() {
    return sha1Bytes;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("sha1", sha1).add("luaScript", luaScript)
        .toString();
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other)
      return true;
    if (!(other instanceof LuaScriptData))
      return false;
    final LuaScriptData castOther = (LuaScriptData) other;
    return Objects.equals(sha1, castOther.sha1);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sha1);
  }
}
