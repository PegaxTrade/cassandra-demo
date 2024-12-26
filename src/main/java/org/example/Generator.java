package org.example;

import org.apache.commons.text.RandomStringGenerator;
import org.jetbrains.annotations.NotNull;

public class Generator {
  private static final RandomStringGenerator randomStringGenerator;

  static {
    final char[][] pairs = {{ 'a', 'z' }, { 'A', 'Z' }, { '0', '9' }};

    randomStringGenerator = new RandomStringGenerator.Builder()
        .withinRange(pairs)
        .get();
  }

  private Generator() {
  }

  public static @NotNull String randomString(final int length) {
    return randomStringGenerator.generate(length);
  }
}
