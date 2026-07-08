import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Integration tests talk to a real Redis and shell out to the verify
    // producers (the Go worker sleeps a few seconds), so hooks need headroom.
    testTimeout: 60_000,
    hookTimeout: 90_000,
    // The backends share one Redis DB via distinct key prefixes; running the
    // files serially keeps the seeded state predictable.
    fileParallelism: false,
  },
});
