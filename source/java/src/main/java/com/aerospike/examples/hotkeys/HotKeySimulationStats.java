package com.aerospike.examples.hotkeys;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe counters for a hot-key simulation phase. Latency is tracked only for successful
 * operations.
 */
public final class HotKeySimulationStats {

    private final AtomicLong attempts = new AtomicLong();
    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong keyBusyErrors = new AtomicLong();
    private final AtomicLong otherErrors = new AtomicLong();
    private final AtomicLong successLatencyNs = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> otherErrorCounts = new ConcurrentHashMap<>();

    public void recordAttempt() {
        attempts.incrementAndGet();
    }

    public void recordSuccess(long latencyNs) {
        successes.incrementAndGet();
        successLatencyNs.addAndGet(latencyNs);
    }

    public void recordKeyBusy() {
        keyBusyErrors.incrementAndGet();
    }

    public void recordOtherError() {
        recordOtherError("Unknown");
    }

    public void recordOtherError(String errorType) {
        otherErrors.incrementAndGet();
        otherErrorCounts.computeIfAbsent(errorType, ignored -> new AtomicLong()).incrementAndGet();
    }

    public long attempts() {
        return attempts.get();
    }

    public long successes() {
        return successes.get();
    }

    public long keyBusyErrors() {
        return keyBusyErrors.get();
    }

    public long otherErrors() {
        return otherErrors.get();
    }

    public double averageSuccessLatencyMs() {
        long count = successes.get();
        if (count == 0) {
            return 0.0;
        }
        return successLatencyNs.get() / (count * 1_000_000.0);
    }

    public HotKeySimulationStats snapshotDelta(HotKeySimulationStats previous) {
        HotKeySimulationStats delta = new HotKeySimulationStats();
        delta.attempts.set(attempts.get() - previous.attempts.get());
        delta.successes.set(successes.get() - previous.successes.get());
        delta.keyBusyErrors.set(keyBusyErrors.get() - previous.keyBusyErrors.get());
        delta.otherErrors.set(otherErrors.get() - previous.otherErrors.get());
        delta.successLatencyNs.set(successLatencyNs.get() - previous.successLatencyNs.get());
        return delta;
    }

    public HotKeySimulationStats copy() {
        HotKeySimulationStats copy = new HotKeySimulationStats();
        copy.attempts.set(attempts.get());
        copy.successes.set(successes.get());
        copy.keyBusyErrors.set(keyBusyErrors.get());
        copy.otherErrors.set(otherErrors.get());
        copy.successLatencyNs.set(successLatencyNs.get());
        return copy;
    }

    @Override
    public String toString() {
        return String.format(
                "attempts=%,d successes=%,d KEY_BUSY=%,d otherErrors=%,d avgSuccessLatency=%.2fms",
                attempts(), successes(), keyBusyErrors(), otherErrors(), averageSuccessLatencyMs());
    }

    public String formatIntervalLine(HotKeySimulationStats interval) {
        double intervalAvgMs = interval.averageSuccessLatencyMs();
        return String.format(
                "  [interval] attempts=%,d successes=%,d KEY_BUSY=%,d otherErrors=%,d avgSuccessLatency=%.2fms",
                interval.attempts(), interval.successes(), interval.keyBusyErrors(),
                interval.otherErrors(), intervalAvgMs);
    }

    public void printOtherErrorBreakdown() {
        if (otherErrors.get() == 0) {
            return;
        }
        System.out.println("  [other errors by type]");
        otherErrorCounts.entrySet().stream()
                .sorted(Map.Entry.<String, AtomicLong>comparingByValue(
                        Comparator.comparingLong(AtomicLong::get)).reversed())
                .forEach(entry -> System.out.printf("    %,d  %s%n",
                        entry.getValue().get(), entry.getKey()));
    }
}
