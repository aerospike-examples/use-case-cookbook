package com.aerospike.examples.hotkeys;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ResultCode;

/**
 * Runs a multi-threaded load phase against a single logical hot key, printing cumulative and
 * per-second statistics until the configured duration elapses.
 */
public final class HotKeySimulation {

    @FunctionalInterface
    public interface OperationAttempt {
        void execute() throws Exception;
    }

    /**
     * Executes {@code numThreads} worker threads for {@code durationSecs}, invoking
     * {@code attempt} as fast as possible on each thread.
     */
    public HotKeySimulationStats run(String phaseLabel, int numThreads, int durationSecs,
            OperationAttempt attempt) throws Exception {
        return run(phaseLabel, numThreads, durationSecs, attempt, null, 0);
    }

    /**
     * Same as {@link #run(String, int, int, OperationAttempt)} but also runs {@code periodicSideTask}
     * on a background thread every {@code periodicIntervalMs} milliseconds. Side-task failures are
     * not counted in the main simulation statistics.
     */
    public HotKeySimulationStats run(String phaseLabel, int numThreads, int durationSecs,
            OperationAttempt attempt, OperationAttempt periodicSideTask, long periodicIntervalMs) throws Exception {
        System.out.println();
        System.out.println("=== " + phaseLabel + " ===");
        System.out.printf("Threads: %,d  Duration: %,ds%n", numThreads, durationSecs);
        if (periodicSideTask != null && periodicIntervalMs > 0) {
            System.out.printf("Periodic side operation every %,dms%n", periodicIntervalMs);
        }

        HotKeySimulationStats totals = new HotKeySimulationStats();
        HotKeySimulationStats previousTotals = new HotKeySimulationStats();
        long endTimeMs = System.currentTimeMillis() + durationSecs * 1000L;
        AtomicBoolean running = new AtomicBoolean(true);

        ScheduledExecutorService sideOpScheduler = startPeriodicSideTask(
                periodicSideTask, periodicIntervalMs, running);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(() -> {
                while (running.get() && System.currentTimeMillis() < endTimeMs) {
                    totals.recordAttempt();
                    long startNs = System.nanoTime();
                    try {
                        attempt.execute();
                        totals.recordSuccess(System.nanoTime() - startNs);
                    } catch (AerospikeException.KeyBusyException e) {
                        totals.recordKeyBusy();
                    } catch (Exception e) {
                        totals.recordOtherError(describeError(e));
                    }
                }
            }));
        }

        long nextReportMs = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < endTimeMs) {
            Thread.sleep(Math.max(1, nextReportMs - System.currentTimeMillis()));
            HotKeySimulationStats interval = totals.snapshotDelta(previousTotals);
            System.out.println(interval.formatIntervalLine(interval));
            previousTotals = totals.copy();
            nextReportMs += 1000;
        }

        running.set(false);
        stopPeriodicSideTask(sideOpScheduler);
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        for (Future<?> future : futures) {
            future.get();
        }

        System.out.println("  [total]   " + totals);
        totals.printOtherErrorBreakdown();
        return totals;
    }

    private static String describeError(Exception e) {
        if (e instanceof AerospikeException) {
            AerospikeException ae = (AerospikeException) e;
            return String.format("AerospikeException code=%d (%s)",
                    ae.getResultCode(), ResultCode.getResultString(ae.getResultCode()));
        }
        return e.getClass().getSimpleName();
    }

    private ScheduledExecutorService startPeriodicSideTask(OperationAttempt periodicSideTask,
            long periodicIntervalMs, AtomicBoolean running) {
        if (periodicSideTask == null || periodicIntervalMs <= 0) {
            return null;
        }
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "hotkey-periodic-side-op");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleAtFixedRate(() -> {
            if (!running.get()) {
                return;
            }
            try {
                periodicSideTask.execute();
            } catch (Exception ignored) {
                // Side operations are background traffic; main stats stay focused on hot-path load.
            }
        }, periodicIntervalMs, periodicIntervalMs, TimeUnit.MILLISECONDS);
        return scheduler;
    }

    private void stopPeriodicSideTask(ScheduledExecutorService scheduler) throws InterruptedException {
        if (scheduler == null) {
            return;
        }
        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);
    }
}
