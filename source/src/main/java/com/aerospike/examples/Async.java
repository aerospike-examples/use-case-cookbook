package com.aerospike.examples;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Async {
    
    public static interface MainThread {
        void run(Async runner);
    }
    
    private final long startTime;
    private final long endTime;
    private final AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newCachedThreadPool();
    
    /**
     * Set up an async execution environment which will persist for the specified duration
     * and then automatically terminat.
     * @param duration
     * @param mainThread
     */
    public static void runFor(Duration duration, MainThread mainThread) {
        new Async(duration).execute(mainThread).waitUntilDone();
    }
    
    private Async(Duration duration) {
        this.startTime = System.currentTimeMillis();
        this.endTime = this.startTime + duration.toMillis();
    }
    
    public long timeRemainingMillis() {
        return Math.max(0, endTime - System.currentTimeMillis());
    }
    
    /**
     * Terminate the run early
     * @return
     */
    public Async terminate() {
        this.shouldTerminate.set(true);
        return this;
    }
    
    public boolean done() {
        return shouldTerminate.get();
    }
    public Random rand() {
        return ThreadLocalRandom.current();
    }
    
    public Async continuous(Runnable runner) {
        return continuous(1, runner);
    }

    public Async continuous(int numberOfCopies, Runnable runner) {
        for (int i = 0; i < numberOfCopies; i++) {
            executor.submit(() -> {
                while (!shouldTerminate.get()) {
                    runner.run();
                }
            });
        }
        return this;
    }

    public Async periodic(Duration period, Runnable runner) {
        return periodic(period, 1, runner);
    }

    public Async periodic(Duration period, int numberOfCopies, Runnable runner) {
        final int periodMs = (int)period.toMillis();
        for (int i = 0; i < numberOfCopies; i++) {
            executor.submit(() -> {
                try {
                    int delayValue = ThreadLocalRandom.current().nextInt(periodMs);
                    if (delayValue > 0) {
                        Thread.sleep(delayValue);
                    }
                    while (!shouldTerminate.get()) {
                        runner.run();
                        Thread.sleep(periodMs);
                    }
                }
                catch (InterruptedException ie) {
                    
                }
            });
        }
        return this;
    }

    private Async execute(MainThread mainThread) {
        mainThread.run(this);
        return this;
    }
    
    private void waitUntilDone() {
        executor.shutdown();
        try {
            executor.awaitTermination(timeRemainingMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
        this.shouldTerminate.set(true);
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        executor.shutdownNow();
        
    }
}
