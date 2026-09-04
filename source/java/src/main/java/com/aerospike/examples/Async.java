package com.aerospike.examples;

import java.time.Duration;
import java.util.Date;
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
    private long timeDivisorNs = -1;
    private long startVirtalTimeNs;
    private long startPhysicalTimeMs;
    
    private final AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newCachedThreadPool();
    
    public static class VirtualTimeBuilder {
        private Duration physicalTime;
        private Duration logicalTime;
        private Duration startOffset;
        private final Async async; 
        
        protected VirtualTimeBuilder(Async async) {
            this.async = async;
        }
        public VirtualTimeBuilder elapse(Duration logicalTime) {
            this.logicalTime = logicalTime;
            return this;
        }
        
        public VirtualTimeBuilder in(Duration phyicalTime) {
            this.physicalTime = phyicalTime;
            return this;
        }
        public VirtualTimeBuilder withPriorOffsetOf(Duration startOffset) {
            this.startOffset = startOffset;
            return this;
        }
        
        public Async startingNow() {
            return async.useVirtualTime(logicalTime, physicalTime, startOffset);
        }
        
    }
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
    
    public VirtualTimeBuilder useVirtualTime() {
        return new VirtualTimeBuilder(this);
    }
    
    protected Async useVirtualTime(Duration logicalTime, Duration physicalTime, Duration startOffset) {
        timeDivisorNs = Math.max(1, logicalTime.toNanos() / physicalTime.toNanos());
        startPhysicalTimeMs = new Date().getTime() - (startOffset != null ? startOffset.toMillis() : 0);
        startVirtalTimeNs = System.nanoTime();
        return this;
    }
    
    public long virtualTime() {
        long now = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis((now - startVirtalTimeNs)* timeDivisorNs) + startPhysicalTimeMs;
    }
    
    public long virtualTimeWithVariance(long minVarianceMs, long maxVarinceMs) {
        long now = virtualTime();
        long offset = ThreadLocalRandom.current().nextLong(minVarianceMs, maxVarinceMs+1);
        return now + offset;
    }
    
    public Date virtualDate() {
        return new Date(virtualTime());
    }
    
    public Date virtualDateWithVariance(long minVarianceMs, long maxVarinceMs) {
        return new Date(virtualTimeWithVariance(minVarianceMs, maxVarinceMs));
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
    
    private int numCopiesToUse(int requestedCopies) {
        if (requestedCopies <= 0) {
            return Math.max(1, Runtime.getRuntime().availableProcessors() + requestedCopies);
        }
        return requestedCopies;
    }

    /**
     * Run a job continuously until the specified time has elapsed.The runner should not loop indefinitely -  
     * it will be called over and over until the time has elapsed. 
     * <p/>
     * Only one thread will be used with this invocation.
     * @param runner
     * @return
     */
    public Async continuous(Runnable runner) {
        return continuous(1, runner);
    }

    /**
     * Run a job continuously until the specified time has elapsed.The runner should not loop indefinitely -  
     * it will be called over and over until the time has elapsed. 
     * <p/>
     * 
     * @param numberOfCopies - The number of threads used to run. Specify 0 for the number of available processors, 
     * -1 for one less than the number of processors, etc.
     * @param runner - The item to execute. This must be thread-safe.
     * @return
     */
    public Async continuous(int numberOfCopies, Runnable runner) {
        numberOfCopies = numCopiesToUse(numberOfCopies);
        for (int i = 0; i < numberOfCopies; i++) {
            executor.submit(() -> {
                try {
                    while (!shouldTerminate.get()) {
                        runner.run();
                    }
                }
                catch (Throwable e) {
                    System.err.printf("Continuously executing thread threw unhandled exception: %s (%s)%n",
                            e.getMessage(), e.getClass());
                    e.printStackTrace();
                }
            });
        }
        return this;
    }

    /**
     * Run a job on an infrequent basis until the specified time has elapsed.The runner should not loop indefinitely -  
     * it will be called over and over until the time has elapsed. 
     * <p/>
     * Only one thread will be used with this invocation.
     * @param runner
     * @return
     */
    public Async periodic(Duration period, Runnable runner) {
        return periodic(period, 1, runner);
    }

    /**
     * Run a job on an infrequent basis until the specified time has elapsed.The runner should not loop indefinitely -  
     * it will be called over and over until the time has elapsed. 
     * <p/>
     * 
     *  @param period - How often the job should run
     * @param numberOfCopies - The number of threads used to run. Specify 0 for the number of available processors, 
     * -1 for one less than the number of processors, etc.
     * @param runner - The item to execute. This must be thread-safe.
     * @return
     */
    public Async periodic(Duration period, int numberOfCopies, Runnable runner) {
        final int periodMs = (int)period.toMillis();
        numberOfCopies = numCopiesToUse(numberOfCopies);
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
                catch (Throwable e) {
                    System.err.printf("Periodic thread threw unhandled exception: %s (%s)%n",
                            e.getMessage(), e.getClass());
                    e.printStackTrace();
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
