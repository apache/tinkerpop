/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * An internal application used to test out configuration parameters for Gremlin Driver.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProfilingApplication {

    private static final Random random = new Random(0); // Same seed to ensure consistent test runs.
    private static final String[] scripts = new String[]{
            "g.V()",
            "g.V(1).out('knows')",
            "g.V(1).out('knows').has('name','josh')",
            "g.V(1).as(\"a\").out(\"knows\").as(\"b\").select(\"a\", \"b\")",
            "g.V(1).as(\"a\").out(\"knows\").as(\"b\").select(\"a\", \"b\").by(\"name\")",
            "g.V().hasLabel(\"person\").as(\"p\").map(__.bothE().label().groupCount()).as(\"r\").select(\"p\", \"r\")",
            "g.V().choose(__.outE().count().is(0L), __.as(\"a\"), __.as(\"b\")).choose(__.select(\"a\"), __.select(\"a\"), __.select(\"b\"))",
            "g.V().group(\"a\").by(T.label).by(outE().values(\"weight\").sum()).cap(\"a\")",
            "g.V().repeat(__.union(__.out(\"knows\").group(\"a\").by(\"age\"), __.out(\"created\").group(\"b\").by(\"name\").by(count())).group(\"a\").by(\"name\")).times(2).cap(\"a\", \"b\")",
            "g.V().match(\n" +
            "  as(\"a\").out(\"knows\").as(\"b\"),\n" +
            "  as(\"b\").out(\"created\").has(\"name\", \"lop\"),\n" +
            "  as(\"b\").match(\n" +
            "  as(\"b\").out(\"created\").as(\"d\"),\n" +
            "  as(\"d\").in(\"created\").as(\"c\")).select(\"c\").as(\"c\")).<Vertex>select(\"a\", \"b\", \"c\")"
    };

    private final Cluster cluster;
    private final int requests;
    private final String executionName;
    private final String script;
    private final int tooSlowThreshold;
    private final boolean exercise;

    private final boolean suppressStackTraces;

    private final ExecutorService executor;

    public ProfilingApplication(final String executionName, final Cluster cluster, final int requests, final ExecutorService executor,
                                final String script, final int tooSlowThreshold, final boolean exercise, final boolean suppressStackTraces) {
        this.executionName = executionName;
        this.cluster = cluster;
        this.requests = requests;
        this.executor = executor;
        this.script = script;
        this.tooSlowThreshold = tooSlowThreshold;
        this.exercise = exercise;
        this.suppressStackTraces = suppressStackTraces;
    }

    public long executeThroughput() throws Exception {
        final AtomicInteger tooSlow = new AtomicInteger(0);
        final AtomicInteger errors = new AtomicInteger(0);

        final Client client = cluster.connect();
        final String executionId = "[" + executionName + "]";
        try {
            final CountDownLatch latch = new CountDownLatch(requests);
            client.init();

            final long start = System.nanoTime();
            IntStream.range(0, requests).forEach(i -> {
                final String s = exercise ? chooseScript() : script;
                client.submitAsync(s).thenAcceptAsync(r -> {
                    try {
                        r.all().get(tooSlowThreshold, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException ex) {
                        tooSlow.incrementAndGet();
                    } catch (Exception ex) {
                        errors.incrementAndGet();
                        if (!suppressStackTraces) ex.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }, executor);
            });

            // finish once all requests are accounted for
            latch.await();

            final long end = System.nanoTime();
            final long total = end - start;
            final double totalSeconds = total / 1000000000d;
            final long requestCount = requests;
            final long reqSec = Math.round(requestCount / totalSeconds);
            System.out.println(String.format(StringUtils.rightPad(executionId, 10) + " requests: %s | time(s): %s | req/sec: %s | too slow: %s | errors: %s",
                    requestCount,
                    StringUtils.rightPad(String.valueOf(totalSeconds), 14),
                    StringUtils.rightPad(String.valueOf(reqSec), 7), exercise ? "N/A" : tooSlow.get(),
                    errors.get()));
            return reqSec;
        } catch (Exception ex) {
            // catch all so that it doesn't tank the whole execution. returning 0 will hose up calculations on
            // averages, but presumably you'd see the failed executions and be suspicious.
            System.out.println("Failed Execution: " + executionName + " - " + ex.getMessage());
            return 0;
        } finally {
            client.close();
        }
    }

    public double executeLatency() throws Exception {
        final Client client = cluster.connect();
        final String executionId = "[" + executionName + "]";
        try {
            client.init();

            final long start = System.nanoTime();
            int size = 0;
            final Iterator itr = client.submitAsync(script).get().iterator();
            try {
                while (true) {
                    itr.next();
                    size++;
                }
            } catch (NoSuchElementException nsee) {
                ; // Expected as hasNext() not called to increase performance.
            }
            final long end = System.nanoTime();
            final long total = (end - start);

            final double totalSeconds = total / 1000000000d;
            System.out.println(String.format(StringUtils.rightPad(executionId, 10) + "time: %s, result count: %s", StringUtils.rightPad(String.valueOf(totalSeconds), 7), StringUtils.rightPad(String.valueOf(size), 10)));
            return totalSeconds;
        } catch (Exception ex) {
            if (!suppressStackTraces) ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            client.close();
        }
    }

    private String chooseScript() {
        return scripts[random.nextInt(scripts.length - 1)];
    }

    public enum TestType { LATENCY, THROUGHPUT };

    public static void main(final String[] args) {
        final Map<String,Object> options = ElementHelper.asMap(args);
        final boolean noExit = Boolean.parseBoolean(options.getOrDefault("noExit", "false").toString());
        final int parallelism = Integer.parseInt(options.getOrDefault("parallelism", "16").toString());
        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("profiler-%d").build();
        final ExecutorService executor = Executors.newFixedThreadPool(parallelism, threadFactory);
        final TestType testType = TestType.values()[(Integer.parseInt(options.getOrDefault("testType", "1").toString()) % TestType.values().length)];

        final String host = options.getOrDefault("host", "localhost").toString();
        final int minExpectedRps = Integer.parseInt(options.getOrDefault("minExpectedRps", "1000").toString());
        final int timeout = Integer.parseInt(options.getOrDefault("timeout", "1200000").toString());
        final int warmups = Integer.parseInt(options.getOrDefault("warmups", "5").toString());
        final int executions = Integer.parseInt(options.getOrDefault("executions", "10").toString());
        final int nioPoolSize = Integer.parseInt(options.getOrDefault("nioPoolSize", "1").toString());
        final int requests = Integer.parseInt(options.getOrDefault("requests", "10000").toString());
        final int maxConnectionPoolSize = Integer.parseInt(options.getOrDefault("maxConnectionPoolSize", "256").toString());
        final int maxWaitForConnection = Integer.parseInt(options.getOrDefault("maxWaitForConnection", "3000").toString());
        final int workerPoolSize = Integer.parseInt(options.getOrDefault("workerPoolSize", Runtime.getRuntime().availableProcessors() * 2).toString());
        final int tooSlowThreshold = Integer.parseInt(options.getOrDefault("tooSlowThreshold", "125").toString());
        final String serializer = options.getOrDefault("serializer", Serializers.GRAPHBINARY_V4.name()).toString();
        final int pauseBetweenRuns = Integer.parseInt(options.getOrDefault("pauseBetweenRuns", "1000").toString());
        final boolean suppressStackTraces = Boolean.parseBoolean(options.getOrDefault("suppressStackTraces", "false").toString());

        final boolean exercise = Boolean.parseBoolean(options.getOrDefault("exercise", "false").toString());
        final String script = options.getOrDefault("script", "1+1").toString();

        final Cluster cluster = Cluster.build(host)
                .maxConnectionPoolSize(maxConnectionPoolSize)
                .nioPoolSize(nioPoolSize)
                .maxWaitForConnection(maxWaitForConnection)
                .serializer(Serializers.valueOf(serializer))
                .workerPoolSize(workerPoolSize).create();

        try {
            if (TestType.LATENCY == testType) {
                System.out.println("-----------------------LATENCY TEST SELECTED----------------------");
            } else {
                System.out.println("---------------------THROUGHPUT TEST SELECTED---------------------");
            }

            if (exercise) {
                System.out.println("--------------------------INITIALIZATION--------------------------");
                final Client client = cluster.connect();
                client.submit("graph.clear()").all().join();
                System.out.println("Cleared existing 'graph'");

                client.submit("TinkerFactory.generateModern(graph)").all().join();
                client.close();

                System.out.println("Modern graph loaded");
            }

            if (TestType.THROUGHPUT == testType) {
                final Object fileName = options.get("store");
                final File f = null == fileName ? null : new File(fileName.toString());
                if (f != null && f.length() == 0) {
                    try (final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(f, true)))) {
                        writer.println("parallelism\tnioPoolSize\tmaxConnectionPoolSize\tworkerPoolSize\trequestPerSecond");
                    }
                }

                // not much point to continuing with a line of tests if we can't get at least minExpectedRps.
                final AtomicBoolean meetsRpsExpectation = new AtomicBoolean(true);
                System.out.println("---------------------------WARMUP CYCLE---------------------------");
                for (int ix = 0; ix < warmups && meetsRpsExpectation.get(); ix++) {
                    final long averageRequestsPerSecond = new ProfilingApplication("warmup-" + (ix + 1), cluster, 1000, executor, script, tooSlowThreshold, exercise, suppressStackTraces).executeThroughput();
                    meetsRpsExpectation.set(averageRequestsPerSecond >= minExpectedRps);
                    TimeUnit.MILLISECONDS.sleep(pauseBetweenRuns); // pause between executions
                }

                final AtomicBoolean exceededTimeout = new AtomicBoolean(false);
                long totalRequestsPerSecond = 0;

                // no need to execute this if we didn't pass the basic expectation in the warmups
                if (exercise || meetsRpsExpectation.get()) {
                    final long start = System.nanoTime();
                    System.out.println("----------------------------TEST CYCLE----------------------------");
                    for (int ix = 0; ix < executions && !exceededTimeout.get(); ix++) {
                        totalRequestsPerSecond += new ProfilingApplication("test-" + (ix + 1), cluster, requests, executor, script, tooSlowThreshold, exercise, suppressStackTraces).executeThroughput();
                        exceededTimeout.set((System.nanoTime() - start) > TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS));
                        TimeUnit.MILLISECONDS.sleep(pauseBetweenRuns); // pause between executions
                    }
                }

                final int averageRequestPerSecond = !meetsRpsExpectation.get() || exceededTimeout.get() ? 0 : Math.round(totalRequestsPerSecond / executions);
                System.out.println(String.format("avg req/sec: %s", averageRequestPerSecond));
                if (f != null) {
                    try (final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(f, true)))) {
                        writer.println(String.join("\t", String.valueOf(parallelism), String.valueOf(nioPoolSize), String.valueOf(maxConnectionPoolSize), String.valueOf(workerPoolSize), String.valueOf(averageRequestPerSecond)));
                    }
                }
            } else if (TestType.LATENCY == testType) {
                final AtomicBoolean meetsTimeoutExpectation = new AtomicBoolean(true);
                System.out.println("---------------------------WARMUP CYCLE---------------------------");
                for (int ix = 0; ix < warmups && meetsTimeoutExpectation.get(); ix++) {
                    final double latency = new ProfilingApplication("warmup-" + (ix + 1), cluster, 1000, executor, script, tooSlowThreshold, exercise, suppressStackTraces).executeLatency();
                    meetsTimeoutExpectation.set(latency < timeout);
                    TimeUnit.MILLISECONDS.sleep(pauseBetweenRuns); // pause between executions
                }

                final AtomicBoolean exceededTimeout = new AtomicBoolean(false);
                double totalTime = 0;

                // no need to execute this if we didn't pass the basic expectation in the warmups
                if (exercise || meetsTimeoutExpectation.get()) {
                    final long start = System.nanoTime();
                    System.out.println("----------------------------TEST CYCLE----------------------------");
                    for (int ix = 0; ix < executions && !exceededTimeout.get(); ix++) {
                        totalTime += new ProfilingApplication("test-" + (ix + 1), cluster, requests, executor, script, tooSlowThreshold, exercise, suppressStackTraces).executeLatency();
                        exceededTimeout.set((System.nanoTime() - start) > TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS));
                        TimeUnit.MILLISECONDS.sleep(pauseBetweenRuns); // pause between executions
                    }
                }

                final double averageLatency = !meetsTimeoutExpectation.get() || exceededTimeout.get() ? 0 : (totalTime / executions);
                System.out.println(String.format("avg latency (sec/req): %s", averageLatency));
            } else {
                System.out.println("Encountered unknown testType. Please enter a valid value and try again.");
            }

            if (!noExit) System.exit(0);
        } catch (Exception ex) {
            ex.printStackTrace();
            if (!noExit) System.exit(1);
        } finally {
            executor.shutdown();
            cluster.close();
        }
    }
}
