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
import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
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
 * An internal application used to test out configuration parameters for Gremlin Driver against Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProfilingApplication {

    private static final Random random = new Random();
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

    private final ExecutorService executor;

    public ProfilingApplication(final String executionName, final Cluster cluster, final int requests, final ExecutorService executor,
                                final String script, final int tooSlowThreshold, final boolean exercise) {
        this.executionName = executionName;
        this.cluster = cluster;
        this.requests = requests;
        this.executor = executor;
        this.script = script;
        this.tooSlowThreshold = tooSlowThreshold;
        this.exercise = exercise;
    }

    public long execute() throws Exception {
        final AtomicInteger tooSlow = new AtomicInteger(0);

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
                        ex.printStackTrace();
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
            System.out.println(String.format(StringUtils.rightPad(executionId, 10) + " requests: %s | time(s): %s | req/sec: %s | too slow: %s", requestCount, StringUtils.rightPad(String.valueOf(totalSeconds), 14), StringUtils.rightPad(String.valueOf(reqSec), 7), exercise ? "N/A" : tooSlow.get()));
            return reqSec;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            client.close();
        }
    }

    private String chooseScript() {
        return scripts[random.nextInt(scripts.length - 1)];
    }

    public static void main(final String[] args) {
        final Map<String,Object> options = ElementHelper.asMap(args);
        final boolean noExit = Boolean.parseBoolean(options.getOrDefault("noExit", "false").toString());
        final int parallelism = Integer.parseInt(options.getOrDefault("parallelism", "16").toString());
        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("profiler-%d").build();
        final ExecutorService executor = Executors.newFixedThreadPool(parallelism, threadFactory);

        final String host = options.getOrDefault("host", "localhost").toString();
        final int minExpectedRps = Integer.parseInt(options.getOrDefault("minExpectedRps", "200").toString());
        final int timeout = Integer.parseInt(options.getOrDefault("timeout", "1200000").toString());
        final int warmups = Integer.parseInt(options.getOrDefault("warmups", "5").toString());
        final int executions = Integer.parseInt(options.getOrDefault("executions", "10").toString());
        final int nioPoolSize = Integer.parseInt(options.getOrDefault("nioPoolSize", "1").toString());
        final int requests = Integer.parseInt(options.getOrDefault("requests", "10000").toString());
        final int maxConnectionPoolSize = Integer.parseInt(options.getOrDefault("maxConnectionPoolSize", "256").toString());
        final int maxWaitForConnection = Integer.parseInt(options.getOrDefault("maxWaitForConnection", "3000").toString());
        final int workerPoolSize = Integer.parseInt(options.getOrDefault("workerPoolSize", "2").toString());
        final int tooSlowThreshold = Integer.parseInt(options.getOrDefault("tooSlowThreshold", "125").toString());
        final String channelizer = options.getOrDefault("channelizer", Channelizer.WebSocketChannelizer.class.getName()).toString();
        final String serializer = options.getOrDefault("serializer", Serializers.GRAPHBINARY_V1D0.name()).toString();

        final boolean exercise = Boolean.parseBoolean(options.getOrDefault("exercise", "false").toString());
        final String script = options.getOrDefault("script", "1+1").toString();

        final Cluster cluster = Cluster.build(host)
                .maxConnectionPoolSize(maxConnectionPoolSize)
                .nioPoolSize(nioPoolSize)
                .channelizer(channelizer)
                .maxWaitForConnection(maxWaitForConnection)
                .serializer(Serializers.valueOf(serializer))
                .workerPoolSize(workerPoolSize).create();

        try {
            if (exercise) {
                System.out.println("--------------------------INITIALIZATION--------------------------");
                final Client client = cluster.connect();
                client.submit("graph.clear()").all().join();
                System.out.println("Cleared existing 'graph'");

                client.submit("TinkerFactory.generateModern(graph)").all().join();
                client.close();

                System.out.println("Modern graph loaded");
            }

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
                final long averageRequestsPerSecond = new ProfilingApplication("warmup-" + (ix + 1), cluster, 1000, executor, script, tooSlowThreshold, exercise).execute();

                // check rps on last warmup round. by then pools should be initialized fully
                if (ix + 1 == warmups)
                    meetsRpsExpectation.set(averageRequestsPerSecond > minExpectedRps);

                if (!meetsRpsExpectation.get()) {
                    System.out.println("Failed to meet minimum RPS at " + averageRequestsPerSecond);
                }

                TimeUnit.SECONDS.sleep(1); // pause between executions
            }

            final AtomicBoolean exceededTimeout = new AtomicBoolean(false);
            long totalRequestsPerSecond = 0;

            // no need to execute this if we didn't pass the basic expectation in the warmups
            if (exercise || meetsRpsExpectation.get()) {
                final long start = System.nanoTime();
                System.out.println("----------------------------TEST CYCLE----------------------------");
                for (int ix = 0; ix < executions && !exceededTimeout.get(); ix++) {
                    totalRequestsPerSecond += new ProfilingApplication("test-" + (ix + 1), cluster, requests, executor, script, tooSlowThreshold, exercise).execute();
                    exceededTimeout.set((System.nanoTime() - start) > TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS));
                    TimeUnit.SECONDS.sleep(1); // pause between executions
                }
            }

            final int averageRequestPerSecond = !meetsRpsExpectation.get() || exceededTimeout.get() ? 0 : Math.round(totalRequestsPerSecond / executions);
            System.out.println(String.format("avg req/sec: %s", averageRequestPerSecond));
            if (f != null) {
                try (final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(f, true)))) {
                    writer.println(String.join("\t", String.valueOf(parallelism), String.valueOf(nioPoolSize), String.valueOf(maxConnectionPoolSize), String.valueOf(workerPoolSize), String.valueOf(averageRequestPerSecond)));
                }
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
