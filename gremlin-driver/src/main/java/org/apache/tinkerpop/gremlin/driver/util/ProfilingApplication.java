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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProfilingApplication {

    private final Cluster cluster;
    private final int requests;
    private final int clients;
    private final String executionName;

    public ProfilingApplication(final String executionName, final Cluster cluster, final int clients, final int requests) {
        this.executionName = executionName;
        this.cluster = cluster;
        this.clients = clients;
        this.requests = requests;
    }

    public long execute() throws Exception {
        final Map<Thread, Long> rps = new ConcurrentHashMap<>();
        final AtomicInteger tooSlow = new AtomicInteger(0);

        // let all the clients fully init before starting to send messages
        final CyclicBarrier barrier = new CyclicBarrier(clients);

        final List<Thread> threads = IntStream.range(0, clients).mapToObj(t -> new Thread(() -> {
            final Client client = cluster.connect();
            final String executionId = "[" + executionName + "-" + (t + 1) + "]";
            try {
                final CountDownLatch latch = new CountDownLatch(requests);
                client.init();
                barrier.await();

                // timer starts after init of all clients
                final long start = System.nanoTime();
                IntStream.range(0, requests).forEach(i ->
                    client.submitAsync("1+1").thenAcceptAsync(r -> {
                        try {
                            r.all().get(125, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException ex) {
                            tooSlow.incrementAndGet();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    })
                );

                // finish once all requests are accounted for
                latch.await();

                final long end = System.nanoTime();
                final long total = end - start;
                final double totalSeconds = total / 1000000000d;
                final long requestCount = requests;
                final long reqSec = Math.round(requestCount / totalSeconds);
                rps.put(Thread.currentThread(), reqSec);

                System.out.println(String.format(executionId + " clients: %s | requests: %s | time(s): %s | req/sec: %s | too slow: %s", clients, requestCount, totalSeconds, reqSec, tooSlow.get()));
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            } finally {
                if (client != null) client.close();
            }
        }, "benchmark-client-" + executionName + "-" + (t + 1))).collect(Collectors.toList());

        threads.forEach(t -> {
            try {
                t.start();
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        });

        threads.forEach(t -> {
            try {
                t.join();
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        });

        return rps.values().stream().collect(Collectors.averagingLong(l -> l)).longValue();
    }

    public static void main(final String[] args) {
        final Map<String,Object> options = ElementHelper.asMap(args);
        final boolean noExit = Boolean.parseBoolean(options.getOrDefault("noExit", "false").toString());

        try {
            final String host = options.getOrDefault("host", "localhost").toString();
            final int warmups = Integer.parseInt(options.getOrDefault("warmups", "5").toString());
            final int executions = Integer.parseInt(options.getOrDefault("executions", "10").toString());
            final int clients = Integer.parseInt(options.getOrDefault("clients", "1").toString());
            final int requests = Integer.parseInt(options.getOrDefault("requests", "10000").toString());
            final int minConnectionPoolSize = Integer.parseInt(options.getOrDefault("minConnectionPoolSize", "256").toString());
            final int maxConnectionPoolSize = Integer.parseInt(options.getOrDefault("maxConnectionPoolSize", "256").toString());
            final int maxSimultaneousRequestsPerConnection = Integer.parseInt(options.getOrDefault("maxSimultaneousRequestsPerConnection", "32").toString());
            final int maxInProcessPerConnection = Integer.parseInt(options.getOrDefault("maxInProcessPerConnection", "8").toString());
            final int workerPoolSize = Integer.parseInt(options.getOrDefault("workerPoolSize", "4").toString());

            final Cluster cluster = Cluster.build(host)
                    .minConnectionPoolSize(minConnectionPoolSize)
                    .maxConnectionPoolSize(maxConnectionPoolSize)
                    .maxSimultaneousRequestsPerConnection(maxSimultaneousRequestsPerConnection)
                    .maxInProcessPerConnection(maxInProcessPerConnection)
                    .nioPoolSize(clients)
                    .workerPoolSize(workerPoolSize).create();

            final Object fileName = options.get("store");
            final File f = null == fileName ? null : new File(fileName.toString());
            if (f != null && f.length() == 0) {
                try (final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(f, true)))) {
                    writer.println("clients\tminConnectionPoolSize\tmaxConnectionPoolSize\tmaxSimultaneousRequestsPerConnection\tmaxInProcessPerConnection\tworkerPoolSize\trequestPerSecond");
                }
            }

            System.out.println("---------------------------WARMUP CYCLE---------------------------");
            for (int ix = 0; ix < warmups; ix ++) {
                TimeUnit.SECONDS.sleep(1); // pause between executions
                new ProfilingApplication("warmup-" + (ix + 1), cluster, clients, requests).execute();
            }

            System.out.println("----------------------------TEST CYCLE----------------------------");
            long totalRequestsPerSecond = 0;
            for (int ix = 0; ix < executions; ix ++) {
                TimeUnit.SECONDS.sleep(1); // pause between executions
                totalRequestsPerSecond += new ProfilingApplication("test-" + (ix + 1), cluster, clients, requests).execute();
            }

            final int averageRequestPerSecond = Math.round(totalRequestsPerSecond / executions);
            System.out.println(String.format("avg req/sec: %s", averageRequestPerSecond));
            if (f != null) {
                try (final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(f, true)))) {
                    writer.println(String.join("\t", String.valueOf(clients), String.valueOf(minConnectionPoolSize), String.valueOf(maxConnectionPoolSize), String.valueOf(maxSimultaneousRequestsPerConnection), String.valueOf(maxInProcessPerConnection), String.valueOf(workerPoolSize), String.valueOf(averageRequestPerSecond)));
                }
            }

            if (!noExit) System.exit(0);
        } catch (Exception ex) {
            ex.printStackTrace();
            if (!noExit) System.exit(1);
        }
    }
}
