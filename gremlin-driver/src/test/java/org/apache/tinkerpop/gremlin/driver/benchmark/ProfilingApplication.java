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
package org.apache.tinkerpop.gremlin.driver.benchmark;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProfilingApplication {
    public static void main(final String[] args) {
        try {
            System.out.println("Initializing at: " + System.nanoTime());

            final String host = args.length == 0 ? "localhost" : args[0];

            final int clients = 1;
            final int requests = 10000;
            final Cluster cluster = Cluster.build(host)
                    .minConnectionPoolSize(256)
                    .maxConnectionPoolSize(256)
                    .nioPoolSize(clients)
                    .workerPoolSize(clients * 2).create();

            final AtomicInteger tooSlow = new AtomicInteger(0);

            // let all the clients fully init before starting to send messages
            final CyclicBarrier barrier = new CyclicBarrier(clients);

            final List<Thread> threads = IntStream.range(0, clients).mapToObj(t -> new Thread(() -> {
                try {
                    final CountDownLatch latch = new CountDownLatch(requests);

                    final Client client = cluster.connect();
                    client.init();

                    barrier.await();
                    final long start = System.nanoTime();

                    System.out.println("Executing at [" + t + "]:" + start);

                    IntStream.range(0, requests).forEach(i -> {
                        client.submitAsync("1+1").thenAcceptAsync(r -> {
                            try {
                                r.all().get(100, TimeUnit.MILLISECONDS);
                            } catch (TimeoutException ex) {
                                tooSlow.incrementAndGet();
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            } finally {
                                latch.countDown();
                            }
                        });
                    });

                    latch.await();

                    final long end = System.nanoTime();
                    final long total = end - start;

                    System.out.println("All responses for [" + t + "] are accounted for at: " + end);

                    final long totalSeconds = Math.round(total / 1000000000d);
                    final long requestCount = requests;
                    final long reqSec = Math.round(requestCount / totalSeconds);
                    System.out.println(String.format("[" + t + "] clients: %s | requests: %s | time(s): %s | req/sec: %s | too slow: %s", clients, requestCount, totalSeconds, reqSec, tooSlow.get()));
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw new RuntimeException(ex);
                }
            })).collect(Collectors.toList());

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

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
