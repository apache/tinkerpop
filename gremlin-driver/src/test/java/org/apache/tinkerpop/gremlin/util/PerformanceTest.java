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

package org.apache.tinkerpop.gremlin.util;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * DO NOT USE - THIS WILL BE DELETED
 * This file is intended for generating Performance test results only.
 * It is not to be used in a production setting.
 * It is being made available as a reference to the test results for
 * anyone that wants see the code used to obtain the performance metrics.
 */
class PerformanceTest {
    private static final int SAMPLE_SIZE = 55;
    private static final int VALUE_MAP_REPEATS = 500;
    private static final String HOST = "localhost";
    private static final int PORT = 45940;
    private static final int POOLING_CONTENT_LENGTH = 25 * 1024 * 1024;
    private static final int MAX_CONTENT_LENGTH = 300 * 1024 * 1024;
    private static final String REMOTE_TRAVERSAL_SOURCE = "ggrateful";
    private static final int POOLING_TRAVERSAL_SIZE = 10;
    private static final List<Integer> POOL_SIZE = new ArrayList<>();
    private static final List<Integer> POOL_QUERY_COUNT = new ArrayList<>();
    static {
        POOL_SIZE.add(1);
        POOL_SIZE.add(2);
        POOL_SIZE.add(4);
        POOL_SIZE.add(8);
        POOL_QUERY_COUNT.add(50);
        POOL_QUERY_COUNT.add(100);
        POOL_QUERY_COUNT.add(250);
        POOL_QUERY_COUNT.add(500);
    }

    private static void main(String[] args) {
        // executePerformanceTests();
        System.exit(0);
    }

    private static DriverRemoteConnection getDriverRemoteConnection(final Integer poolSize) {
        return DriverRemoteConnection.using(getCluster(poolSize), REMOTE_TRAVERSAL_SOURCE);
    }

    private static Cluster getCluster(final Integer poolSize) {

        return (poolSize == null) ? Cluster.build(HOST).port(PORT).maxContentLength(MAX_CONTENT_LENGTH).create()
                :Cluster.build(HOST).port(PORT).maxContentLength(POOLING_CONTENT_LENGTH).maxConnectionPoolSize(poolSize).create();
    }

    private static Client getClient(final Cluster cluster) {
        return cluster.connect().alias(REMOTE_TRAVERSAL_SOURCE);
    }

    private static GraphTraversalSource getGraphTraversalSource(final DriverRemoteConnection connection) {
        return traversal().withRemote(connection);
    }

    private static String[] getArgs(final int repeats) {
        final String[] args = new String[repeats];
        for (int i = 0; i < repeats; i++) {
            args[i] = Integer.toString(i);
        }
        return args;
    }

    private static GraphTraversal<?, ?> getProjectTraversal(final GraphTraversalSource g, final int repeats,
                                                            final String[] args) {
        GraphTraversal<?, ?> traversal = g.V().project(args[0], Arrays.copyOfRange(args, 1, repeats));
        for (int i = 0; i < repeats; i++) {
            traversal = traversal.by(__.valueMap(true));
        }
        return traversal;
    }

    private static void executePerformanceTests() {
        System.out.println("~~~~~~~ PERFORMANCE TESTS STARTED ~~~~~~~");
        System.out.println("~~~~~~~ RUNNING ONE ITEM PERFORMANCE TEST ~~~~~~~");
        executeGetNextPerformanceTest();
        System.out.println("~~~~~~~ RUNNING LIST PERFORMANCE TEST ~~~~~~~");
        executeGetToListPerformanceTest();
        System.out.println("~~~~~~~ RUNNING THROUGHPUT PERFORMANCE TEST ~~~~~~~");
        POOL_QUERY_COUNT.forEach(queryCount -> {
            POOL_SIZE.forEach(poolSize -> {
                System.out.println("~~~ Pool size " + poolSize + ", query count " + queryCount + " ~~~");
                executeThroughputPerformanceTest(poolSize, queryCount);
            });
        });
        System.out.println("~~~~~~~ PERFORMANCE TESTS COMPLETE ~~~~~~~");
    }

    private static void executeThroughputPerformanceTest(final int poolSize, final int queryCount) {
        final DriverRemoteConnection connection = getDriverRemoteConnection(poolSize);
        final GraphTraversalSource g = getGraphTraversalSource(connection);
        final Client client = getClient(getCluster(poolSize));
        final List<Duration> durations = new ArrayList<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            try {
                final List<CompletableFuture<ResultSet>> results = new ArrayList<>();
                final Instant start = Instant.now();
                for (int j = 0; j < queryCount; j++) {
                    final String[] args = getArgs(POOLING_TRAVERSAL_SIZE);
                    results.add(client.submitAsync(getProjectTraversal(g, POOLING_TRAVERSAL_SIZE, args).asAdmin().getBytecode()));
                }
                for (final CompletableFuture<ResultSet> result : results) {
                    result.get().all().get();
                }
                final Instant end = Instant.now();
                durations.add(Duration.between(start, end));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        final TimingData data = getTimingDataFromDurationList(durations);
        System.out.println(data.toThroughput("Pooling", queryCount));
    }

    private static void executeGetNextPerformanceTest() {
        GraphTraversalSource g = getGraphTraversalSource(getDriverRemoteConnection(null));
        final List<Duration> durations = new ArrayList<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            try {
                final String[] args = getArgs(VALUE_MAP_REPEATS);
                final Instant starts = Instant.now();
                // execute and retrieve(timed)
                getProjectTraversal(g, VALUE_MAP_REPEATS, args).next();
                final Instant ends = Instant.now();
                durations.add(Duration.between(starts, ends));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        final TimingData data = getTimingDataFromDurationList(durations);
        System.out.println(data.toStringMillis("One Item"));
    }

    private static void executeGetToListPerformanceTest() {
        final GraphTraversalSource g = getGraphTraversalSource(getDriverRemoteConnection(null));
        final List<Duration> durations = new ArrayList<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            try {
                final String[] args = getArgs(VALUE_MAP_REPEATS);
                final Instant starts = Instant.now();
                // execute and retrieve(timed)
                getProjectTraversal(g, VALUE_MAP_REPEATS, args).toList();
                final Instant ends = Instant.now();
                durations.add(Duration.between(starts, ends));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        final TimingData data = getTimingDataFromDurationList(durations);
        System.out.println(data.toStringMillis("List"));
    }

    private static TimingData getTimingDataFromDurationList(final List<Duration> durations) {
        Collections.sort(durations);
        for (int i = 0; i < 5; i++) {
            durations.remove(durations.size() - 1);
        }
        return new TimingData(
                durations.stream().reduce(Duration.ZERO, Duration::plus).dividedBy(durations.size()),
                durations.get(durations.size() / 2),
                durations.get((int) (durations.size() * 0.90)),
                durations.get((int) (durations.size() * 0.95)),
                durations.get((int) (durations.size() * 0.10)),
                durations.get((int) (durations.size() * 0.05)),
                durations.get(0),
                durations.get(durations.size() - 1)
        );
    }
}


class TimingData {
    final Duration AVG;
    final Duration MEDIAN;
    final Duration PERCENTILE_90;
    final Duration PERCENTILE_95;
    final Duration PERCENTILE_10;
    final Duration PERCENTILE_5;
    final Duration MIN;
    final Duration MAX;

    TimingData(Duration avg, Duration median, Duration percentile_90, Duration percentile_95, Duration percentile_10, Duration percentile_5, Duration min,
               Duration max) {
        AVG = avg;
        MEDIAN = median;
        PERCENTILE_90 = percentile_90;
        PERCENTILE_95 = percentile_95;
        PERCENTILE_10 = percentile_10;
        PERCENTILE_5 = percentile_5;
        MIN = min;
        MAX = max;
    }

    public String toStringMillis(final String testType) {
        return "Test Type: " + testType + "\n" +
                "\tAVG=" + AVG.toMillis() + "ms \n" +
                "\tPERCENTILE_5=" + PERCENTILE_5.toMillis() + "ms \n" +
                "\tPERCENTILE_10=" + PERCENTILE_10.toMillis() + "ms \n" +
                "\tMEDIAN=" + MEDIAN.toMillis() + "ms \n" +
                "\tPERCENTILE_90=" + PERCENTILE_90.toMillis() + "ms \n" +
                "\tPERCENTILE_95=" + PERCENTILE_95.toMillis() + "ms \n" +
                "\tMIN=" + MIN.toMillis() + "ms \n" +
                "\tMAX=" + MAX.toMillis() + "ms \n";
    }

    public String toThroughput(final String testType, int queryCount) {
        return "Test Type: " + testType + "\n" +
                "\tAVG=" + ((1000L * queryCount) / AVG.toMillis()) + " query/s \n" +
                "\tPERCENTILE_5=" + ((1000L * queryCount) / PERCENTILE_95.toMillis()) + " query/s \n" +
                "\tPERCENTILE_10=" + ((1000L * queryCount) / PERCENTILE_90.toMillis()) + " query/s \n" +
                "\tMEDIAN=" + ((1000L * queryCount) / MEDIAN.toMillis()) + " query/s \n" +
                "\tPERCENTILE_90=" + ((1000L * queryCount) / PERCENTILE_10.toMillis()) + " query/s \n" +
                "\tPERCENTILE_95=" + ((1000L * queryCount) / PERCENTILE_5.toMillis()) + " query/s \n" +
                "\tMIN=" + ((1000L * queryCount) / MAX.toMillis()) + " query/s \n" +
                "\tMAX=" + ((1000L * queryCount) / MIN.toMillis()) + " query/s \n";
    }
}
