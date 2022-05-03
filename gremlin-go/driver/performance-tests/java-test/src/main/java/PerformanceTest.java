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

// DO NOT USE - THIS WILL BE DELETED
// This file is intended for generating Performance test results only.
// It is not to be used in a production setting.
// It is being made available as a reference to the test results for
// anyone that wants see the code used to obtain the performance metrics.

import org.apache.tinkerpop.gremlin.driver.Cluster;
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

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class PerformanceTest {
    static final int SAMPLE_SIZE = 21;
    static final int VALUE_MAP_REPEATS = 500;
    static final Cluster cluster = Cluster.build("172.31.24.17").port(45940).maxContentLength(300 * 1024 *1024).create();
    static final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster, "ggrateful");
    static final GraphTraversalSource g = traversal().withRemote(connection);

    public static void main(String[] args) {
        executePerformanceTests();
        System.exit(0);
    }

    public static String[] getArgs(final int repeats) {
        final String[] args = new String[repeats];
        for (int i = 0; i < repeats; i++) {
            args[i] = Integer.toString(i);
        }
        return args;
    }

    public static GraphTraversal<?, ?> getProjectTraversal(final GraphTraversalSource g, final int repeats, final String[] args) {
        GraphTraversal<?, ?> traversal = g.V().project(args[0], Arrays.copyOfRange(args, 1, repeats));
        for (int i = 0; i < repeats; i++) {
            traversal = traversal.by(__.valueMap(true));
        }
        return traversal;
    }

    public static void executePerformanceTests() {
        System.out.println("~~~~~~~ PERFORMANCE TESTS STARTED ~~~~~~~");
        System.out.println("~~~~~~~ RUNNING ONE ITEM PERFORMANCE TEST ~~~~~~~");
        executeGetNextPerformanceTest();
        System.out.println("~~~~~~~ RUNNING LIST PERFORMANCE TEST ~~~~~~~");
        executeGetToListPerformanceTest();
        System.out.println("~~~~~~~ PERFORMANCE TESTS COMPLETE ~~~~~~~");
    }

    public static TimingData executeGetNextPerformanceTest() {
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
        TimingData data = getTimingDataFromDurationList(durations);
        System.out.println(data.toStringMillis("One Item"));
        return data;
    }

    public static TimingData executeGetToListPerformanceTest() {
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
        TimingData data = getTimingDataFromDurationList(durations);
        System.out.println(data.toStringMillis("List"));
        return data;
    }

    private static TimingData getTimingDataFromDurationList(final List<Duration> durations) {
        Collections.sort(durations);
        durations.remove(durations.size()-1);
        return new TimingData(
                durations.stream().reduce(Duration.ZERO, Duration::plus).dividedBy(durations.size()),
                durations.get(durations.size() / 2),
                durations.get((int)(durations.size() * 0.90)),
                durations.get((int)(durations.size() * 0.95)),
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
    final Duration MIN;
    final Duration MAX;

    TimingData(Duration avg, Duration median, Duration percentile_90, Duration percentile_95, Duration min, Duration max) {
        AVG = avg;
        MEDIAN = median;
        PERCENTILE_90 = percentile_90;
        PERCENTILE_95 = percentile_95;
        MIN = min;
        MAX = max;
    }

    public String toStringMillis(final String testType) {
        return "Test Type: " + testType + "\n"+
                "\tAVG=" + AVG.toMillis() + "ms \n" +
                "\tMEDIAN=" + MEDIAN.toMillis() + "ms \n" +
                "\tPERCENTILE_90=" + PERCENTILE_90.toMillis() + "ms \n" +
                "\tPERCENTILE_95=" + PERCENTILE_95.toMillis() + "ms \n" +
                "\tMIN=" + MIN.toMillis() + "ms \n" +
                "\tMAX=" + MAX.toMillis() + "ms \n";
    }

    public String toStringNanos(final String testType) {
        return "Test Type " + testType + "\n"+
                "\tAVG=" + AVG.toNanos() + "ms \n" +
                "\tMEDIAN=" + MEDIAN.toNanos() + "ms \n" +
                "\tPERCENTILE_90=" + PERCENTILE_90.toNanos() + "ms \n" +
                "\tPERCENTILE_95=" + PERCENTILE_95.toNanos() + "ms \n" +
                "\tMIN=" + MIN.toNanos() + "ms \n" +
                "\tMAX=" + MAX.toNanos() + "ms \n";
    }
}
