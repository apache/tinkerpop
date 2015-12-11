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
package org.apache.tinkerpop.gremlin.server;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Execute a simple script (1+1).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "gremlin-addition")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-gremlin-addition")
public class GremlinAdditionPerformanceTest extends AbstractGremlinServerPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(GremlinAdditionPerformanceTest.class);

    public final static int DEFAULT_BENCHMARK_ROUNDS = 50;
    public final static int DEFAULT_WARMUP_ROUNDS = 5;

    public final static int DEFAULT_CONCURRENT_BENCHMARK_ROUNDS = 500;
    public final static int DEFAULT_CONCURRENT_WARMUP_ROUNDS = 10;

    private final static Cluster cluster = Cluster.build("localhost").create();
    private final static Random rand = new Random();

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @Test
    public void webSocketsGremlin() throws Exception {
        tryWebSocketGremlin();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_CONCURRENT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_CONCURRENT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_AVAILABLE_CORES)
    @Test
    public void webSocketsGremlinConcurrent() throws Exception {
        tryWebSocketGremlin();
    }

    @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 1, concurrency = BenchmarkOptions.CONCURRENCY_AVAILABLE_CORES)
    @Test
    public void webSocketsGremlinConcurrentAlternateSerialization() throws Exception {
        final Serializers[] mimes = new Serializers[]{Serializers.GRAPHSON, Serializers.GRAPHSON_V1D0, Serializers.GRYO_V1D0};
        final Serializers mimeType = mimes[rand.nextInt(3)];
        logger.info(mimeType.toString());
        final Cluster cluster = Cluster.build("localhost")
                .serializer(mimeType)
                .create();
        final Client client = cluster.connect();
        assertEquals("2", client.submit("1+1").stream().map(Result::getString).findAny().orElse("invalid"));
    }

    @BeforeClass
    public static void before() {
        // good to call init here ahead of performance tracking
        cluster.init();
    }

    @AfterClass
    public static void after() {
        cluster.close();
    }

    private void tryWebSocketGremlin() throws Exception {
        final Client client = cluster.connect();
        assertEquals("2", client.submit("1+1").stream().map(Result::getString).findAny().orElse("invalid"));
    }
}
