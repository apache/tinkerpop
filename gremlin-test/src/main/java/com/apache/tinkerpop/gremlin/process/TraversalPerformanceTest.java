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
package com.apache.tinkerpop.gremlin.process;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.apache.tinkerpop.gremlin.AbstractGremlinTest;
import com.apache.tinkerpop.gremlin.LoadGraphWith;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static com.apache.tinkerpop.gremlin.process.graph.traversal.__.out;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "gremlin-traversal")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-gremlin-traversal")
public class TraversalPerformanceTest extends AbstractGremlinTest {

    public final static int DEFAULT_BENCHMARK_ROUNDS = 25;
    public final static int DEFAULT_WARMUP_ROUNDS = 5;

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void g_V_outE_inV_outE_inV_outE_inV() throws Exception {
        g.V().outE().inV().outE().inV().outE().inV().iterate();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void g_V_out_out_out() throws Exception {
        g.V().out().out().out().iterate();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void g_V_out_out_out_path() throws Exception {
        g.V().out().out().out().path().iterate();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void g_V_repeatXoutX_timesX2X() throws Exception {
        g.V().repeat(out()).times(2).iterate();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void g_V_repeatXoutX_timesX3X() throws Exception {
        g.V().repeat(out()).times(3).iterate();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void g_V_localXout_out_valuesXnameX_foldX() throws Exception {
        g.V().local(out().out().values("name").fold()).iterate();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void g_V_out_localXout_out_valuesXnameX_foldX() throws Exception {
        g.V().out().local(out().out().values("name").fold()).iterate();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void g_V_out_mapXout_out_valuesXnameX_toListX() throws Exception {
        g.V().out().map(v -> v.get().out().out().values("name").toList()).iterate();
    }
}
