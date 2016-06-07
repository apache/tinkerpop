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
package org.apache.tinkerpop.gremlin.groovy.engine;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.1, replaced by gremlin-benchmark.
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "gremlin-executor")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-gremlin-executor")
@Deprecated
public class GremlinExecutorPerformanceTest extends AbstractGremlinTest {

    private static final Random rand = new Random(9585834534l);
    private static final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();
    private GremlinGenerator generator;
    private Graph syntaxGraph = null;
    private Configuration syntaxGraphConfig = null;

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @Rule
    public TestName testName = new TestName();

    public final static int DEFAULT_BENCHMARK_ROUNDS = 500;
    public final static int DEFAULT_WARMUP_ROUNDS = 10;

    @Override
    public void setup() throws Exception {
        super.setup();
        syntaxGraphConfig = graphProvider.newGraphConfiguration("gremlin-executor-test",
                GremlinExecutorPerformanceTest.class, testName.getMethodName(), null);
        syntaxGraph = graphProvider.openTestGraph(syntaxGraphConfig);
        generator = new GremlinGenerator(syntaxGraph, rand);
    }

    @Override
    public void tearDown() throws Exception {
        if (syntaxGraph != null) graphProvider.clear(syntaxGraph, syntaxGraphConfig);
        super.tearDown();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void executorEval() throws Exception {
        final Map<String, Object> params = new HashMap<>();
        params.put("g", g);

        final String traversal = generator.generateGremlin();
        final int resultsToNextOut = rand.nextInt(512) + 1;
        final String nextedTraversal = traversal + ".next(" + resultsToNextOut + ")";
        final CompletableFuture<Object> future1 = gremlinExecutor.eval(nextedTraversal, params);
        future1.join();
    }

    public static class GremlinGenerator {
        private final Random rand;

        private final Graph syntaxGraph;

        public GremlinGenerator(final Graph syntaxGraph, final Random rand) {
            this.rand = rand;
            this.syntaxGraph = syntaxGraph;
            loadGraph(this.syntaxGraph);
        }

        public String generateGremlin() {
            final int targetStepCount = rand.nextInt(10);
            final StringBuilder sb = new StringBuilder("g.V()");
            final Vertex start = syntaxGraph.traversal().V().has("starter", true).order().by(this::shuffle).next();
            sb.append((String) start.value("step"));

            syntaxGraph.traversal().V(start).times(targetStepCount - 1).repeat(
                    __.local(__.outE().has("weight", P.gte(rand.nextDouble()))
                            .inV().order().by(this::shuffle).limit(1)).sideEffect(t -> sb.append((String) t.get().value("step")))
            ).iterate();

            return sb.toString();
        }

        private int shuffle(final Object o1, final Object o2) {
            return rand.nextBoolean() ? -1 : 1;
        }

        private static void loadGraph(final Graph syntaxGraph) {
            final Vertex vOutStep = syntaxGraph.addVertex("step", ".out()", "starter", true);
            final Vertex vInStep = syntaxGraph.addVertex("step", ".in()", "starter", true);
            final Vertex vBothStep = syntaxGraph.addVertex("step", ".both()", "starter", true);
            final Vertex vInEStep = syntaxGraph.addVertex("step", ".inE()", "starter", true);
            final Vertex vOutEStep = syntaxGraph.addVertex("step", ".outE()", "starter", true);
            final Vertex vBothEStep = syntaxGraph.addVertex("step", ".bothE()", "starter", true);
            final Vertex vInVStep = syntaxGraph.addVertex("step", ".inV()", "starter", false);
            final Vertex vOutVStep = syntaxGraph.addVertex("step", ".outV()", "starter", false);
            final Vertex vOtherVStep = syntaxGraph.addVertex("step", ".otherV()", "starter", false);

            vOutStep.addEdge("followedBy", vOutStep, "weight", 1.0d);
            vOutStep.addEdge("followedBy", vInStep, "weight", 0.15d);
            vOutStep.addEdge("followedBy", vBothStep, "weight", 0.15d);
            vOutStep.addEdge("followedBy", vOutEStep, "weight", 0.75d);
            vOutStep.addEdge("followedBy", vInEStep, "weight", 0.1d);
            vOutStep.addEdge("followedBy", vBothEStep, "weight", 0.1d);

            vInStep.addEdge("followedBy", vOutStep, "weight", 0.15d);
            vInStep.addEdge("followedBy", vInStep, "weight", 1.0d);
            vInStep.addEdge("followedBy", vBothStep, "weight", 0.15d);
            vInStep.addEdge("followedBy", vOutEStep, "weight", 0.1d);
            vInStep.addEdge("followedBy", vInEStep, "weight", 0.75d);
            vInStep.addEdge("followedBy", vBothEStep, "weight", 0.1d);

            vOtherVStep.addEdge("followedBy", vOutStep, "weight", 0.15d);
            vOtherVStep.addEdge("followedBy", vInStep, "weight", 1.0d);
            vOtherVStep.addEdge("followedBy", vBothStep, "weight", 0.15d);
            vOtherVStep.addEdge("followedBy", vOutEStep, "weight", 0.1d);
            vOtherVStep.addEdge("followedBy", vInEStep, "weight", 0.75d);
            vOtherVStep.addEdge("followedBy", vBothEStep, "weight", 0.1d);

            vBothStep.addEdge("followedBy", vOutStep, "weight", 1.0d);
            vBothStep.addEdge("followedBy", vInStep, "weight", 1.0d);
            vBothStep.addEdge("followedBy", vBothStep, "weight", 0.1d);
            vBothStep.addEdge("followedBy", vOutEStep, "weight", 0.15d);
            vBothStep.addEdge("followedBy", vInEStep, "weight", 0.15d);
            vBothStep.addEdge("followedBy", vBothEStep, "weight", 0.1d);

            vInEStep.addEdge("followedBy", vOutVStep, "weight", 1.0d);
            vInEStep.addEdge("followedBy", vInVStep, "weight", 0.1d);

            vOutEStep.addEdge("followedBy", vInVStep, "weight", 1.0d);
            vInEStep.addEdge("followedBy", vOutVStep, "weight", 0.1d);

            vBothEStep.addEdge("followedBy", vOtherVStep, "weight", 1.0d);

            vInVStep.addEdge("followedBy", vOutStep, "weight", 1.0d);
            vInVStep.addEdge("followedBy", vInStep, "weight", 0.25d);
            vInVStep.addEdge("followedBy", vBothStep, "weight", 0.1d);
            vInVStep.addEdge("followedBy", vOutEStep, "weight", 1.0d);
            vInVStep.addEdge("followedBy", vInEStep, "weight", 0.25d);
            vInVStep.addEdge("followedBy", vBothEStep, "weight", 0.1d);

            vOutVStep.addEdge("followedBy", vOutStep, "weight", 0.25d);
            vOutVStep.addEdge("followedBy", vInStep, "weight", 1.0d);
            vOutVStep.addEdge("followedBy", vBothStep, "weight", 0.1d);
            vOutVStep.addEdge("followedBy", vOutEStep, "weight", 0.25d);
            vOutVStep.addEdge("followedBy", vInEStep, "weight", 1.0d);
            vOutVStep.addEdge("followedBy", vBothEStep, "weight", 0.1d);
        }
    }
}
