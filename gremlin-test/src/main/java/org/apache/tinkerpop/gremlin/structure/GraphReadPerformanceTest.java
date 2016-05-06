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
package org.apache.tinkerpop.gremlin.structure;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.algorithm.generator.Distribution;
import org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGenerator;
import org.apache.tinkerpop.gremlin.algorithm.generator.PowerLawDistribution;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.1, replaced by gremlin-benchmark.
 */
@RunWith(Enclosed.class)
@Deprecated
public class GraphReadPerformanceTest {
    @AxisRange(min = 0, max = 1)
    @BenchmarkMethodChart(filePrefix = "gremlin-read")
    @BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-gremlin-read")
    public static class ReadFromGraph extends AbstractGremlinTest {

        @Rule
        public TestRule benchmarkRun = new BenchmarkRule();

        private Set<Object> ids = new HashSet<>();
        private int edgeCount = 0;

        @Override
        protected void afterLoadGraphWith(final Graph g) throws Exception {
            ids.clear();
            final int numVertices = 10000;
            final Random r = new Random(854939487556l);
            for (int i = 0; i < numVertices; i++) {
                final Vertex v = g.addVertex("oid", i, "name", RandomStringUtils.randomAlphabetic(r.nextInt(1024)));
                ids.add(v.id());
            }

            final Distribution inDist = new PowerLawDistribution(2.3);
            final Distribution outDist = new PowerLawDistribution(2.8);
            final DistributionGenerator generator = DistributionGenerator.build(g)
                    .label("knows")
                    .seedGenerator(r::nextLong)
                    .outDistribution(outDist)
                    .inDistribution(inDist)
                    .edgeProcessor(e -> e.<Double>property("weight", r.nextDouble()))
                    .expectedNumEdges(numVertices * 3).create();
            edgeCount = generator.generate();
        }

        @Test
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void readAllVerticesAndProperties() throws Exception {
            final AtomicInteger counter = new AtomicInteger(0);

            // read the vertices 10 times over
            for (int ix = 0; ix < 10; ix++) {
                graph.vertices().forEachRemaining(vertex -> {
                    assertNotNull(vertex.value("name"));
                    counter.incrementAndGet();
                });

                assertEquals(10000, counter.get());
                counter.set(0);
            }
        }

        @Test
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void readAllEdgesAndProperties() throws Exception {
            final AtomicInteger counter = new AtomicInteger(0);

            // read the vertices 10 times over
            for (int ix = 0; ix < 10; ix++) {
                graph.edges().forEachRemaining(edge -> {
                    assertNotNull(edge.value("weight"));
                    counter.incrementAndGet();
                });

                assertEquals(edgeCount, counter.get());
                counter.set(0);
            }
        }
    }
}
