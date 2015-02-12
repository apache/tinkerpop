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
package org.apache.tinkerpop.gremlin.algorithm.generator;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.StreamFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class CommunityGeneratorTest {

    @RunWith(Parameterized.class)
    public static class DifferentDistributionsTest extends AbstractGeneratorTest {

        @Parameterized.Parameters(name = "test({0},{1},{2})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {new NormalDistribution(2), new PowerLawDistribution(2.4), 0.1},
                    {new NormalDistribution(2), new PowerLawDistribution(2.4), 0.5},
                    {new NormalDistribution(2), new NormalDistribution(4), 0.5},
                    {new NormalDistribution(2), new NormalDistribution(4), 0.1},
                    {new PowerLawDistribution(2.3), new PowerLawDistribution(2.4), 0.25},
                    {new PowerLawDistribution(2.3), new NormalDistribution(4), 0.25}
            });
        }

        @Parameterized.Parameter(value = 0)
        public Distribution communityDistribution;

        @Parameterized.Parameter(value = 1)
        public Distribution degreeDistribution;

        @Parameterized.Parameter(value = 2)
        public double crossPcent;

        private static final int numberOfVertices = 100;

        /**
         * Keep a failures count across all tests in the set and continually evaluate if those failures exceed
         * the threshold for "total" failure.  This approach helps soften the tests a bit to prevent failures
         * due to non-deterministic behavior in graph generation techniques.
         */
        private final AtomicInteger failures = new AtomicInteger();

        private final static int ultimateFailureThreshold = 3;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldGenerateDifferentGraph() throws Exception {
            final Configuration configuration = graphProvider.newGraphConfiguration("g1", this.getClass(), name.getMethodName());
            final Graph g1 = graphProvider.openTestGraph(configuration);

            try {
                communityGeneratorTest(g, () -> 123456789l);

                afterLoadGraphWith(g1);
                communityGeneratorTest(g1, () -> 987654321l);

                assertTrue(g.E().count().next() > 0);
                assertTrue(g.V().count().next() > 0);
                assertTrue(g1.E().count().next() > 0);
                assertTrue(g1.V().count().next() > 0);

                // don't assert counts of edges...those may be the same, just ensure that not every vertex has the
                // same number of edges between graphs.  that should make it harder for the test to fail.
                assertFalse(same(g, g1));
            } catch (Exception ex) {
                throw ex;
            } finally {
                graphProvider.clear(g1, configuration);
            }

            assertFalse(failures.get() >= ultimateFailureThreshold);
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldGenerateSameGraph() throws Exception {
            final Configuration configuration = graphProvider.newGraphConfiguration("g1", this.getClass(), name.getMethodName());
            final Graph g1 = graphProvider.openTestGraph(configuration);

            try {
                communityGeneratorTest(g, () -> 123456789l);

                afterLoadGraphWith(g1);
                communityGeneratorTest(g1, () -> 123456789l);

                assertTrue(g.E().count().next() > 0);
                assertTrue(g.V().count().next() > 0);
                assertTrue(g1.E().count().next() > 0);
                assertTrue(g1.V().count().next() > 0);
                assertEquals(g.E().count().next(), g1.E().count().next());

                // ensure that every vertex has the same number of edges between graphs.
                assertTrue(same(g, g1));
            } catch (Exception ex) {
                throw ex;
            } finally {
                graphProvider.clear(g1, configuration);
            }

            assertFalse(failures.get() >= ultimateFailureThreshold);
        }

        @Override
        protected void afterLoadGraphWith(final Graph graph) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) graph.addVertex("oid", i);
            tryCommit(graph);
        }

        protected Iterable<Vertex> verticesByOid(final Graph graph) {
            List<Vertex> vertices = graph.V().toList();
            Collections.sort(vertices,
                    (v1, v2) -> ((Integer) v1.value("oid")).compareTo((Integer) v2.value("oid")));
            return vertices;
        }

        private void communityGeneratorTest(final Graph graph, final Supplier<Long> seedGenerator) throws Exception {
            boolean generated = false;
            double localCrossPcent = crossPcent;
            while (!generated) {
                try {
                    final CommunityGenerator generator = CommunityGenerator.build(graph)
                            .label("knows")
                            .communityDistribution(communityDistribution)
                            .degreeDistribution(degreeDistribution)
                            .crossCommunityPercentage(localCrossPcent)
                            .expectedNumCommunities(numberOfVertices / 10)
                            .expectedNumEdges(numberOfVertices * 10)
                            .seedGenerator(seedGenerator)
                            .verticesToGenerateEdgesFor(verticesByOid(graph))
                            .create();
                    final int numEdges = generator.generate();
                    assertTrue(numEdges > 0);
                    tryCommit(graph, g -> assertEquals(new Long(numEdges), new Long(IteratorUtils.count(g.iterators().edgeIterator()))));
                    generated = true;
                } catch (IllegalArgumentException iae) {
                    localCrossPcent = localCrossPcent - 0.005d;
                    generated = localCrossPcent < 0d;

                    graph.iterators().vertexIterator().forEachRemaining(Vertex::remove);
                    tryCommit(graph);
                    afterLoadGraphWith(graph);

                    System.out.println(String.format("Ran CommunityGeneratorTest with different CrossCommunityPercentage, expected %s but used %s", crossPcent, localCrossPcent));

                    if (generated) failures.incrementAndGet();
                }
            }
        }
    }

    public static class ProcessorTest extends AbstractGremlinTest {
        private static final int numberOfVertices = 100;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldProcessVerticesEdges() {
            final Distribution dist = new NormalDistribution(2);
            final CommunityGenerator generator = CommunityGenerator.build(g)
                    .label("knows")
                    .edgeProcessor(e -> e.<String>property("data", "test"))
                    .vertexProcessor((v, m) -> {
                        m.forEach(v::property);
                        v.property("test", "data");
                    })
                    .communityDistribution(dist)
                    .degreeDistribution(dist)
                    .crossCommunityPercentage(0.0d)
                    .expectedNumCommunities(2)
                    .expectedNumEdges(1000).create();
            final long edgesGenerated = generator.generate();
            assertTrue(edgesGenerated > 0);
            tryCommit(g, g -> {
                assertEquals(new Long(edgesGenerated), new Long(IteratorUtils.count(g.iterators().edgeIterator())));
                assertTrue(IteratorUtils.count(g.iterators().vertexIterator()) > 0);
                assertTrue(StreamFactory.stream(g.iterators().edgeIterator()).allMatch(e -> e.value("data").equals("test")));
                assertTrue(StreamFactory.stream(g.iterators().vertexIterator()).allMatch(
                        v -> v.value("test").equals("data") && v.property("communityIndex").isPresent()
                ));
            });
        }

        @Override
        protected void afterLoadGraphWith(final Graph graph) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) graph.addVertex("oid", i);
            tryCommit(graph);
        }
    }
}
