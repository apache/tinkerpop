package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.function.Supplier;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class CommunityGeneratorTest {

    @RunWith(Parameterized.class)
    public static class DifferentDistributionsTest extends AbstractGeneratorTest {

        @Parameterized.Parameters(name = "{index}: {0}.test({1},{2})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {new NormalDistribution(2), new PowerLawDistribution(2.4), 0.1},
                    {new NormalDistribution(2), new PowerLawDistribution(2.4), 0.5},
                    {new NormalDistribution(2), new NormalDistribution(4), 0.5},
                    {new NormalDistribution(2), new NormalDistribution(4), 0.1},
                    {new PowerLawDistribution(2.3), new PowerLawDistribution(2.4), 0.2},
                    {new PowerLawDistribution(2.3), new NormalDistribution(4), 0.2}
            });
        }

        @Parameterized.Parameter(value = 0)
        public Distribution communityDistribution;

        @Parameterized.Parameter(value = 1)
        public Distribution degreeDistribution;

        @Parameterized.Parameter(value = 2)
        public double crossPcent;

        private static final int numberOfVertices = 100;

        @Test
        public void shouldGenerateRandomGraph() throws Exception {
            final Configuration configuration = graphProvider.newGraphConfiguration("g1");
            final Graph g1 = graphProvider.openTestGraph(configuration);

            try {
                communityGeneratorTest(g, null);

                prepareGraph(g1);
                communityGeneratorTest(g1, null);

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
        }

        @Test
        public void shouldGenerateSameGraph() throws Exception {
            final Configuration configuration = graphProvider.newGraphConfiguration("g1");
            final Graph g1 = graphProvider.openTestGraph(configuration);

            try {
                communityGeneratorTest(g, () -> 123456789l);

                prepareGraph(g1);
                communityGeneratorTest(g1, () -> 123456789l);

                assertTrue(g.E().count().next() > 0);
                assertTrue(g.V().count().next() > 0);
                assertTrue(g1.E().count().next() > 0);
                assertTrue(g1.V().count().next() > 0);
                assertEquals(g.E().count(), g1.E().count());

                // ensure that every vertex has the same number of edges between graphs.
                assertTrue(same(g, g1));
            } catch (Exception ex) {
                throw ex;
            } finally {
                graphProvider.clear(g1, configuration);
            }
        }

        @Override
        protected void prepareGraph(final Graph graph) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) graph.addVertex("oid", i);
            tryCommit(graph);
        }

        private void communityGeneratorTest(final Graph graph, final Supplier<Long> seedGenerator) throws Exception {
            boolean generated = false;
            double localCrossPcent = crossPcent;
            while (!generated) {
                try {
                    final CommunityGenerator generator = CommunityGenerator.create(graph)
                            .label("knows")
                            .communityDistribution(communityDistribution)
                            .degreeDistribution(degreeDistribution)
                            .crossCommunityPercentage(localCrossPcent)
                            .expectedNumCommunities(numberOfVertices / 10)
                            .expectedNumEdges(numberOfVertices * 10)
                            .seedGenerator(seedGenerator).build();
                    final int numEdges = generator.generate();
                    assertTrue(numEdges > 0);
                    tryCommit(graph, g -> assertEquals(new Long(numEdges), g.E().count().next()));
                    generated = true;
                } catch (IllegalArgumentException iae) {
                    generated = false;
                    localCrossPcent = localCrossPcent - 0.005d;

                    if (localCrossPcent < 0d)
                        fail("Cross community percentage should not be less than zero");

                    graph.V().remove();
                    tryCommit(graph);
                    prepareGraph(graph);
                    System.out.println(String.format("Ran CommunityGeneratorTest with different CrossCommunityPercentage, expected %s but used %s", crossPcent, localCrossPcent));
                }
            }
        }
    }

    public static class ProcessorTest extends AbstractGremlinTest {
        private static final int numberOfVertices = 100;

        @Test
        public void shouldProcessVerticesEdges() {
            final Distribution dist = new NormalDistribution(2);
            final CommunityGenerator generator = CommunityGenerator.create(g)
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
                    .expectedNumEdges(1000).build();
            final long edgesGenerated = generator.generate();
            assertTrue(edgesGenerated > 0);
            tryCommit(g, g -> {
                assertEquals(new Long(edgesGenerated), g.E().count().next());
                assertTrue(g.V().count().next() > 0);
                assertTrue(g.E().toList().stream().allMatch(e -> e.value("data").equals("test")));
                assertTrue(g.V().toList().stream().allMatch(
                        v -> v.value("test").equals("data") && v.property("communityIndex").isPresent()
                ));
            });
        }

        @Override
        protected void prepareGraph(final Graph graph) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) graph.addVertex("oid", i);
            tryCommit(graph);
        }
    }
}
