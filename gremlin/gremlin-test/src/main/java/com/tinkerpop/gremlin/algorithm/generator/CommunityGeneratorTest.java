package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Triplet;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

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
                final CommunityGenerator generator = new CommunityGenerator("knows");
                communityGeneratorTest(g, generator);

                prepareGraph(g1);
                final CommunityGenerator generator1 = new CommunityGenerator("knows");
                communityGeneratorTest(g1, generator1);

                assertTrue(g.E().count() > 0);
                assertTrue(g.V().count() > 0);
                assertTrue(g1.E().count() > 0);
                assertTrue(g1.V().count() > 0);

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
                final CommunityGenerator generator = new CommunityGenerator("knows", null, null, () -> 123456789l);
                communityGeneratorTest(g, generator);

                prepareGraph(g1);
                final CommunityGenerator generator1 = new CommunityGenerator("knows", null, null, () -> 123456789l);
                communityGeneratorTest(g1, generator1);

                assertTrue(g.E().count() > 0);
                assertTrue(g.V().count() > 0);
                assertTrue(g1.E().count() > 0);
                assertTrue(g1.V().count() > 0);
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

        private void communityGeneratorTest(final Graph graph, final CommunityGenerator generator) throws Exception {
            boolean generated = false;
            double localCrossPcent = crossPcent;
            while (!generated) {
                try {
                    generator.setCommunityDistribution(communityDistribution);
                    generator.setDegreeDistribution(degreeDistribution);
                    generator.setCrossCommunityPercentage(localCrossPcent);
                    final int numEdges = generator.generate(graph, numberOfVertices / 10, numberOfVertices * 10);
                    assertTrue(numEdges > 0);
                    tryCommit(graph, g -> assertEquals(numEdges, g.E().count()));
                    generated = true;
                } catch (IllegalArgumentException iae) {
                    generated = false;
                    localCrossPcent = localCrossPcent - 0.05d;

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
        public void shouldProcessEdges() {
            final CommunityGenerator generator = new CommunityGenerator("knows", e -> e.setProperty("data", "test"));
            final Distribution dist = new NormalDistribution(2);
            generator.setCommunityDistribution(dist);
            generator.setDegreeDistribution(dist);
            generator.setCrossCommunityPercentage(0.0);
            final int edgesGenerated = generator.generate(g, 2, 1000);
            assertTrue(edgesGenerated > 0);
            tryCommit(g, g -> {
                assertEquals(edgesGenerated, g.E().count());
                assertTrue(g.V().count() > 0);
                assertTrue(g.E().toList().stream().allMatch(e -> e.getValue("data").equals("test")));
            });
        }

        @Test
        public void shouldProcessVertices() {
            final CommunityGenerator generator = new CommunityGenerator("knows", e -> e.setProperty("data", "test"));
            final Distribution dist = new NormalDistribution(2);
            generator.setCommunityDistribution(dist);
            generator.setDegreeDistribution(dist);
            generator.setCrossCommunityPercentage(0.0);
            final int edgesGenerated = generator.generate(g, 2, 1000);
            assertTrue(edgesGenerated > 0);
            tryCommit(g, g -> {
                assertEquals(edgesGenerated, g.E().count());
                assertTrue(g.V().count() > 0);
                assertTrue(g.E().toList().stream().allMatch(e -> e.getValue("data").equals("test")));
            });
        }

        @Test
        public void shouldProcessVerticesEdges() {
            final CommunityGenerator generator = new CommunityGenerator("knows", e -> e.setProperty("data", "test"), (v, m) -> {
                m.forEach(v::setProperty);
                v.setProperty("test", "data");
            });
            final Distribution dist = new NormalDistribution(2);
            generator.setCommunityDistribution(dist);
            generator.setDegreeDistribution(dist);
            generator.setCrossCommunityPercentage(0.0);
            final int edgesGenerated = generator.generate(g, 2, 1000);
            assertTrue(edgesGenerated > 0);
            tryCommit(g, g -> {
                assertEquals(edgesGenerated, g.E().count());
                assertTrue(g.V().count() > 0);
                assertTrue(g.E().toList().stream().allMatch(e -> e.getValue("data").equals("test")));
                assertTrue(g.V().toList().stream().allMatch(
                        v -> v.getValue("test").equals("data") && v.getProperty("communityIndex").isPresent()
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
