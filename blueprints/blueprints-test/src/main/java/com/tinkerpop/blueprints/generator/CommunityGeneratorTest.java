package com.tinkerpop.blueprints.generator;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StreamFactory;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class CommunityGeneratorTest {

    @RunWith(Parameterized.class)
    public static class DifferentDistributionsTest extends AbstractBlueprintsTest {

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
            final CommunityGenerator generator = new CommunityGenerator("knows");
            communityGeneratorTest(g, generator);

            final Configuration configuration = graphProvider.newGraphConfiguration("g1");
            final Graph g1 = graphProvider.openTestGraph(configuration);
            prepareGraph(g1);
            final CommunityGenerator generator1 = new CommunityGenerator("knows");
            communityGeneratorTest(g1, generator1);

            assertNotEquals(StreamFactory.stream(g.query().edges()).count(), StreamFactory.stream(g1.query().edges()).count());

            // ensure that not every vertex has the same number of edges between graphs
            assertFalse(StreamFactory.stream(g.query().vertices())
                    .map(v -> Triplet.with(v.getValue("oid"), StreamFactory.stream(v.query().direction(Direction.IN).edges()).count(),
                            StreamFactory.stream(v.query().direction(Direction.OUT).edges()).count()))
                    .allMatch(p -> {
                        final Vertex v = g1.query().has("oid", p.getValue0()).vertices().iterator().next();
                        return p.getValue1() == StreamFactory.stream(v.query().direction(Direction.IN).edges()).count()
                                && p.getValue2() == StreamFactory.stream(v.query().direction(Direction.OUT).edges()).count();
                    }));

            graphProvider.clear(g1, configuration);
        }

        @Test
        public void shouldGenerateSameGraph() throws Exception {
            final CommunityGenerator generator = new CommunityGenerator("knows", null , null, ()->123456789l);
            communityGeneratorTest(g, generator);

            final Configuration configuration = graphProvider.newGraphConfiguration("g1");
            final Graph g1 = graphProvider.openTestGraph(configuration);
            prepareGraph(g1);
            final CommunityGenerator generator1 = new CommunityGenerator("knows", null , null, ()->123456789l);
            communityGeneratorTest(g1, generator1);

            assertEquals(StreamFactory.stream(g.query().edges()).count(), StreamFactory.stream(g1.query().edges()).count());

            // ensure that every vertex has the same number of edges between graphs.
            assertTrue(StreamFactory.stream(g.query().vertices())
                    .map(v -> Triplet.with(v.getValue("oid"), StreamFactory.stream(v.query().direction(Direction.IN).edges()).count(),
                            StreamFactory.stream(v.query().direction(Direction.OUT).edges()).count()))
                    .allMatch(p -> {
                        final Vertex v = g1.query().has("oid", p.getValue0()).vertices().iterator().next();
                        return p.getValue1() == StreamFactory.stream(v.query().direction(Direction.IN).edges()).count()
                                && p.getValue2() == StreamFactory.stream(v.query().direction(Direction.OUT).edges()).count();
                    }));

            graphProvider.clear(g1, configuration);
        }

        @Override
        protected void prepareGraph(final Graph g) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) g.addVertex("oid", i);
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
                    assertEquals(numEdges, SizableIterable.sizeOf(graph.query().edges()));
                    generated = true;
                } catch (IllegalArgumentException iae) {
                    generated = false;
                    localCrossPcent = localCrossPcent - 0.05d;
                    graph.query().vertices().forEach(Vertex::remove);
                    prepareGraph(graph);
                    System.out.println(String.format("Ran CommunityGeneratorTest with different CrossCommunityPercentage, expected %s but used %s", crossPcent, localCrossPcent));
                }
            }
        }
    }


    public static class AnnotatorTest extends AbstractBlueprintsTest {
        @Test
        public void shouldAnnotateEdges() {
            final CommunityGenerator generator = new CommunityGenerator("knows", e->e.setProperty("data", "test"));
            final Distribution dist = new NormalDistribution(2);
            generator.setCommunityDistribution(dist);
            generator.setDegreeDistribution(dist);
            generator.setCrossCommunityPercentage(0.0);
            generator.generate(g, 100, 1000);
            tryCommit(g, g -> assertTrue(StreamFactory.stream(g.query().edges()).allMatch(e -> e.getValue("data").equals("test"))));
        }

        @Test
        public void shouldAnnotateVertices() {
            final CommunityGenerator generator = new CommunityGenerator("knows", e->e.setProperty("data", "test"));
            final Distribution dist = new NormalDistribution(2);
            generator.setCommunityDistribution(dist);
            generator.setDegreeDistribution(dist);
            generator.setCrossCommunityPercentage(0.0);
            generator.generate(g, 100, 1000);
            tryCommit(g, g -> assertTrue(StreamFactory.stream(g.query().edges()).allMatch(e -> e.getValue("data").equals("test"))));
        }

        @Test
        public void shouldAnnotateVerticesEdges() {
            final CommunityGenerator generator = new CommunityGenerator("knows", e->e.setProperty("data", "test"), (v,m)-> {
                m.forEach(v::setProperty);
                v.setProperty("test", "data");
            });
            final Distribution dist = new NormalDistribution(2);
            generator.setCommunityDistribution(dist);
            generator.setDegreeDistribution(dist);
            generator.setCrossCommunityPercentage(0.0);
            generator.generate(g, 100, 1000);
            tryCommit(g, g -> {
                assertTrue(StreamFactory.stream(g.query().edges()).allMatch(e -> e.getValue("data").equals("test")));
                assertTrue(StreamFactory.stream(g.query().vertices()).allMatch(
                    v -> v.getValue("test").equals("data") && v.getProperty("communityIndex").isPresent()
                ));
            });
        }
    }
}
