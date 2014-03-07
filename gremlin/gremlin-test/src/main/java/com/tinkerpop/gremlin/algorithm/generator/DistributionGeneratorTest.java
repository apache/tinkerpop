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
import java.util.Optional;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class DistributionGeneratorTest {

    @RunWith(Parameterized.class)
    public static class DifferentDistributionsTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: test({0},{1})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {new NormalDistribution(2), null},
                    {new NormalDistribution(2), new NormalDistribution(5)},
                    {new PowerLawDistribution(2.1), null},
                    {new PowerLawDistribution(2.9), null},
                    {new PowerLawDistribution(3.9), null},
                    {new PowerLawDistribution(2.3), new PowerLawDistribution(2.8)}
            });
        }

        @Parameterized.Parameter(value = 0)
        public Distribution inDistribution;

        @Parameterized.Parameter(value = 1)
        public Distribution outDistribution;

        private static final int numberOfVertices = 100;

        @Test
        public void shouldGenerateRandomGraph() throws Exception {
            final Configuration configuration = graphProvider.newGraphConfiguration("g1");
            final Graph g1 = graphProvider.openTestGraph(configuration);
            try {
                final DistributionGenerator generator = new DistributionGenerator("knows");
                distributionGeneratorTest(g, generator);

                prepareGraph(g1);
                final DistributionGenerator generator1 = new DistributionGenerator("knows");
                distributionGeneratorTest(g1, generator1);

                // don't assert counts of edges...those may be the same, just ensure that not every vertex has the
                // same number of edges between graphs.  that should make it harder for the test to fail.
                assertFalse(g.V().toList().stream()
                        .map(v -> Triplet.with(v.getValue("oid"), v.inE().count(), v.outE().count()))
                        .allMatch(p -> {
                            final Vertex v = (Vertex) g1.V().has("oid", p.getValue0()).next();
                            return p.getValue1() == v.inE().count()
                                    && p.getValue2() == v.outE().count();
                        }));
            } catch(Exception ex) {
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
                final DistributionGenerator generator = new DistributionGenerator("knows", null, () -> 123456789l);
                distributionGeneratorTest(g, generator);

                prepareGraph(g1);
                final DistributionGenerator generator1 = new DistributionGenerator("knows", null, () -> 123456789l);
                distributionGeneratorTest(g1, generator1);

                // ensure that every vertex has the same number of edges between graphs.
                assertTrue(g.V().toList().stream()
                        .map(v -> Triplet.with(v.getValue("oid"), v.inE().count(), v.outE().count()))
                        .allMatch(p -> {
                            final Vertex v = (Vertex) g1.V().has("oid", p.getValue0()).next();
                            return p.getValue1() == v.inE().count()
                                    && p.getValue2() == v.outE().count();
                        }));
            } catch(Exception ex) {
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

        private void distributionGeneratorTest(final Graph graph, final DistributionGenerator generator) {
            generator.setOutDistribution(inDistribution);
            Optional.ofNullable(outDistribution).ifPresent(generator::setOutDistribution);
            final int numEdges = generator.generate(graph, numberOfVertices * 10);
            assertTrue(numEdges > 0);
            tryCommit(graph, g -> assertEquals(numEdges, g.E().count()));
        }
    }

    public static class AnnotatorTest extends AbstractGremlinTest {
        private static final int numberOfVertices = 100;

        @Test
        public void shouldAnnotateEdges() {
            final DistributionGenerator generator = new DistributionGenerator("knows", e -> e.setProperty("data", "test"));
            final Distribution dist = new NormalDistribution(2);
            generator.setOutDistribution(dist);
            generator.setInDistribution(dist);
            final int edgesGenerated = generator.generate(g, 100);
            assertTrue(edgesGenerated > 0);
            tryCommit(g, g -> {
                assertEquals(edgesGenerated, g.E().count());
                assertTrue(g.V().count() > 0);
                assertTrue(g.E().toList().stream().allMatch(e -> e.getValue("data").equals("test")));
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
