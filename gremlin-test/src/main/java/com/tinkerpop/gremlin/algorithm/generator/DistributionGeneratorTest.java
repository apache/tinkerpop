package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class DistributionGeneratorTest {

    @RunWith(Parameterized.class)
    public static class DifferentDistributionsTest extends AbstractGeneratorTest {

        @Parameterized.Parameters(name = "{index}: test({0},{1})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {new NormalDistribution(2), new NormalDistribution(2)},
                    {new NormalDistribution(2), new NormalDistribution(5)},
                    {new PowerLawDistribution(2.1), new PowerLawDistribution(2.1)},
                    {new PowerLawDistribution(2.9), new PowerLawDistribution(2.9)},
                    {new PowerLawDistribution(3.9), new PowerLawDistribution(3.9)},
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
            int executions = 0;
            boolean same = true;

            // try this a few times because it's possible that the distribution generator is not random enough.
            // if it doesn't generate a random one after 5 times then there must be a problem
            do {
                final Configuration configuration1 = graphProvider.newGraphConfiguration("g1");
                final Graph g1 = graphProvider.openTestGraph(configuration1);

                final Configuration configuration2 = graphProvider.newGraphConfiguration("g2");
                final Graph g2 = graphProvider.openTestGraph(configuration2);

                try {
                    prepareGraph(g1);
                    final DistributionGenerator generator = makeGenerator(g1).build();
                    distributionGeneratorTest(g1, generator);

                    prepareGraph(g2);
                    final DistributionGenerator generator1 = makeGenerator(g2).build();
                    distributionGeneratorTest(g2, generator1);

                    same = same(g1, g2);
                } catch (Exception ex) {
                    throw ex;
                } finally {
                    graphProvider.clear(g1, configuration1);
                    graphProvider.clear(g2, configuration2);
                    executions++;
                }
            } while (same || executions < 5);
        }

        private DistributionGenerator.Builder makeGenerator(final Graph g) {
            return DistributionGenerator.create(g)
                    .label("knows")
                    .outDistribution(outDistribution)
                    .inDistribution(inDistribution)
                    .expectedNumEdges(numberOfVertices * 10);
        }

        @Test
        public void shouldGenerateSameGraph() throws Exception {
            final Configuration configuration = graphProvider.newGraphConfiguration("g1");
            final Graph g1 = graphProvider.openTestGraph(configuration);
            try {
                final DistributionGenerator generator = makeGenerator(g).seedGenerator(() -> 123456789l).build();
                distributionGeneratorTest(g, generator);

                prepareGraph(g1);
                final DistributionGenerator generator1 = makeGenerator(g1).seedGenerator(() -> 123456789l).build();
                distributionGeneratorTest(g1, generator1);

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

        private void distributionGeneratorTest(final Graph graph, final DistributionGenerator generator) {
            final int numEdges = generator.generate();
            assertTrue(numEdges > 0);
            tryCommit(graph, g -> assertEquals(numEdges, g.E().count()));
        }
    }

    public static class ProcessorTest extends AbstractGremlinTest {
        private static final int numberOfVertices = 100;

        @Test
        public void shouldProcessEdges() {
            final Distribution dist = new NormalDistribution(2);
            final DistributionGenerator generator = DistributionGenerator.create(g)
                    .label("knows")
                    .edgeProcessor(e -> e.property("data", "test"))
                    .outDistribution(dist)
                    .inDistribution(dist)
                    .expectedNumEdges(100).build();
            final int edgesGenerated = generator.generate();
            assertTrue(edgesGenerated > 0);
            tryCommit(g, g -> {
                assertEquals(edgesGenerated, g.E().count());
                assertTrue(g.V().count() > 0);
                assertTrue(g.E().toList().stream().allMatch(e -> e.value("data").equals("test")));
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
