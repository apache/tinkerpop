package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.*;

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
            final DistributionGenerator generator = new DistributionGenerator("knows");
            distributionGeneratorTest(g, generator);

            final Configuration configuration = graphProvider.newGraphConfiguration("g1");
            final Graph g1 = graphProvider.openTestGraph(configuration);
            prepareGraph(g1);
            final DistributionGenerator generator1 = new DistributionGenerator("knows");
            distributionGeneratorTest(g1, generator1);

            // todo: test graph structure

            graphProvider.clear(g1, configuration);
        }

        @Test
        public void shouldGenerateSameGraph() throws Exception {
            final DistributionGenerator generator = new DistributionGenerator("knows", null, () -> 123456789l);
            distributionGeneratorTest(g, generator);

            final Configuration configuration = graphProvider.newGraphConfiguration("g1");
            final Graph g1 = graphProvider.openTestGraph(configuration);
            prepareGraph(g1);
            final DistributionGenerator generator1 = new DistributionGenerator("knows", null, () -> 123456789l);
            distributionGeneratorTest(g1, generator1);

            assertEquals(g.E().count(), g1.E().count());

            graphProvider.clear(g1, configuration);
        }

        @Override
        protected void prepareGraph(final Graph g) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) g.addVertex("oid", i);
        }

        private void distributionGeneratorTest(final Graph graph, final DistributionGenerator generator) {
            generator.setOutDistribution(inDistribution);
            Optional.ofNullable(outDistribution).ifPresent(generator::setOutDistribution);
            final int numEdges = generator.generate(graph, numberOfVertices * 10);
            tryCommit(graph, g -> assertEquals(numEdges, g.E().count()));
        }
    }

    public static class AnnotatorTest extends AbstractGremlinTest {
        @Test
        public void shouldAnnotateEdges() {
            final DistributionGenerator generator = new DistributionGenerator("knows", e -> e.setProperty("data", "test"));
            final Distribution dist = new NormalDistribution(2);
            generator.setOutDistribution(dist);
            generator.setInDistribution(dist);
            generator.generate(g, 100);
            tryCommit(g, g -> assertTrue(g.E().toList().stream().allMatch(e -> e.getValue("data").equals("test"))));
        }
    }
}
