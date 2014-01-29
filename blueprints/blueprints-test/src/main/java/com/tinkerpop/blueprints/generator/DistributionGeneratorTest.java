package com.tinkerpop.blueprints.generator;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.BlueprintsStandardSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.util.StreamFactory;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class DistributionGeneratorTest {

    @RunWith(Parameterized.class)
    public static class DifferentDistributionTest extends AbstractBlueprintsTest {

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

            final Graph g1 = BlueprintsStandardSuite.GraphManager.get().newTestGraph();
            prepareGraph(g1);
            final DistributionGenerator generator1 = new DistributionGenerator("knows");
            distributionGeneratorTest(g1, generator1);

            assertNotEquals(StreamFactory.stream(g.query().edges()).count(), StreamFactory.stream(g1.query().edges()).count());
        }

        @Test
        public void shouldGenerateSameGraph() throws Exception {
            final DistributionGenerator generator = new DistributionGenerator("knows", null, ()->123456789l);
            distributionGeneratorTest(g, generator);

            final Graph g1 = BlueprintsStandardSuite.GraphManager.get().newTestGraph();
            prepareGraph(g1);
            final DistributionGenerator generator1 = new DistributionGenerator("knows", null, ()->123456789l);
            distributionGeneratorTest(g1, generator1);

            assertEquals(StreamFactory.stream(g.query().edges()).count(), StreamFactory.stream(g1.query().edges()).count());
        }

        @Override
        protected void prepareGraph(final Graph g) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) g.addVertex(Property.Key.ID, i);
        }

        private void distributionGeneratorTest(final Graph graph, final DistributionGenerator generator) {
            generator.setOutDistribution(inDistribution);
            if (outDistribution != null) generator.setOutDistribution(outDistribution);
            final int numEdges = generator.generate(graph, numberOfVertices * 10);
            tryCommit(graph, g -> assertEquals(numEdges, SizableIterable.sizeOf(g.query().edges())));
        }
    }
}
