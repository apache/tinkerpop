package com.tinkerpop.blueprints.generator;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

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

        @Test
        public void shouldGenerateGraph() {
            distributionGeneratorTest(inDistribution, outDistribution);
        }

        private void distributionGeneratorTest(final Distribution indist, final Distribution outdist) {
            final int numNodes = 100;
            final Graph graph = g;
            for (int i = 0; i < numNodes; i++) graph.addVertex(Property.Key.ID, i);

            final DistributionGenerator generator = new DistributionGenerator("knows");
            generator.setOutDistribution(indist);
            if (outdist != null) generator.setOutDistribution(outdist);
            final int numEdges = generator.generate(graph, numNodes * 10);
            assertEquals(numEdges, SizableIterable.sizeOf(graph.query().edges()));
        }
    }
}
