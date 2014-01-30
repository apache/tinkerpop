package com.tinkerpop.blueprints.generator;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import org.junit.Ignore;
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

        @Test
        @Ignore("Make not lock or cause exceptions")
        public void shouldGenerateGraph() {
            communityGeneratorTest(communityDistribution, degreeDistribution, crossPcent);
        }

        // todo: make this test like the DistributionGeneratorTest.

        private void communityGeneratorTest(final Distribution community, final Distribution degree, final double crossPercentage) {
            final int numNodes = 100;
            final Graph graph = g;
            for (int i = 0; i < numNodes; i++) graph.addVertex(Property.Key.ID, i);

            final CommunityGenerator generator = new CommunityGenerator("knows");
            generator.setCommunityDistribution(community);
            generator.setDegreeDistribution(degree);
            generator.setCrossCommunityPercentage(crossPercentage);
            final int numEdges = generator.generate(graph, numNodes / 10, numNodes * 10);
            assertEquals(numEdges, SizableIterable.sizeOf(graph.query().edges()));
        }
    }

    // todo: add tests for annotations.
}
