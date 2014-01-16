package com.tinkerpop.blueprints;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.tinkerpop.blueprints.generator.CommunityGenerator;
import com.tinkerpop.blueprints.generator.Distribution;
import com.tinkerpop.blueprints.generator.DistributionGenerator;
import com.tinkerpop.blueprints.generator.NormalDistribution;
import com.tinkerpop.blueprints.generator.PowerLawDistribution;
import com.tinkerpop.blueprints.generator.SizableIterable;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class GraphGeneratePerformanceTest {

    @AxisRange(min = 0, max = 1)
    @BenchmarkMethodChart(filePrefix = "blueprints-write")
    @BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-blueprints-write")
    public static class WriteToGraph extends AbstractBlueprintsTest {

        @Rule
        public TestRule benchmarkRun = new BenchmarkRule();

        @Test
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void writeEmptyVertices() throws Exception {
            final int verticesToGenerate = 100000;
            for (int ix = 0; ix < verticesToGenerate; ix++) {
                g.addVertex();
            }

            AbstractBlueprintsSuite.assertVertexEdgeCounts(verticesToGenerate, 0).accept(g);
        }

        @Test
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void writeEmptyVerticesAndEdges() throws Exception {
            final int verticesToGenerate = 100000;
            Optional<Vertex> lastVertex = Optional.empty();
            for (int ix = 0; ix < verticesToGenerate; ix++) {
                final Vertex v = g.addVertex();
                if (lastVertex.isPresent())
                    v.addEdge("parent", lastVertex.get());

                lastVertex = Optional.of(v);
            }

            AbstractBlueprintsSuite.assertVertexEdgeCounts(verticesToGenerate, verticesToGenerate - 1).accept(g);
        }
    }

    @RunWith(Parameterized.class)
    public static class DistributionGeneratorTest extends AbstractBlueprintsTest {

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

    @RunWith(Parameterized.class)
    public static class CommunityGeneratorTest extends AbstractBlueprintsTest {

        @Parameterized.Parameters(name = "{index}: {0}.test({1},{2})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {new NormalDistribution(2), new PowerLawDistribution(2.4), 0.1},
                    {new NormalDistribution(2), new PowerLawDistribution(2.4), 0.5},
                    {new NormalDistribution(2), new NormalDistribution(4), 0.5},
                    {new NormalDistribution(2), new NormalDistribution(4), 0.1},
                    {new PowerLawDistribution(2.3), new PowerLawDistribution(2.4), 0.2},
                    {new PowerLawDistribution(2.3), new NormalDistribution(4),0.2}
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
}
