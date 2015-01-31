package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class DistributionGeneratorTest {

    @RunWith(Parameterized.class)
    public static class DifferentDistributionsTest extends AbstractGeneratorTest {

        @Parameterized.Parameters(name = "test({0},{1})")
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
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldGenerateDifferentGraph() throws Exception {
            int executions = 0;
            boolean same = true;

            // try this a few times because it's possible that the distribution generator is not random enough.
            // if it doesn't generate a random one after 5 times then there must be a problem
            do {
                final Configuration configuration1 = graphProvider.newGraphConfiguration("g1", this.getClass(), name.getMethodName());
                final Graph g1 = graphProvider.openTestGraph(configuration1);

                final Configuration configuration2 = graphProvider.newGraphConfiguration("g2", this.getClass(), name.getMethodName());
                final Graph g2 = graphProvider.openTestGraph(configuration2);

                try {
                    afterLoadGraphWith(g1);
                    final DistributionGenerator generator = makeGenerator(g1).seedGenerator(() -> 123456789l).create();
                    distributionGeneratorTest(g1, generator);

                    afterLoadGraphWith(g2);
                    final DistributionGenerator generator1 = makeGenerator(g2).seedGenerator(() -> 987654321l).create();
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
            return DistributionGenerator.build(g)
                    .label("knows")
                    .outDistribution(outDistribution)
                    .inDistribution(inDistribution)
                    .expectedNumEdges(numberOfVertices * 10);
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldGenerateSameGraph() throws Exception {
            final Configuration configuration = graphProvider.newGraphConfiguration("g1", this.getClass(), name.getMethodName());
            final Graph g1 = graphProvider.openTestGraph(configuration);
            try {
                final Iterable<Vertex> vordered = verticesByOid(g);
                final DistributionGenerator generator = makeGenerator(g).seedGenerator(() -> 123456789l).inVertices(vordered).outVertices(vordered).create();
                distributionGeneratorTest(g, generator);

                afterLoadGraphWith(g1);
                final Iterable<Vertex> vordered1 = verticesByOid(g1);
                final DistributionGenerator generator1 = makeGenerator(g1).seedGenerator(() -> 123456789l).inVertices(vordered1).outVertices(vordered1).create();
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
        protected void afterLoadGraphWith(final Graph graph) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) graph.addVertex("oid", i);
            tryCommit(graph);
        }

        protected Iterable<Vertex> verticesByOid(final Graph graph) {
            List<Vertex> vertices = graph.V().toList();
            Collections.sort(vertices,
                    (v1, v2) -> ((Integer) v1.value("oid")).compareTo((Integer) v2.value("oid")));
            return vertices;
        }

        private void distributionGeneratorTest(final Graph graph, final DistributionGenerator generator) {
            final int numEdges = generator.generate();
            assertTrue(numEdges > 0);
            tryCommit(graph, g -> assertEquals(new Long(numEdges), g.E().count().next()));
        }
    }

    public static class ProcessorTest extends AbstractGremlinTest {
        private static final int numberOfVertices = 100;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldProcessEdges() {
            final Distribution dist = new NormalDistribution(2);
            final DistributionGenerator generator = DistributionGenerator.build(g)
                    .label("knows")
                    .edgeProcessor(e -> e.<String>property("data", "test"))
                    .outDistribution(dist)
                    .inDistribution(dist)
                    .expectedNumEdges(100).create();
            final int edgesGenerated = generator.generate();
            assertTrue(edgesGenerated > 0);
            tryCommit(g, g -> {
                assertEquals(new Long(edgesGenerated), new Long(IteratorUtils.count(g.iterators().edgeIterator())));
                assertTrue(IteratorUtils.count(g.iterators().vertexIterator()) > 0);
                assertTrue(StreamFactory.stream(g.iterators().edgeIterator()).allMatch(e -> e.value("data").equals("test")));
            });
        }

        @Override
        protected void afterLoadGraphWith(final Graph graph) throws Exception {
            final int numNodes = numberOfVertices;
            for (int i = 0; i < numNodes; i++) graph.addVertex("oid", i);
            tryCommit(graph);
        }
    }
}
