package com.tinkerpop.gremlin.structure;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.io.GraphWriter;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class GraphWritePerformanceTest {

    @AxisRange(min = 0, max = 1)
    @BenchmarkMethodChart(filePrefix = "structure-write")
    @BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-structure-write")
    public static class WriteToGraph extends AbstractGremlinTest {

        @Rule
        public TestRule benchmarkRun = new BenchmarkRule();

        @Test
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void writeEmptyVertices() throws Exception {
            final int verticesToGenerate = 100000;
            for (int ix = 0; ix < verticesToGenerate; ix++) {
                g.addVertex();
                tryBatchCommit(g, ix);
            }

            assertVertexEdgeCounts(verticesToGenerate, 0).accept(g);
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
                tryBatchCommit(g, ix);
            }

            assertVertexEdgeCounts(verticesToGenerate, verticesToGenerate - 1).accept(g);
        }
    }

    @AxisRange(min = 0, max = 1)
    @BenchmarkMethodChart(filePrefix = "io-write")
    @BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-io-write")
    public static class WriteToIO extends AbstractGremlinTest {
        @Rule
        public TestRule benchmarkRun = new BenchmarkRule();

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void writeKryo() throws Exception {
            final GraphWriter writer = KryoWriter.build().create();
            final OutputStream os = new ByteArrayOutputStream();
            writer.writeGraph(os, g);
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void writeGraphML() throws Exception {
            final GraphWriter writer = GraphMLWriter.build().create();
            final OutputStream os = new ByteArrayOutputStream();
            writer.writeGraph(os, g);
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void writeGraphSON() throws Exception {
            final GraphWriter writer = GraphSONWriter.build().create();
            final OutputStream os = new ByteArrayOutputStream();
            writer.writeGraph(os, g);
        }
    }

    private static void tryBatchCommit(final Graph g, int ix) {
        if (g.features().graph().supportsTransactions() && ix % 1000 == 0)
            g.tx().commit();
    }
}
