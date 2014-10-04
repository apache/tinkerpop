package com.tinkerpop.gremlin.structure;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.algorithm.generator.Distribution;
import com.tinkerpop.gremlin.algorithm.generator.DistributionGenerator;
import com.tinkerpop.gremlin.algorithm.generator.PowerLawDistribution;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class GraphReadPerformanceTest {
    @AxisRange(min = 0, max = 1)
    @BenchmarkMethodChart(filePrefix = "gremlin-read")
    @BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-gremlin-read")
    public static class ReadFromGraph extends AbstractGremlinTest {

        @Rule
        public TestRule benchmarkRun = new BenchmarkRule();

        private Set<Object> ids = new HashSet<>();

        @Override
        protected void afterLoadGraphWith(final Graph g) throws Exception {
            ids.clear();
            final int numVertices = 10000;
            final Random r = new Random(854939487556l);
            for (int i = 0; i < numVertices; i++) {
                final Vertex v = g.addVertex("oid", i, "name", RandomStringUtils.randomAlphabetic(r.nextInt(1024)));
                ids.add(v.id());
            }

            final Distribution inDist = new PowerLawDistribution(2.3);
            final Distribution outDist = new PowerLawDistribution(2.8);
            final DistributionGenerator generator = DistributionGenerator.build(g)
                    .label("knows")
                    .seedGenerator(r::nextLong)
                    .outDistribution(outDist)
                    .inDistribution(inDist)
                    .expectedNumEdges(numVertices * 3).create();
            generator.generate();
        }

        @Test
        @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        public void readAllProperties() throws Exception {
            final AtomicInteger counter = new AtomicInteger(0);

            // read the vertices 10 times over
            for (int ix = 0; ix < 10; ix++) {
                ids.stream().map(g::v).forEach(v -> {
                    assertNotNull(v.value("name"));
                    counter.incrementAndGet();
                });

                assertEquals(10000, counter.get());
                counter.set(0);
            }
        }
    }
}
