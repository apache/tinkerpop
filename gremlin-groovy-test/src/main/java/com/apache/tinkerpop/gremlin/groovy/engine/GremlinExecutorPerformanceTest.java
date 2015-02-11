package com.apache.tinkerpop.gremlin.groovy.engine;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.apache.tinkerpop.gremlin.AbstractGremlinTest;
import com.apache.tinkerpop.gremlin.LoadGraphWith;
import com.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "gremlin-executor")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-gremlin-executor")
public class GremlinExecutorPerformanceTest extends AbstractGremlinTest  {

    private static final GremlinExecutor gremlinExecutor = GremlinExecutor.build().create();

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    public final static int DEFAULT_BENCHMARK_ROUNDS = 500;
    public final static int DEFAULT_WARMUP_ROUNDS = 10;

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void executorEval() throws Exception {
        final Map<String, Object> params = new HashMap<>();
        params.put("g", g);

        final CompletableFuture<Object> future1 = gremlinExecutor.eval("g.V().outE().inV().outE().inV().outE().inV().next(512)", params);
        final CompletableFuture<Object> future2 = gremlinExecutor.eval("g.V().local(out().out().values(\"name\").fold()).next(7)", params);
        final CompletableFuture<Object> future3 = gremlinExecutor.eval("g.V().repeat(out()).times(3).next(10)", params);
        final CompletableFuture<Object> future4 = gremlinExecutor.eval("g.V().repeat(out()).times(2).next(10)", params);
        final CompletableFuture<Object> future5 = gremlinExecutor.eval("g.V().out().out().out().path().next(1)", params);

        assertEquals(512, IteratorUtils.count(IteratorUtils.convertToIterator(future1.join())));
        assertEquals(7, IteratorUtils.count(IteratorUtils.convertToIterator(future2.join())));
        assertEquals(10, IteratorUtils.count(IteratorUtils.convertToIterator(future3.join())));
        assertEquals(10, IteratorUtils.count(IteratorUtils.convertToIterator(future4.join())));
        assertEquals(1, IteratorUtils.count(IteratorUtils.convertToIterator(future5.join())));
    }
}
