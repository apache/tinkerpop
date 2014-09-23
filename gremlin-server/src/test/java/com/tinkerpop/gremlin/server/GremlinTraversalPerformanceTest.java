package com.tinkerpop.gremlin.server;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.ResultSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * Uses a single client across multiple threads to issue requests against the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "gremlin-traversal")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-gremlin-traversal")
public class GremlinTraversalPerformanceTest extends AbstractGremlinServerPerformanceTest {

    public final static int DEFAULT_BENCHMARK_ROUNDS = 50;
    public final static int DEFAULT_WARMUP_ROUNDS = 5;

    public final static int DEFAULT_CONCURRENT_BENCHMARK_ROUNDS = 500;
    public final static int DEFAULT_CONCURRENT_WARMUP_ROUNDS = 10;

    private final static Cluster cluster = Cluster.build("localhost").create();
    private final static AtomicReference<Client> client = new AtomicReference<>();

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BenchmarkOptions(benchmarkRounds = DEFAULT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
    @Test
    public void webSocketsGremlin() throws Exception {
        tryWebSocketGremlin();
    }

    @BenchmarkOptions(benchmarkRounds = DEFAULT_CONCURRENT_BENCHMARK_ROUNDS, warmupRounds = DEFAULT_CONCURRENT_WARMUP_ROUNDS, concurrency = BenchmarkOptions.CONCURRENCY_AVAILABLE_CORES)
    @Test
    public void webSocketsGremlinConcurrent() throws Exception {
        tryWebSocketGremlin();
    }

    @BeforeClass
    public static void before() {
        // good to call init here ahead of performance tracking
        cluster.init();
        client.compareAndSet(null, cluster.connect());
    }

    @AfterClass
    public static void after() {
        cluster.close();
    }

    private void tryWebSocketGremlin() throws Exception {
        final Map<String, Object> params = new HashMap<>();
        params.put("x", 16384l);

        final CompletableFuture<ResultSet> future1 = client.get().submitAsync("g.v(x).out().out().next(512)", params);
        final CompletableFuture<ResultSet> future2 = client.get().submitAsync("g.v(x).out().next(7)", params);
        final CompletableFuture<ResultSet> future3 = client.get().submitAsync("g.v(16384l).out().out().next(10)");
        final CompletableFuture<ResultSet> future4 = client.get().submitAsync("g.v(16432l).out().out().next(10)");
        final CompletableFuture<ResultSet> future5 = client.get().submitAsync("g.v(14l).out().next(1)");

        assertEquals(512, future1.get().stream().count());
        assertEquals(7, future2.get().stream().count());
        assertEquals(10, future3.get().stream().count());
        assertEquals(10, future4.get().stream().count());
        assertEquals(1, future5.get().stream().count());
    }
}

