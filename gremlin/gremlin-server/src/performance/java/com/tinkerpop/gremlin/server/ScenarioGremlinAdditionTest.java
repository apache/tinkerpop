package com.tinkerpop.gremlin.server;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;

/**
 * Execute a simple script (1+1).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "gremlin-addition")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20, filePrefix = "hx-gremlin-addition")
public class ScenarioGremlinAdditionTest extends AbstractGremlinServerPerformanceTest {

    public final static int DEFAULT_BENCHMARK_ROUNDS = 50;
    public final static int DEFAULT_WARMUP_ROUNDS = 5;

    public final static int DEFAULT_CONCURRENT_BENCHMARK_ROUNDS = 500;
    public final static int DEFAULT_CONCURRENT_WARMUP_ROUNDS = 10;

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

    private void tryWebSocketGremlin() throws Exception {
        final String url = getWebSocketBaseUri();
        final WebSocketClient client = new WebSocketClient(url);
        client.open();
        final Object o = client.eval("1+1");
        assertEquals("2", (String) o);
        client.close();
    }
}
