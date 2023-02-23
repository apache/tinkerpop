package org.apache.tinkerpop.jsr223;

import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.Optional;

@State(Scope.Thread)
public class GremlinScriptCheckerBenchmark extends AbstractBenchmarkBase {

    @Benchmark
    public Optional<String> testParseRequestId() {
        return GremlinScriptChecker.parse("g.with('requestId', '4F53FB59-CFC9-4984-B477-452073A352FD').with(true).V().out('knows')").getRequestId();
    }

    @Benchmark
    public Optional<String> testParseMaterializeProperties() {
        return GremlinScriptChecker.parse("g.with('materializeProperties', 'all').with(true).V().out('knows')").getMaterializeProperties();
    }

    @Benchmark
    public GremlinScriptChecker.Result testParseAll() {
        return GremlinScriptChecker.parse("g.with('evaluationTimeout', 1000L).with('materializeProperties', 'all').with('requestId', '4F53FB59-CFC9-4984-B477-452073A352FD').with(true).V().out('knows')");
    }
}
