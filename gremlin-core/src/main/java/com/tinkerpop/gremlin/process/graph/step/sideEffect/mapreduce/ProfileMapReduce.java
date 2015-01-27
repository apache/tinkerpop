package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.VertexTraversalSideEffects;
import com.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.tinkerpop.gremlin.process.util.StandardTraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public final class ProfileMapReduce extends StaticMapReduce<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> {

    public ProfileMapReduce() {

    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public String getMemoryKey() {
        return TraversalMetrics.METRICS_KEY;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, StandardTraversalMetrics> emitter) {
        VertexTraversalSideEffects.of(vertex).<StandardTraversalMetrics>ifPresent(TraversalMetrics.METRICS_KEY, emitter::emit);
    }

    @Override
    public void combine(final NullObject key, final Iterator<StandardTraversalMetrics> values, final ReduceEmitter<NullObject, StandardTraversalMetrics> emitter) {
        reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<StandardTraversalMetrics> values, final ReduceEmitter<NullObject, StandardTraversalMetrics> emitter) {
        emitter.emit(StandardTraversalMetrics.merge(values));
    }

    @Override
    public StandardTraversalMetrics generateFinalResult(final Iterator<KeyValue<NullObject, StandardTraversalMetrics>> keyValues) {
        return keyValues.next().getValue();
    }
}
