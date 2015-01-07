package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.StandardTraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

public final class ProfileMapReduce implements MapReduce<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> {

    private Traversal traversal;

    public ProfileMapReduce(final ProfileStep step) {
        this.traversal = step.getTraversal();
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
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
        this.traversal.asAdmin().getSideEffects().setLocalVertex(vertex);
        this.traversal.asAdmin().getSideEffects().<StandardTraversalMetrics>ifPresent(TraversalMetrics.METRICS_KEY, emitter::emit);
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

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this);
    }

    @Override
    public ProfileMapReduce clone() throws CloneNotSupportedException {
        final ProfileMapReduce clone = (ProfileMapReduce) super.clone();
        clone.traversal = this.traversal.clone();
        return clone;
    }
}
