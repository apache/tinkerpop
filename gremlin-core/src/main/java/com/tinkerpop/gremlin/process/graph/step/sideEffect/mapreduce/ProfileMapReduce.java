package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

public final class ProfileMapReduce implements MapReduce<MapReduce.NullObject, TraversalMetrics, MapReduce.NullObject, TraversalMetrics, TraversalMetrics> {

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
        return ProfileStep.METRICS_KEY;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, TraversalMetrics> emitter) {
        this.traversal.asAdmin().getSideEffects().setLocalVertex(vertex);
        this.traversal.asAdmin().getSideEffects().<TraversalMetrics>ifPresent(ProfileStep.METRICS_KEY, emitter::emit);
    }

    @Override
    public void combine(final NullObject key, final Iterator<TraversalMetrics> values, final ReduceEmitter<NullObject, TraversalMetrics> emitter) {
        reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<TraversalMetrics> values, final ReduceEmitter<NullObject, TraversalMetrics> emitter) {
        emitter.emit(TraversalMetrics.merge(values));
    }

    @Override
    public TraversalMetrics generateFinalResult(final Iterator<KeyValue<NullObject, TraversalMetrics>> keyValues) {
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
