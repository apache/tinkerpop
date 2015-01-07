package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetricsUtil;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

public final class ProfileMapReduce implements MapReduce<MapReduce.NullObject, TraversalMetricsUtil, MapReduce.NullObject, TraversalMetricsUtil, TraversalMetricsUtil> {

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
    public void map(final Vertex vertex, final MapEmitter<NullObject, TraversalMetricsUtil> emitter) {
        this.traversal.asAdmin().getSideEffects().setLocalVertex(vertex);
        this.traversal.asAdmin().getSideEffects().<TraversalMetricsUtil>ifPresent(TraversalMetrics.METRICS_KEY, emitter::emit);
    }

    @Override
    public void combine(final NullObject key, final Iterator<TraversalMetricsUtil> values, final ReduceEmitter<NullObject, TraversalMetricsUtil> emitter) {
        reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<TraversalMetricsUtil> values, final ReduceEmitter<NullObject, TraversalMetricsUtil> emitter) {
        emitter.emit(TraversalMetricsUtil.merge(values));
    }

    @Override
    public TraversalMetricsUtil generateFinalResult(final Iterator<KeyValue<NullObject, TraversalMetricsUtil>> keyValues) {
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
