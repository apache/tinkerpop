package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.Iterator;

public final class ProfileMapReduce implements MapReduce<MapReduce.NullObject, TraversalMetrics, MapReduce.NullObject, TraversalMetrics, TraversalMetrics> {

    public ProfileMapReduce() {
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public TraversalMetrics generateSideEffect(final Iterator<Pair<NullObject, TraversalMetrics>> keyValues) {
        return keyValues.next().getValue1();
    }

    @Override
    public String getSideEffectKey() {
        return ProfileStep.METRICS_KEY;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, TraversalMetrics> emitter) {
        TraversalVertexProgram.getLocalSideEffects(vertex).<TraversalMetrics>ifPresent(ProfileStep.METRICS_KEY, metrics -> emitter.emit(NullObject.instance(), metrics));
    }

    @Override
    public void combine(final NullObject key, final Iterator<TraversalMetrics> values, final ReduceEmitter<NullObject, TraversalMetrics> emitter) {
        reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<TraversalMetrics> values, final ReduceEmitter<NullObject, TraversalMetrics> emitter) {
        emitter.emit(NullObject.instance(), TraversalMetrics.merge(values));
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, "");
    }
}
