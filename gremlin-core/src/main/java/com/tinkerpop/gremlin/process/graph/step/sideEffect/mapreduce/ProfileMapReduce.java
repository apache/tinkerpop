package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.GlobalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Iterator;

public final class ProfileMapReduce implements MapReduce<MapReduce.NullObject, GlobalMetrics, MapReduce.NullObject, GlobalMetrics, GlobalMetrics> {

    public ProfileMapReduce() {
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public GlobalMetrics generateSideEffect(final Iterator<Pair<NullObject, GlobalMetrics>> keyValues) {
        return keyValues.next().getValue1();
    }

    @Override
    public String getSideEffectKey() {
        return ProfileStep.METRICS_KEY;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, GlobalMetrics> emitter) {
        MapReduce.getLocalSideEffects(vertex).<GlobalMetrics>ifPresent(ProfileStep.METRICS_KEY, metrics -> emitter.emit(NullObject.instance(), metrics));
    }

    @Override
    public void combine(final NullObject key, final Iterator<GlobalMetrics> values, final ReduceEmitter<NullObject, GlobalMetrics> emitter) {
        reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<GlobalMetrics> values, final ReduceEmitter<NullObject, GlobalMetrics> emitter) {
        emitter.emit(NullObject.instance(), GlobalMetrics.merge(values));
    }
}
