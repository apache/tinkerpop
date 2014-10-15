package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.GlobalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Iterator;

public class ProfileMapReduce implements MapReduce<MapReduce.NullObject, GlobalMetrics, MapReduce.NullObject, GlobalMetrics, GlobalMetrics> {
    public ProfileMapReduce() {
    }

    @Override
    public boolean doStage(Stage stage) {
        return true;
    }

    @Override
    public GlobalMetrics generateSideEffect(Iterator<Pair<NullObject, GlobalMetrics>> keyValues) {
        return keyValues.next().getValue1();
    }

    @Override
    public String getSideEffectKey() {
        return ProfileStep.METRICS_KEY;
    }

    public void map(final Vertex vertex, final MapEmitter<NullObject, GlobalMetrics> emitter) {
        if (MapReduce.getLocalSideEffects(vertex).exists(ProfileStep.METRICS_KEY)) {
            emitter.emit(NullObject.instance(), MapReduce.getLocalSideEffects(vertex).<GlobalMetrics>get(ProfileStep.METRICS_KEY));
        }
    }

    public void combine(final NullObject key, final Iterator<GlobalMetrics> values, final ReduceEmitter<NullObject, GlobalMetrics> emitter) {
        reduce(key, values, emitter);
    }

    public void reduce(final NullObject key, final Iterator<GlobalMetrics> values, final ReduceEmitter<NullObject, GlobalMetrics> emitter) {
        GlobalMetrics globalMetrics = new GlobalMetrics();
        globalMetrics.merge(values);
        emitter.emit(NullObject.instance(), globalMetrics);
    }
}
